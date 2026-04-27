"""
quant_system/risk/market_risk_monitors.py — 多市場風險監控器

三個業務線各有專屬風險檢查：
  SpotRiskMonitor   — 現貨：24h 回撤 + 曝險上限（沿用 RiskManager 現有邏輯）
  MarginRiskMonitor — 槓桿：新增保證金水位監控（margin_level < 警戒值時封鎖下單）
  FuturesRiskMonitor— 永續：新增強平距離監控（距強平 < liq_buffer 時封鎖下單）

CheckResult
-----------
    ok      : bool   — True = 可下單；False = 封鎖
    reason  : str    — 封鎖原因（ok=True 時為空字串）
    details : dict   — 診斷數值（供 UI 顯示）
"""
from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass, field
from typing import Dict, Optional

from quant_system.grid.contract_interface import calc_liquidation_price

# 強平資料最小刷新間隔（秒）— 避免每 30s tick 都打 Binance API
_FUTURES_REFRESH_INTERVAL_SEC = float(os.getenv("FUTURES_POS_REFRESH_SEC", "60"))

log = logging.getLogger(__name__)


# ── 回傳型別 ─────────────────────────────────────────────────
@dataclass
class CheckResult:
    ok:      bool
    reason:  str        = ""
    details: Dict       = field(default_factory=dict)


# ════════════════════════════════════════════════════════════
# 基底類別
# ════════════════════════════════════════════════════════════
class BaseRiskMonitor:
    """
    所有市場風險監控器的基底。

    子類必須實作 check(current_price, **kwargs) -> CheckResult。
    所有子類都會先呼叫 RiskManager 的通用檢查（24h 回撤 + 曝險上限），
    再疊加業務線專屬檢查。
    """

    def __init__(self, risk_mgr, symbol: str):
        self._risk_mgr = risk_mgr
        self._symbol   = symbol

    def _common_checks(self, max_drawdown_pct: Optional[float] = None,
                       proposed_usdt: float = 0.0,
                       total_equity:  float = 1.0) -> CheckResult:
        """共用：24h 回撤 + 曝險上限。"""
        # 24h 回撤
        ok, dd_status = self._risk_mgr.check_24h_drawdown(
            self._symbol,
            max_drawdown_pct=max_drawdown_pct,
        )
        if not ok:
            return CheckResult(
                ok=False,
                reason=dd_status.reason,
                details={"drawdown_24h_pct": dd_status.drawdown_24h_pct},
            )

        # 曝險上限
        if proposed_usdt > 0:
            exp_ok, exp_reason, exp_info = self._risk_mgr.check_exposure(
                self._symbol, proposed_usdt, total_equity
            )
            if not exp_ok:
                return CheckResult(
                    ok=False,
                    reason=exp_reason,
                    details={"exposure": vars(exp_info) if exp_info else {}},
                )

        return CheckResult(ok=True)

    def check(self, current_price: float = 0.0, **kwargs) -> CheckResult:
        raise NotImplementedError


# ════════════════════════════════════════════════════════════
# 現貨監控器
# ════════════════════════════════════════════════════════════
class SpotRiskMonitor(BaseRiskMonitor):
    """現貨：只執行通用的 24h 回撤 + 曝險上限檢查。"""

    def check(self, current_price: float = 0.0,
              proposed_usdt: float = 0.0,
              total_equity: float = 1.0,
              **kwargs) -> CheckResult:
        return self._common_checks(
            proposed_usdt=proposed_usdt,
            total_equity=total_equity,
        )


# ════════════════════════════════════════════════════════════
# 保證金監控器
# ════════════════════════════════════════════════════════════
class MarginRiskMonitor(BaseRiskMonitor):
    """
    槓桿：在通用檢查之上，額外檢查帳戶保證金水位。

    margin_level = 帳戶淨值 / 已借貸金額 × 100
    Binance 全倉保證金水位 < 1.1 觸發強平；< warn_level_pct（預設 150）發出警告並封鎖。
    """

    def __init__(self, risk_mgr, symbol: str, warn_level_pct: float = 150.0):
        super().__init__(risk_mgr, symbol)
        self._warn_level = warn_level_pct

    def check(self, current_price: float = 0.0,
              proposed_usdt: float = 0.0,
              total_equity: float = 1.0,
              margin_account: Optional[Dict] = None,
              **kwargs) -> CheckResult:

        base = self._common_checks(
            proposed_usdt=proposed_usdt,
            total_equity=total_equity,
        )
        if not base.ok:
            return base

        if margin_account:
            try:
                margin_level = float(margin_account.get("marginLevel", 999))
                if margin_level < self._warn_level:
                    msg = (
                        f"保證金水位 {margin_level:.1f}% < 警戒值 {self._warn_level:.0f}%，"
                        f"封鎖新倉位防止強平"
                    )
                    log.warning("[MarginRisk] %s %s", self._symbol, msg)
                    return CheckResult(
                        ok=False,
                        reason=msg,
                        details={"margin_level": margin_level,
                                 "warn_level":   self._warn_level},
                    )
            except (TypeError, ValueError) as e:
                log.debug("[MarginRisk] 無法解析保證金水位: %s", e)

        return CheckResult(ok=True, details={"margin_account_checked": margin_account is not None})


# ════════════════════════════════════════════════════════════
# 期貨監控器
# ════════════════════════════════════════════════════════════
class FuturesRiskMonitor(BaseRiskMonitor):
    """
    永續合約：在通用檢查之上，額外監控強平距離。

    強平緩衝規則（使用 Binance 即時標記價格 + 實際強平價）：
      distance_pct = |mark_price - liq_price| / mark_price
      若 distance_pct < liq_buffer → 封鎖新下單，通知減倉

    資料來源優先順序：
      1. 即時資料（refresh_from_exchange() 每 60s 從 Binance 拉取）
           liq_price  ← /fapi/v2/positionRisk  liquidationPrice
           mark_price ← /fapi/v1/premiumIndex  markPrice
      2. 公式估算（無持倉或 Binance 未回應時 fallback）
           calc_liquidation_price(entry_price, leverage, side)
    """

    def __init__(self, risk_mgr, symbol: str,
                 leverage: int = 1, liq_buffer: float = 0.15):
        super().__init__(risk_mgr, symbol)
        self._leverage   = leverage
        self._liq_buffer = liq_buffer

        # ── 即時倉位快取（由 refresh_from_exchange() 更新）──
        self._liq_price:    float = 0.0   # Binance 回傳的實際強平價
        self._mark_price:   float = 0.0   # 標記價格（防止 kline 偏差）
        self._entry_price:  float = 0.0   # 開倉均價
        self._pos_side:     str   = "LONG"
        self._refreshed_at: float = 0.0   # 上次成功刷新的 Unix ts

    # ── 從 Binance 拉取即時倉位資料（限速：每 60s 最多一次）──────
    def refresh_from_exchange(self, client, symbol: str) -> None:
        """
        在 tick() 的 Observation 階段呼叫（Phase 1，永遠執行）。
        透過 python-binance 同步 client 取得標記價格與強平價，
        快取於 self._mark_price / self._liq_price / self._entry_price。
        限速：兩次刷新間隔 < _FUTURES_REFRESH_INTERVAL_SEC 時直接跳過。
        """
        now = time.time()
        if now - self._refreshed_at < _FUTURES_REFRESH_INTERVAL_SEC:
            return  # 快取仍新鮮，不重複請求

        # ① 標記價格 /fapi/v1/premiumIndex
        try:
            mark_data = client.futures_mark_price(symbol=symbol)
            if isinstance(mark_data, dict):
                mp = float(mark_data.get("markPrice", 0.0))
                if mp > 0:
                    self._mark_price = mp
        except Exception as e:
            log.debug("[FuturesRisk] mark price 取得失敗: %s", e)

        # ② 倉位資料 /fapi/v2/positionRisk → 強平價 + 開倉均價
        try:
            positions = client.futures_position_information(symbol=symbol)
            for pos in (positions or []):
                qty = float(pos.get("positionAmt", 0))
                if abs(qty) < 1e-10:
                    continue  # 空倉跳過
                liq_px  = float(pos.get("liquidationPrice", 0.0))
                ent_px  = float(pos.get("entryPrice",       0.0))
                p_side  = "LONG" if qty > 0 else "SHORT"
                if liq_px > 0:
                    self._liq_price   = liq_px
                    self._entry_price = ent_px
                    self._pos_side    = p_side
                    break  # 取第一個有效持倉
        except Exception as e:
            log.debug("[FuturesRisk] position risk 取得失敗: %s", e)

        self._refreshed_at = now
        log.debug(
            "[FuturesRisk] %s 倉位快取更新 mark=%.4f liq=%.4f entry=%.4f side=%s",
            symbol, self._mark_price, self._liq_price,
            self._entry_price, self._pos_side,
        )

    def check(self, current_price: float = 0.0,
              proposed_usdt: float = 0.0,
              total_equity:  float = 1.0,
              entry_price:   float = 0.0,
              side:          str   = "LONG",
              **kwargs) -> CheckResult:

        base = self._common_checks(
            proposed_usdt=proposed_usdt,
            total_equity=total_equity,
        )
        if not base.ok:
            return base

        # ── 決定有效的標記價格（優先使用 Binance 即時值）──────
        eff_mark  = self._mark_price  if self._mark_price  > 0 else current_price
        eff_entry = self._entry_price if self._entry_price > 0 else entry_price
        eff_side  = self._pos_side    if self._entry_price > 0 else side.upper()

        # ── 決定強平價（優先使用 Binance 回傳的實際值）──────────
        if self._liq_price > 0:
            liq_px      = self._liq_price
            data_source = "live"
        elif eff_entry > 0 and self._leverage > 1:
            liq_px      = calc_liquidation_price(
                entry_price=eff_entry,
                leverage=self._leverage,
                side=eff_side,
            )
            data_source = "formula"
        else:
            # 無持倉資料 → 跳過強平檢查（允許下單）
            return CheckResult(
                ok=True,
                details={
                    "liq_checked":   False,
                    "reason":        "no_position_data",
                    "leverage":      self._leverage,
                    "mark_price":    eff_mark,
                    "data_age_sec":  round(time.time() - self._refreshed_at, 1),
                },
            )

        # ── 強平距離計算 ────────────────────────────────────────
        if liq_px > 0 and eff_mark > 0:
            distance_pct = abs(eff_mark - liq_px) / eff_mark
            details = {
                "liq_price":    round(liq_px, 4),
                "mark_price":   round(eff_mark, 4),
                "distance_pct": round(distance_pct, 4),
                "liq_buffer":   self._liq_buffer,
                "leverage":     self._leverage,
                "side":         eff_side,
                "data_source":  data_source,
                "data_age_sec": round(time.time() - self._refreshed_at, 1),
            }
            if distance_pct < self._liq_buffer:
                msg = (
                    f"強平距離 {distance_pct:.1%} < 緩衝 {self._liq_buffer:.1%}，"
                    f"封鎖新倉位（強平價 {liq_px:.2f}，標記價格 {eff_mark:.2f}，"
                    f"來源={data_source}）"
                )
                log.warning("[FuturesRisk] %s %s", self._symbol, msg)
                return CheckResult(ok=False, reason=msg, details=details)
            return CheckResult(ok=True, details=details)

        return CheckResult(
            ok=True,
            details={
                "liq_checked": False,
                "leverage":    self._leverage,
                "mark_price":  eff_mark,
            },
        )
