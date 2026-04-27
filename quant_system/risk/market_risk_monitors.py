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
from dataclasses import dataclass, field
from typing import Dict, Optional

from quant_system.grid.contract_interface import calc_liquidation_price

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

    強平緩衝規則：
      distance_pct = |current_price - liq_price| / current_price
      若 distance_pct < liq_buffer（預設 15%） → 封鎖新下單，通知減倉
    """

    def __init__(self, risk_mgr, symbol: str,
                 leverage: int = 1, liq_buffer: float = 0.15):
        super().__init__(risk_mgr, symbol)
        self._leverage   = leverage
        self._liq_buffer = liq_buffer

    def check(self, current_price: float = 0.0,
              proposed_usdt: float = 0.0,
              total_equity: float = 1.0,
              entry_price: float = 0.0,
              side: str = "LONG",
              **kwargs) -> CheckResult:

        base = self._common_checks(
            proposed_usdt=proposed_usdt,
            total_equity=total_equity,
        )
        if not base.ok:
            return base

        # 強平距離檢查
        if entry_price > 0 and current_price > 0 and self._leverage > 1:
            liq_px = calc_liquidation_price(
                entry_price=entry_price,
                leverage=self._leverage,
                side=side.upper(),
            )
            if liq_px > 0:
                distance_pct = abs(current_price - liq_px) / current_price
                if distance_pct < self._liq_buffer:
                    msg = (
                        f"強平距離 {distance_pct:.1%} < 緩衝 {self._liq_buffer:.1%}，"
                        f"封鎖新倉位（強平價 {liq_px:.2f}，現價 {current_price:.2f}）"
                    )
                    log.warning("[FuturesRisk] %s %s", self._symbol, msg)
                    return CheckResult(
                        ok=False,
                        reason=msg,
                        details={
                            "liq_price":    liq_px,
                            "current_price": current_price,
                            "distance_pct": round(distance_pct, 4),
                            "liq_buffer":   self._liq_buffer,
                            "leverage":     self._leverage,
                        },
                    )

        return CheckResult(
            ok=True,
            details={
                "leverage":     self._leverage,
                "liq_buffer":   self._liq_buffer,
                "entry_checked": entry_price > 0,
            },
        )
