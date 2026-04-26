"""
quant_system/grid/trend_filter.py — 趨勢過濾器（Phase F 強化版）

原有功能（三層過濾）：
  Layer 1 — ATR/price 極端偵測：ATR/price > extreme_thresh → 暫停整個網格
  Layer 2 — 快速崩跌偵測：N 根 K 線跌幅 > crash_pct → 停止買入
  Layer 3 — EMA200 過濾：price < EMA200 → 停止向下掛 BUY 單

Phase F 新增（④ 微觀結構過濾器）：
  Layer 4a — 成交量爆發（Volume Surge）：
              最新 volume > vol_surge_lookback 期均量 × vol_surge_mult
              + 收盤跌破開盤（收黑 K） → 暫停買入（資金出逃訊號）
  Layer 4b — 短期價格衝擊（Price Shock）：
              price_shock_bars 根 K 線內漲/跌幅超過 price_shock_pct
              + 方向為「下跌衝擊」 → 暫停買入
              + 方向為「上漲衝擊」 → 記錄警示（可能是假突破後急速拉回）

微觀結構過濾的設計原理：
  EMA200 是「事後確認」指標（滯後 100+ 根 K 線），無法在大量成交量出現時
  提前預警。Volume Surge + Price Shock 組合偵測的是「當下發生的」賣壓爆發，
  讓系統比 EMA200 更早停止掛買單，減少在崩跌初期接刀。
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import List, Optional

import pandas as pd

log = logging.getLogger(__name__)


@dataclass
class FilterResult:
    allow_buy:   bool   # 允許掛 BUY 單
    allow_sell:  bool   # 允許掛 SELL 單
    allow_grid:  bool   # 整個網格是否運作
    reason:      str    # 停止原因（空字串 = 正常）
    ema200:      float = 0.0
    atr_ratio:   float = 0.0
    # ④ 微觀結構診斷資訊
    vol_surge:   bool  = False    # 是否偵測到成交量爆發
    price_shock: float = 0.0      # 短期價格衝擊幅度（正 = 上漲，負 = 下跌）
    micro_reason: str = ""        # 微觀結構過濾原因


class TrendFilter:
    """
    多層趨勢過濾器。

    Phase F 新增第 4 層微觀結構過濾（Vol Surge + Price Shock）。
    兩個子過濾器可獨立開關，互相搭配使用。

    Args：
        # 原有參數
        ema_period      — 長期趨勢均線（預設 200）
        extreme_thresh  — ATR/price 黑天鵝閾值（預設 3.5%）
        crash_bars      — 快速崩跌偵測根數（預設 5）
        crash_pct       — 快速崩跌閾值（預設 8%）
        # ④ 新增參數
        vol_surge_lookback — Volume Surge 計算均量的回溯根數（預設 20）
        vol_surge_mult     — 成交量超過均量幾倍觸發（預設 3.0×）
        vol_require_red    — Volume Surge 是否要求收黑 K 才觸發（預設 True）
        price_shock_bars   — Price Shock 偵測回溯根數（預設 3）
        price_shock_pct    — 觸發 Price Shock 的幅度閾值（預設 4%）
        enable_vol_filter  — 開關 Volume Surge 過濾（預設 True）
        enable_shock_filter— 開關 Price Shock 過濾（預設 True）
    """

    def __init__(
        self,
        # 原有
        ema_period:       int   = 200,
        extreme_thresh:   float = 0.035,
        crash_bars:       int   = 5,
        crash_pct:        float = 0.08,
        # ④ 微觀結構 — Volume Surge
        vol_surge_lookback:  int   = 20,
        vol_surge_mult:      float = 3.0,
        vol_require_red:     bool  = True,   # True=需要收黑才觸發（降低假訊號）
        # ④ 微觀結構 — Price Shock
        price_shock_bars:    int   = 3,
        price_shock_pct:     float = 0.04,   # 3 根 K 線內 4% 衝擊
        # 開關
        enable_vol_filter:   bool  = True,
        enable_shock_filter: bool  = True,
    ) -> None:
        self.ema_period          = ema_period
        self.extreme_thresh      = extreme_thresh
        self.crash_bars          = crash_bars
        self.crash_pct           = crash_pct
        self.vol_surge_lookback  = vol_surge_lookback
        self.vol_surge_mult      = vol_surge_mult
        self.vol_require_red     = vol_require_red
        self.price_shock_bars    = price_shock_bars
        self.price_shock_pct     = price_shock_pct
        self.enable_vol_filter   = enable_vol_filter
        self.enable_shock_filter = enable_shock_filter

    # ════════════════════════════════════════════════════════
    # 主過濾入口
    # ════════════════════════════════════════════════════════
    def check(self, df: pd.DataFrame) -> FilterResult:
        """
        依序執行四層過濾，遇到觸發條件立即回傳（短路評估）。

        優先順序：Layer 1 > Layer 2 > Layer 4 > Layer 3
        （微觀結構 Layer 4 插在 EMA200 Layer 3 之前，實現「提早」攔截）
        """
        if len(df) < self.ema_period + 5:
            return FilterResult(
                allow_buy=True, allow_sell=True, allow_grid=True,
                reason="資料不足，預設放行"
            )

        cl  = df["Close"].values.astype(float)
        op  = df["Open"].values.astype(float) if "Open" in df.columns else cl
        hi  = df["High"].values.astype(float)
        lo  = df["Low"].values.astype(float)
        vol = df["Volume"].values.astype(float) if "Volume" in df.columns else None
        price = cl[-1]

        # ── EMA200 ──────────────────────────────────────────
        ema200 = float(
            pd.Series(cl).ewm(span=self.ema_period, adjust=False).mean().iloc[-1]
        )

        # ── ATR（14 期）──────────────────────────────────────
        trs = [
            max(hi[i] - lo[i], abs(hi[i] - cl[i-1]), abs(lo[i] - cl[i-1]))
            for i in range(1, len(cl))
        ]
        atr   = (sum(trs[-14:]) / 14 if len(trs) >= 14
                 else (sum(trs) / len(trs) if trs else price * 0.01))
        atr_r = atr / (price + 1e-9)

        # ── Layer 1：極端波動（黑天鵝）───────────────────────
        if atr_r > self.extreme_thresh:
            log.warning(
                f"[Filter] Layer1 黑天鵝: ATR/price={atr_r:.3%} > {self.extreme_thresh:.3%}"
            )
            return FilterResult(
                False, False, False,
                f"極端波動 ATR/price={atr_r:.2%}", ema200, atr_r
            )

        # ── Layer 2：快速崩跌 ────────────────────────────────
        if len(cl) > self.crash_bars:
            recent_drop = (cl[-(self.crash_bars + 1)] - price) / (cl[-(self.crash_bars + 1)] + 1e-9)
        else:
            recent_drop = 0.0

        if recent_drop > self.crash_pct:
            log.warning(
                f"[Filter] Layer2 快速崩跌: {recent_drop:.2%} in {self.crash_bars} bars"
            )
            return FilterResult(
                False, True, True,
                f"快速崩跌 {recent_drop:.2%}，暫停買入", ema200, atr_r
            )

        # ── Layer 4：微觀結構過濾（④ Phase F 新增）──────────
        vol_surge, shock_pct, micro_reason = self._check_microstructure(
            cl, op, hi, lo, vol
        )
        if micro_reason:
            log.warning(f"[Filter] Layer4 微觀結構: {micro_reason}")
            return FilterResult(
                allow_buy   = False,
                allow_sell  = True,
                allow_grid  = True,
                reason      = micro_reason,
                ema200      = ema200,
                atr_ratio   = atr_r,
                vol_surge   = vol_surge,
                price_shock = shock_pct,
                micro_reason= micro_reason,
            )

        # ── Layer 3：EMA200 趨勢過濾 ─────────────────────────
        if price < ema200:
            log.info(
                f"[Filter] Layer3 EMA200: price({price:.4f}) < EMA200({ema200:.4f})，停止買入"
            )
            return FilterResult(
                False, True, True,
                f"price < EMA200({ema200:.2f})，停止買入",
                ema200, atr_r,
                vol_surge=vol_surge, price_shock=shock_pct,
            )

        return FilterResult(
            True, True, True, "",
            ema200, atr_r,
            vol_surge=vol_surge, price_shock=shock_pct,
        )

    # ════════════════════════════════════════════════════════
    # ④ 微觀結構過濾子方法
    # ════════════════════════════════════════════════════════
    def _check_microstructure(
        self,
        cl:  "np.ndarray",
        op:  "np.ndarray",
        hi:  "np.ndarray",
        lo:  "np.ndarray",
        vol: "Optional[np.ndarray]",
    ) -> tuple[bool, float, str]:
        """
        ④ 微觀結構偵測。

        Returns:
            (vol_surge, price_shock_pct, micro_reason)
            micro_reason 非空字串時表示觸發，應停止買入。
        """
        vol_surge   = False
        shock_pct   = 0.0
        reasons: List[str] = []

        # ── 4a：成交量爆發（Volume Surge）───────────────────
        if self.enable_vol_filter and vol is not None and len(vol) >= self.vol_surge_lookback + 1:
            avg_vol      = vol[-(self.vol_surge_lookback + 1):-1].mean()
            latest_vol   = vol[-1]
            is_red_candle = cl[-1] < op[-1]   # 收黑 K（收盤 < 開盤）

            if avg_vol > 0 and (latest_vol / avg_vol) >= self.vol_surge_mult:
                vol_surge = True
                # 是否要求收黑 K 才觸發
                if not self.vol_require_red or is_red_candle:
                    reasons.append(
                        f"Volume Surge {latest_vol / avg_vol:.1f}×均量"
                        + ("（收黑）" if is_red_candle else "")
                    )
                    log.debug(
                        f"[Filter] ④ Vol Surge: latest={latest_vol:.0f} "
                        f"avg={avg_vol:.0f} ratio={latest_vol/avg_vol:.1f}× "
                        f"red={is_red_candle}"
                    )
                else:
                    # 成交量爆發但收紅 → 記錄觀察，不觸發過濾
                    log.debug(
                        f"[Filter] ④ Vol Surge 但收紅 K（上漲爆量），不觸發暫停"
                    )

        # ── 4b：短期價格衝擊（Price Shock）──────────────────
        if self.enable_shock_filter and len(cl) > self.price_shock_bars:
            base_px   = cl[-(self.price_shock_bars + 1)]
            shock_pct = (cl[-1] - base_px) / (base_px + 1e-9)

            if shock_pct < -self.price_shock_pct:
                # 下跌衝擊：停止買入
                reasons.append(
                    f"Price Shock 下跌 {shock_pct:.2%} in {self.price_shock_bars} bars"
                )
                log.debug(
                    f"[Filter] ④ Price Shock 下跌: {shock_pct:.2%} "
                    f"in {self.price_shock_bars} bars（閾值 {-self.price_shock_pct:.2%}）"
                )
            elif shock_pct > self.price_shock_pct:
                # 上漲衝擊：記錄觀察（假突破後急拉可能急跌）
                log.info(
                    f"[Filter] ④ Price Shock 上漲 {shock_pct:.2%}，注意可能假突破"
                )
                # 上漲衝擊不停買（不影響做多），但 shock_pct 記錄在 FilterResult 中

        micro_reason = "；".join(reasons) if reasons else ""
        return vol_surge, shock_pct, micro_reason
