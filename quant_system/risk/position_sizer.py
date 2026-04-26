"""
quant_system/risk/position_sizer.py — ATR / 固定比例 / Kelly 倉位計算
"""
from __future__ import annotations
import logging
from enum import Enum
from typing import Optional

import pandas as pd

log = logging.getLogger(__name__)


class SizingMethod(Enum):
    FIXED_PCT  = "fixed_pct"    # 固定比例
    ATR_BASED  = "atr_based"    # ATR 波動調整
    KELLY      = "kelly"        # Kelly 準則（需勝率+賠率）


class PositionSizer:
    def __init__(
        self,
        method:      SizingMethod = SizingMethod.ATR_BASED,
        risk_pct:    float = 0.01,    # 每筆最大風險比例
        max_qty_pct: float = 0.30,    # 單倉最多佔資金比例
        atr_period:  int   = 14,
        atr_multiplier: float = 2.0,
    ) -> None:
        self.method    = method
        self.risk_pct  = risk_pct
        self.max_qty_p = max_qty_pct
        self.atr_period = atr_period
        self.atr_mult  = atr_multiplier

    def calculate(
        self,
        equity:    float,
        avail:     float,
        price:     float,
        df:        Optional[pd.DataFrame] = None,
        win_rate:  float = 0.5,
        avg_win:   float = 0.04,
        avg_loss:  float = 0.02,
    ) -> float:
        if price <= 0 or equity <= 0:
            return 0.0

        if self.method == SizingMethod.FIXED_PCT:
            qty = (equity * self.risk_pct) / price

        elif self.method == SizingMethod.ATR_BASED:
            atr = self._atr(df) if df is not None else price * 0.02
            risk_amount = equity * self.risk_pct
            stop_dist   = atr * self.atr_mult
            qty = risk_amount / stop_dist if stop_dist > 0 else 0.0

        elif self.method == SizingMethod.KELLY:
            # Kelly f* = W/L - (1-W)/W_ratio; 使用半 Kelly
            if avg_loss <= 0:
                qty = 0.0
            else:
                ratio = avg_win / avg_loss
                kelly = win_rate - (1 - win_rate) / ratio
                kelly = max(kelly, 0.0) * 0.5   # half-Kelly
                qty   = (equity * kelly) / price
        else:
            qty = 0.0

        # 上限：不超過可用資金的 max_qty_pct
        max_qty = (avail * self.max_qty_p) / price
        return min(qty, max_qty)

    # ── internal ─────────────────────────────────────────────
    def _atr(self, df: pd.DataFrame) -> float:
        try:
            hi = df["High"].values
            lo = df["Low"].values
            cl = df["Close"].values
            n  = self.atr_period
            trs = []
            for i in range(1, len(cl)):
                tr = max(hi[i] - lo[i], abs(hi[i] - cl[i-1]), abs(lo[i] - cl[i-1]))
                trs.append(tr)
            if not trs:
                return cl[-1] * 0.02
            return sum(trs[-n:]) / min(n, len(trs))
        except Exception:
            return df["Close"].iloc[-1] * 0.02
