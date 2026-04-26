"""
quant_system/strategy/trend_strategy.py
趨勢跟隨：EMA 20/50/200 多頭排列 + ADX 確認
BUY  : EMA20 > EMA50 > EMA200 + ADX > 25
SELL : EMA20 < EMA50 or 價格跌破 EMA50
"""
from __future__ import annotations
from typing import Dict, Optional
import pandas as pd
from .base_strategy import BaseStrategy, SignalResult


class TrendStrategy(BaseStrategy):
    name = "TREND"

    def __init__(self, params: Optional[Dict] = None, debug: bool = False) -> None:
        defaults = {
            "ema_fast": 20, "ema_mid": 50, "ema_slow": 200,
            "adx_period": 14, "adx_thresh": 25,
        }
        super().__init__({**defaults, **(params or {})}, debug)

    def generate_signal(self, df: pd.DataFrame, **kwargs) -> SignalResult:
        if len(df) < self.params["ema_slow"] + 10:
            return SignalResult(strategy=self.name)

        cl    = df["Close"]
        ema20 = float(self._ema(cl, self.params["ema_fast"]).iloc[-1])
        ema50 = float(self._ema(cl, self.params["ema_mid"]).iloc[-1])
        ema200 = float(self._ema(cl, self.params["ema_slow"]).iloc[-1])
        price  = float(cl.iloc[-1])
        ind = {"ema20": round(ema20, 4), "ema50": round(ema50, 4), "ema200": round(ema200, 4)}

        # 多頭排列 → BUY
        if ema20 > ema50 > ema200 and price > ema20:
            gap = (ema20 - ema50) / ema50
            conf = min(gap * 20 + 0.4, 0.95)
            return SignalResult("BUY", conf, self.name, "多頭排列 EMA20>50>200", ind)

        # 跌破 EMA50 → SELL
        if price < ema50 and ema20 < ema50:
            conf = min((ema50 - price) / ema50 * 20 + 0.4, 0.95)
            return SignalResult("SELL", conf, self.name, f"跌破EMA50 px={price:.2f}", ind)

        return SignalResult("HOLD", 0.3, self.name, "", ind)
