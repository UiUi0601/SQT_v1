"""
quant_system/strategy/mean_reversion_strategy.py
均值回歸：布林通道 + RSI 雙重確認
BUY  : 價格觸及下軌 + RSI < 35
SELL : 價格觸及上軌 or RSI > 65
"""
from __future__ import annotations
from typing import Dict, Optional
import pandas as pd
from .base_strategy import BaseStrategy, SignalResult


class MeanReversionStrategy(BaseStrategy):
    name = "MEAN_REVERSION"

    def __init__(self, params: Optional[Dict] = None, debug: bool = False) -> None:
        defaults = {
            "bb_period": 20, "bb_std": 2.0,
            "rsi_period": 14, "rsi_low": 35, "rsi_high": 65,
        }
        super().__init__({**defaults, **(params or {})}, debug)

    def generate_signal(self, df: pd.DataFrame, **kwargs) -> SignalResult:
        if len(df) < self.params["bb_period"] + 5:
            return SignalResult(strategy=self.name)

        cl  = df["Close"]
        rsi = self._rsi(cl, self.params["rsi_period"])
        ub, mb, lb = self._bbands(cl, self.params["bb_period"], self.params["bb_std"])

        price = float(cl.iloc[-1])
        r     = float(rsi.iloc[-1])
        upper = float(ub.iloc[-1])
        lower = float(lb.iloc[-1])
        mid   = float(mb.iloc[-1])
        ind   = {"rsi": round(r, 2), "bb_upper": round(upper, 4),
                 "bb_lower": round(lower, 4), "bb_mid": round(mid, 4)}

        # 觸及下軌 + RSI 超賣
        if price <= lower and r < self.params["rsi_low"]:
            dist = (lower - price) / lower if lower > 0 else 0
            conf = min(dist * 10 + (self.params["rsi_low"] - r) / 20 + 0.4, 0.95)
            return SignalResult("BUY", conf, self.name, f"BB下軌 RSI={r:.1f}", ind)

        # 觸及上軌 or RSI 超買
        if price >= upper or r > self.params["rsi_high"]:
            conf = 0.6 if price >= upper else 0.45
            return SignalResult("SELL", conf, self.name, f"BB上軌 RSI={r:.1f}", ind)

        return SignalResult("HOLD", 0.2, self.name, "", ind)
