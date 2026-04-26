"""
quant_system/strategy/rsi_macd_strategy.py
BUY  : RSI 從超賣回升 + MACD 金叉
SELL : RSI 超買 or MACD 死叉
"""
from __future__ import annotations
from typing import Dict, Optional
import pandas as pd
from .base_strategy import BaseStrategy, SignalResult


class RSIMACDStrategy(BaseStrategy):
    name = "RSI_MACD"

    def __init__(self, params: Optional[Dict] = None, debug: bool = False) -> None:
        defaults = {
            "rsi_period":  14,
            "rsi_oversold": 35,
            "rsi_overbought": 65,
            "macd_fast": 12, "macd_slow": 26, "macd_signal": 9,
            "cooldown_bars": 3,
        }
        super().__init__({**defaults, **(params or {})}, debug)
        self._last_signal_bar = -999

    def generate_signal(self, df: pd.DataFrame, bar_idx: int = -1, **kwargs) -> SignalResult:
        if len(df) < 35:
            return SignalResult(strategy=self.name)

        cl = df["Close"]
        rsi = self._rsi(cl, self.params["rsi_period"])
        macd, sig, hist = self._macd(cl, self.params["macd_fast"],
                                      self.params["macd_slow"], self.params["macd_signal"])

        r   = float(rsi.iloc[-1])
        m   = float(macd.iloc[-1])
        s   = float(sig.iloc[-1])
        m1  = float(macd.iloc[-2])
        s1  = float(sig.iloc[-2])
        ind = {"rsi": round(r, 2), "macd": round(m, 4), "macd_sig": round(s, 4)}

        # 冷卻期
        current_bar = len(df)
        if current_bar - self._last_signal_bar < self.params["cooldown_bars"]:
            return SignalResult(signal="HOLD", strategy=self.name, indicators=ind)

        # BUY：RSI 從超賣反彈 + MACD 金叉
        if (r < self.params["rsi_oversold"] + 5 and m > s and m1 <= s1):
            self._last_signal_bar = current_bar
            conf = min((self.params["rsi_oversold"] + 5 - r) / 15 + 0.3, 1.0)
            return SignalResult("BUY", conf, self.name, f"RSI={r:.1f} MACD金叉", ind)

        # SELL：RSI 超買 or MACD 死叉
        if r > self.params["rsi_overbought"] or (m < s and m1 >= s1):
            self._last_signal_bar = current_bar
            conf = min((r - self.params["rsi_overbought"]) / 10 + 0.4, 1.0) if r > self.params["rsi_overbought"] else 0.5
            return SignalResult("SELL", conf, self.name, f"RSI={r:.1f} MACD死叉", ind)

        return SignalResult(signal="HOLD", strategy=self.name, indicators=ind)
