"""
quant_system/strategy/momentum_strategy.py
動量突破：價格突破近期高點 + 成交量放大
BUY  : 突破 N 日高點 + 量 > 均量 × 1.5
SELL : 跌破 N 日低點 or 成交量萎縮後拉回
"""
from __future__ import annotations
from typing import Dict, Optional
import pandas as pd
from .base_strategy import BaseStrategy, SignalResult


class MomentumStrategy(BaseStrategy):
    name = "MOMENTUM"

    def __init__(self, params: Optional[Dict] = None, debug: bool = False) -> None:
        defaults = {
            "breakout_period": 20,      # 突破參考周期
            "vol_ma_period":   20,      # 成交量均線
            "vol_multiplier":  1.5,     # 量放大倍數
        }
        super().__init__({**defaults, **(params or {})}, debug)

    def generate_signal(self, df: pd.DataFrame, **kwargs) -> SignalResult:
        n = self.params["breakout_period"]
        if len(df) < n + 5:
            return SignalResult(strategy=self.name)

        cl   = df["Close"]
        hi   = df["High"]
        lo   = df["Low"]

        price     = float(cl.iloc[-1])
        hi_n      = float(hi.iloc[-(n+1):-1].max())   # 前 n 根高點（不含最新）
        lo_n      = float(lo.iloc[-(n+1):-1].min())   # 前 n 根低點
        ind = {"breakout_high": round(hi_n, 4), "breakout_low": round(lo_n, 4)}

        # 成交量確認
        vol_col = None
        for c in ("Volume", "volume"):
            if c in df.columns:
                vol_col = c; break

        vol_ok = True
        if vol_col:
            vol    = df[vol_col]
            vol_ma = float(vol.rolling(self.params["vol_ma_period"]).mean().iloc[-1])
            cur_vol = float(vol.iloc[-1])
            vol_ok  = cur_vol > vol_ma * self.params["vol_multiplier"]
            ind["vol_ratio"] = round(cur_vol / (vol_ma + 1e-9), 2)

        if price > hi_n and vol_ok:
            conf = min((price - hi_n) / hi_n * 50 + 0.5, 0.95)
            return SignalResult("BUY", conf, self.name, f"突破{n}日高點 vol_ok={vol_ok}", ind)

        if price < lo_n:
            conf = min((lo_n - price) / lo_n * 50 + 0.5, 0.95)
            return SignalResult("SELL", conf, self.name, f"跌破{n}日低點", ind)

        return SignalResult("HOLD", 0.2, self.name, "", ind)
