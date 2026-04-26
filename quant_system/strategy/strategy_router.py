"""
quant_system/strategy/strategy_router.py — 根據市場狀態自動路由策略
  TRENDING  → TrendStrategy + MomentumStrategy（取較高信心）
  RANGING   → MeanReversionStrategy
  VOLATILE  → RSIMACDStrategy（短線反轉）
  AUTO/其他  → RSIMACDStrategy（預設）
"""
from __future__ import annotations
import logging
from typing import Dict, Optional

import pandas as pd

from .base_strategy        import SignalResult
from .rsi_macd_strategy    import RSIMACDStrategy
from .trend_strategy       import TrendStrategy
from .mean_reversion_strategy import MeanReversionStrategy
from .momentum_strategy    import MomentumStrategy

log = logging.getLogger(__name__)


class StrategyRouter:
    def __init__(self, params: Optional[Dict] = None, debug: bool = False) -> None:
        p = params or {}
        self._rsi_macd    = RSIMACDStrategy(p, debug)
        self._trend       = TrendStrategy(p, debug)
        self._mean_rev    = MeanReversionStrategy(p, debug)
        self._momentum    = MomentumStrategy(p, debug)
        self.debug        = debug

    def route(
        self,
        df:        pd.DataFrame,
        regime:    str = "AUTO",
        timeframe: str = "5m",
    ) -> SignalResult:
        regime = (regime or "AUTO").upper()

        if regime == "TRENDING":
            s1 = self._trend.generate_signal(df)
            s2 = self._momentum.generate_signal(df)
            sig = s1 if s1.confidence >= s2.confidence else s2

        elif regime == "RANGING":
            sig = self._mean_rev.generate_signal(df)

        elif regime == "VOLATILE":
            sig = self._rsi_macd.generate_signal(df)

        else:   # AUTO / UNKNOWN
            sig = self._rsi_macd.generate_signal(df)

        if self.debug:
            log.debug(f"[Router] regime={regime} tf={timeframe} → {sig.signal}({sig.confidence:.0%}) [{sig.strategy}]")

        return sig
