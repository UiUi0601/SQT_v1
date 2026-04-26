"""
quant_system/strategy/base_strategy.py — 策略基底類別
"""
from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, Optional

import pandas as pd


@dataclass
class SignalResult:
    signal:     str   = "HOLD"   # BUY | SELL | HOLD
    confidence: float = 0.0      # 0~1
    strategy:   str   = ""       # 策略名稱
    reason:     str   = ""       # 觸發原因
    indicators: Dict  = field(default_factory=dict)


class BaseStrategy(ABC):
    """所有策略的共同基底。"""

    name: str = "base"

    def __init__(self, params: Optional[Dict] = None, debug: bool = False) -> None:
        self.params = params or {}
        self.debug  = debug

    @abstractmethod
    def generate_signal(self, df: pd.DataFrame, **kwargs) -> SignalResult:
        """給定 OHLCV DataFrame，回傳 SignalResult（不含價格）。"""
        ...

    # ── helpers ──────────────────────────────────────────────
    @staticmethod
    def _rsi(series: pd.Series, period: int = 14) -> pd.Series:
        delta  = series.diff()
        gain   = delta.clip(lower=0).ewm(span=period, adjust=False).mean()
        loss   = (-delta.clip(upper=0)).ewm(span=period, adjust=False).mean()
        rs     = gain / (loss + 1e-9)
        return 100 - 100 / (1 + rs)

    @staticmethod
    def _ema(series: pd.Series, span: int) -> pd.Series:
        return series.ewm(span=span, adjust=False).mean()

    @staticmethod
    def _macd(series: pd.Series, fast=12, slow=26, signal=9):
        fast_ema = series.ewm(span=fast, adjust=False).mean()
        slow_ema = series.ewm(span=slow, adjust=False).mean()
        macd     = fast_ema - slow_ema
        sig      = macd.ewm(span=signal, adjust=False).mean()
        hist     = macd - sig
        return macd, sig, hist

    @staticmethod
    def _bbands(series: pd.Series, period: int = 20, std_mult: float = 2.0):
        mid = series.rolling(period).mean()
        std = series.rolling(period).std()
        return mid + std_mult * std, mid, mid - std_mult * std
