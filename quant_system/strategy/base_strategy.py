"""
quant_system/strategy/base_strategy.py — 策略基底類別
"""
from __future__ import annotations
import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, Optional

import pandas as pd


@dataclass
class SignalResult:
    signal:         str   = "HOLD"   # BUY | SELL | HOLD
    confidence:     float = 0.0      # 0~1
    strategy:       str   = ""
    reason:         str   = ""       # signal_reason：人類可讀觸發說明
    indicators:     Dict  = field(default_factory=dict)   # indicator_data


@dataclass
class TradeSignal:
    """
    SEMI_AUTO 確認流程的待下單訊號。

    由 GridBotInstance 在 SEMI_AUTO 執行階段建立，
    透過 /ws/signals 推送至 UI，等待人工確認。
    """
    symbol:          str
    side:            str             # BUY | SELL
    price:           float
    quantity:        float
    market_type:     str   = "SPOT"
    leverage:        int   = 1
    indicator_data:  Dict  = field(default_factory=dict)
    signal_reason:   str   = ""
    slippage_tolerance_pct: float = 0.005   # 確認時容許的最大滑點（0.5%）

    # 系統欄位（自動填充）
    signal_id:   str   = field(default_factory=lambda: uuid.uuid4().hex[:16])
    expires_at:  float = field(default_factory=lambda: time.time() + float(
        __import__("os").getenv("SIGNAL_EXPIRE_SECONDS", "30")
    ))
    created_at:  float = field(default_factory=time.time)

    def to_dict(self) -> Dict:
        return {
            "signal_id":              self.signal_id,
            "symbol":                 self.symbol,
            "side":                   self.side,
            "price":                  self.price,
            "quantity":               self.quantity,
            "market_type":            self.market_type,
            "leverage":               self.leverage,
            "indicator_data":         self.indicator_data,
            "signal_reason":          self.signal_reason,
            "slippage_tolerance_pct": self.slippage_tolerance_pct,
            "expires_at":             self.expires_at,
            "created_at":             self.created_at,
        }


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
