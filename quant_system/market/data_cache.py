"""
quant_system/market/data_cache.py — 多時框資料快取（yfinance）
"""
from __future__ import annotations
import logging
import time
from typing import Dict, Optional

import pandas as pd

log = logging.getLogger(__name__)


class DataCache:
    """快取多時框 OHLCV 資料，避免頻繁呼叫 yfinance。"""

    def __init__(self, ttl_seconds: int = 60) -> None:
        self._ttl   = ttl_seconds
        self._store: Dict[str, Dict] = {}   # key → {df, ts}

    # ── public ───────────────────────────────────────────────
    def get(self, symbol: str, period: str = "5d", interval: str = "5m") -> Optional[pd.DataFrame]:
        key = f"{symbol}_{interval}"
        cached = self._store.get(key)
        if cached and (time.time() - cached["ts"] < self._ttl):
            return cached["df"]
        df = self._fetch(symbol, period, interval)
        if df is not None:
            self._store[key] = {"df": df, "ts": time.time()}
        return df

    def get_multi_timeframe(
        self, symbol: str, timeframes: Dict[str, Dict]
    ) -> Dict[str, Optional[pd.DataFrame]]:
        result = {}
        for tf, cfg in timeframes.items():
            result[tf] = self.get(symbol, cfg.get("period", "5d"), cfg.get("interval", tf))
        return result

    def invalidate(self, symbol: str) -> None:
        keys = [k for k in self._store if k.startswith(symbol + "_")]
        for k in keys:
            del self._store[k]

    # ── internal ─────────────────────────────────────────────
    @staticmethod
    def _fetch(symbol: str, period: str, interval: str) -> Optional[pd.DataFrame]:
        try:
            import yfinance as yf  # type: ignore
            df = yf.download(symbol, period=period, interval=interval,
                             progress=False, auto_adjust=True)
            if df is None or df.empty:
                log.warning(f"[DataCache] {symbol} {interval} 無資料")
                return None
            df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]
            df.dropna(inplace=True)
            return df
        except Exception as e:
            log.error(f"[DataCache] fetch 失敗 {symbol} {interval}: {e}")
            return None
