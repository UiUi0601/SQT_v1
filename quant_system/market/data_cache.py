"""
quant_system/market/data_cache.py — 多時框資料快取

資料來源優先順序：
  1. MarketDataStream（WS 即時串流，需先呼叫 set_stream()）
  2. Binance REST /fapi/v1/klines（WS 無資料時的冷啟動種子）
  3. yfinance（最後備援，僅在 REST 也失敗時使用）

切換說明：
  - 呼叫 set_stream(market_data_stream) 後，get() / get_multi_timeframe()
    將優先從 WS 串流緩衝區讀取，避免 yfinance 延遲與 rate-limit。
  - WS 緩衝區資料不足（<5 根）時，自動回退至 REST 種子載入。
"""
from __future__ import annotations

import logging
import os
import time
from typing import Dict, Optional, TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from quant_system.exchange.market_data_stream import MarketDataStream

log = logging.getLogger(__name__)

_TESTNET = os.getenv("BINANCE_TESTNET", "true").lower() == "true"


class DataCache:
    """快取多時框 OHLCV 資料。"""

    def __init__(self, ttl_seconds: int = 60) -> None:
        self._ttl    = ttl_seconds
        self._store: Dict[str, Dict] = {}   # key → {df, ts}
        self._stream: Optional["MarketDataStream"] = None

    def set_stream(self, stream: "MarketDataStream") -> None:
        """
        注入 MarketDataStream 實例。
        注入後 get() 將優先從 WS 緩衝區讀取。
        """
        self._stream = stream
        log.info("[DataCache] MarketDataStream 已注入，切換至 WS 資料來源")

    # ── public ───────────────────────────────────────────────
    def get(self, symbol: str, period: str = "5d",
            interval: str = "5m") -> Optional[pd.DataFrame]:
        key = f"{symbol}_{interval}"

        # ① 嘗試從 WS 串流讀取
        if self._stream is not None:
            df_ws = self._stream.get_ohlcv_df(symbol, interval, limit=500)
            if df_ws is not None and len(df_ws) >= 5:
                self._store[key] = {"df": df_ws, "ts": time.time()}
                return df_ws
            # WS 資料不足 → 走 REST 種子，然後快取
            log.debug(
                "[DataCache] %s %s WS 資料不足（%d 根），嘗試 REST 種子",
                symbol, interval, len(df_ws) if df_ws is not None else 0,
            )

        # ② 快取有效期內直接回傳
        cached = self._store.get(key)
        if cached and (time.time() - cached["ts"] < self._ttl):
            return cached["df"]

        # ③ 從 Binance REST 載入種子資料
        df = self._fetch_rest(symbol, interval)
        if df is not None and not df.empty:
            self._store[key] = {"df": df, "ts": time.time()}
            return df

        # ④ 最後備援：yfinance（若 REST 也失敗）
        df = self._fetch_yfinance(symbol, period, interval)
        if df is not None:
            self._store[key] = {"df": df, "ts": time.time()}
        return df

    def get_multi_timeframe(
        self, symbol: str, timeframes: Dict[str, Dict]
    ) -> Dict[str, Optional[pd.DataFrame]]:
        result = {}
        for tf, cfg in timeframes.items():
            result[tf] = self.get(symbol, cfg.get("period", "5d"),
                                  cfg.get("interval", tf))
        return result

    def invalidate(self, symbol: str) -> None:
        keys = [k for k in self._store if k.startswith(symbol + "_")]
        for k in keys:
            del self._store[k]

    # ── internal ─────────────────────────────────────────────
    @staticmethod
    def _fetch_rest(symbol: str, interval: str,
                    limit: int = 500) -> Optional[pd.DataFrame]:
        """
        從 Binance fapi REST 載入歷史 K 線（冷啟動種子）。
        不依賴 yfinance，直接使用標準函式庫 urllib。
        """
        import urllib.request
        import json as _json

        base = ("https://testnet.binancefuture.com"
                if _TESTNET else "https://fapi.binance.com")
        url  = (f"{base}/fapi/v1/klines"
                f"?symbol={symbol.upper()}&interval={interval}&limit={limit}")
        try:
            with urllib.request.urlopen(url, timeout=10) as resp:
                raw: list = _json.loads(resp.read())
        except Exception as e:
            log.warning("[DataCache] REST 種子載入失敗 %s %s: %s", symbol, interval, e)
            return None

        if not raw:
            return None

        rows = []
        for k in raw:
            rows.append({
                "Open Time": int(k[0]),
                "Open":      float(k[1]),
                "High":      float(k[2]),
                "Low":       float(k[3]),
                "Close":     float(k[4]),
                "Volume":    float(k[5]),
            })
        df = pd.DataFrame(rows)
        df.index = pd.to_datetime(df["Open Time"], unit="ms", utc=True)
        return df

    @staticmethod
    def _fetch_yfinance(symbol: str, period: str,
                        interval: str) -> Optional[pd.DataFrame]:
        """yfinance 備援（僅在 REST 失敗時使用）。"""
        try:
            import yfinance as yf  # type: ignore
            df = yf.download(symbol, period=period, interval=interval,
                             progress=False, auto_adjust=True)
            if df is None or df.empty:
                log.warning("[DataCache] yfinance %s %s 無資料", symbol, interval)
                return None
            df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]
            df.dropna(inplace=True)
            return df
        except Exception as e:
            log.error("[DataCache] yfinance 備援失敗 %s %s: %s", symbol, interval, e)
            return None
