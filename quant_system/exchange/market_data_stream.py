"""
quant_system/exchange/market_data_stream.py — Binance 期貨行情 WebSocket 串流

連接至 wss://fstream.binance.com/stream（U 本位永續合約 Combined Stream）。
支援動態訂閱/取消訂閱，無需重連。

功能：
  - Kline（OHLCV）快取：保留最近 500 根 K 棒
  - Depth 5檔快照：即時更新最優 bid/ask
  - 動態 subscribe/unsubscribe（SUBSCRIBE 方法訊息）
  - 斷線重連（指數退避，最長 120 秒，保留現有訂閱）
  - API weight 監控：超限時拒絕新訂閱

保留原則：
  - 不刪除連線錯誤重試與日誌記錄功能（遵循 CLAUDE.md 要求）
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import threading
import time
from collections import deque
from typing import Dict, List, Optional, Set

log = logging.getLogger(__name__)

_STREAM_URL_PROD    = "wss://fstream.binance.com/stream"
_STREAM_URL_TESTNET = "wss://stream.binancefuture.com/stream"

_DEFAULT_INTERVALS = ("1m", "15m")
_DEFAULT_DEPTH     = "depth5@100ms"
_KLINE_BUFFER      = 500   # 每個 symbol/interval 保留最近 N 根 K 棒
_MAX_WEIGHT        = 1800  # 每分鐘 API weight 上限（守護 IP ban）


# ────────────────────────────────────────────────────────────
class KlineBar:
    """單根 K 棒資料容器。"""
    __slots__ = ("open_time", "open", "high", "low", "close", "volume",
                 "close_time", "is_closed")

    def __init__(self, k: dict) -> None:
        self.open_time  = int(k["t"])
        self.open       = float(k["o"])
        self.high       = float(k["h"])
        self.low        = float(k["l"])
        self.close      = float(k["c"])
        self.volume     = float(k["v"])
        self.close_time = int(k["T"])
        self.is_closed  = bool(k.get("x", False))

    def to_dict(self) -> dict:
        return {
            "Open":   self.open,  "High":   self.high,
            "Low":    self.low,   "Close":  self.close,
            "Volume": self.volume,
            "open_time": self.open_time, "close_time": self.close_time,
        }


class DepthSnapshot:
    """最優 N 檔 bid/ask 快照。"""
    __slots__ = ("bids", "asks", "ts")

    def __init__(self, bids: list, asks: list) -> None:
        self.bids = bids   # [[price_str, qty_str], ...]
        self.asks = asks
        self.ts   = time.time()


# ────────────────────────────────────────────────────────────
class MarketDataStream:
    """
    Binance 期貨行情 Combined WebSocket 串流管理器。

    用法：
        stream = MarketDataStream(testnet=True)
        stream.start()
        stream.subscribe("BTCUSDT")
        ...
        df = stream.get_ohlcv_df("BTCUSDT", "1m", limit=100)
        depth = stream.get_depth("BTCUSDT")
        stream.stop()
    """

    def __init__(
        self,
        testnet:   bool       = True,
        intervals: tuple      = _DEFAULT_INTERVALS,
        depth_sub: str        = _DEFAULT_DEPTH,
    ) -> None:
        self._url      = _STREAM_URL_TESTNET if testnet else _STREAM_URL_PROD
        self._intervals = intervals
        self._depth_sub = depth_sub

        # 資料緩衝區
        self._klines: Dict[str, deque] = {}   # key "BTCUSDT_1m" → deque[KlineBar]
        self._depth:  Dict[str, DepthSnapshot] = {}  # key "BTCUSDT" → DepthSnapshot

        # 訂閱狀態
        self._subscribed_symbols: Set[str] = set()
        self._lock = threading.Lock()

        # API weight 追蹤（每分鐘視窗）
        self._weight_count: int   = 0
        self._weight_reset: float = time.time() + 60.0

        # 非同步事件迴圈（背景執行緒）
        self._loop:   Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread]          = None
        self._running = False
        self._ws      = None     # websockets connection handle
        self._msg_id  = 0        # 用於 SUBSCRIBE/UNSUBSCRIBE 訊息的遞增 ID

    # ════════════════════════════════════════════════════════
    # 公開介面
    # ════════════════════════════════════════════════════════
    def start(self) -> None:
        """啟動背景執行緒與 WebSocket 連線。"""
        if self._running:
            return
        self._running = True
        self._loop    = asyncio.new_event_loop()
        self._thread  = threading.Thread(
            target=self._run_loop, name="mds-loop", daemon=True
        )
        self._thread.start()
        for _ in range(50):
            if self._loop.is_running():
                break
            time.sleep(0.02)
        log.info("[MDS] MarketDataStream 已啟動 → %s", self._url)

    def stop(self) -> None:
        """停止 WebSocket 連線與背景執行緒。"""
        self._running = False
        if self._loop and self._loop.is_running():
            asyncio.run_coroutine_threadsafe(self._disconnect(), self._loop)
        if self._thread:
            self._thread.join(timeout=10)
        log.info("[MDS] MarketDataStream 已停止")

    def subscribe(self, symbol: str,
                  intervals: Optional[tuple] = None,
                  depth: Optional[str] = None) -> bool:
        """
        動態訂閱一個幣種的 K 線與深度串流。
        若 WebSocket 尚未建立（冷啟動），訂閱記錄會在連線後自動送出。

        回傳 True = 成功排入訂閱；False = weight 超限拒絕。
        """
        sym = symbol.upper()
        if not self._check_weight(add=1):
            log.warning("[MDS] API weight 超限，拒絕訂閱 %s", sym)
            return False

        with self._lock:
            if sym in self._subscribed_symbols:
                return True
            self._subscribed_symbols.add(sym)
            # 初始化緩衝區
            for iv in (intervals or self._intervals):
                self._klines[f"{sym}_{iv}"] = deque(maxlen=_KLINE_BUFFER)
            self._depth[sym] = DepthSnapshot([], [])

        # 若 WS 已連線，即時送 SUBSCRIBE 訊息（無需重連）
        if self._loop and self._loop.is_running():
            params = self._build_params(sym, intervals, depth)
            asyncio.run_coroutine_threadsafe(
                self._send_subscribe(params), self._loop
            )
        log.info("[MDS] 訂閱 %s 完成", sym)
        return True

    def unsubscribe(self, symbol: str) -> None:
        """動態取消訂閱。"""
        sym = symbol.upper()
        with self._lock:
            if sym not in self._subscribed_symbols:
                return
            self._subscribed_symbols.discard(sym)
            for iv in self._intervals:
                self._klines.pop(f"{sym}_{iv}", None)
            self._depth.pop(sym, None)

        if self._loop and self._loop.is_running():
            params = self._build_params(sym)
            asyncio.run_coroutine_threadsafe(
                self._send_unsubscribe(params), self._loop
            )
        log.info("[MDS] 取消訂閱 %s", sym)

    # ── 資料查詢 ─────────────────────────────────────────────
    def get_kline(self, symbol: str, interval: str = "1m") -> Optional[KlineBar]:
        """回傳最新一根已收盤的 K 棒（is_closed=True）。"""
        key  = f"{symbol.upper()}_{interval}"
        buf  = self._klines.get(key)
        if not buf:
            return None
        for bar in reversed(buf):
            if bar.is_closed:
                return bar
        return buf[-1] if buf else None

    def get_depth(self, symbol: str) -> Optional[DepthSnapshot]:
        """回傳最新深度快照。"""
        return self._depth.get(symbol.upper())

    def get_ohlcv_df(self, symbol: str, interval: str = "1m",
                     limit: int = 200):
        """
        回傳最近 limit 根 K 棒的 pandas DataFrame（欄位與 DataCache 相容）。
        若無快取資料，回傳 None。
        """
        try:
            import pandas as pd
        except ImportError:
            return None

        key = f"{symbol.upper()}_{interval}"
        buf = self._klines.get(key)
        if not buf or len(buf) < 2:
            return None

        rows = list(buf)[-limit:]
        df = pd.DataFrame([b.to_dict() for b in rows])
        df.rename(columns={"open_time": "Open Time"}, inplace=True)
        df.index = pd.to_datetime(df["Open Time"], unit="ms", utc=True)
        return df

    @property
    def subscribed_symbols(self) -> Set[str]:
        with self._lock:
            return set(self._subscribed_symbols)

    # ════════════════════════════════════════════════════════
    # 內部：WebSocket 連線與重連
    # ════════════════════════════════════════════════════════
    def _run_loop(self) -> None:
        asyncio.set_event_loop(self._loop)
        self._loop.run_until_complete(self._connect_loop())

    async def _connect_loop(self) -> None:
        retries = 0
        while self._running:
            try:
                await self._connect_and_listen()
                retries = 0
            except Exception as e:
                if not self._running:
                    break
                wait = min(2 ** retries, 120.0)
                log.warning(
                    "[MDS] 斷線 (retry #%d): %s → %.0fs 後重連",
                    retries + 1, e, wait
                )
                retries += 1
                await asyncio.sleep(wait)

    async def _connect_and_listen(self) -> None:
        try:
            import websockets  # type: ignore
        except ImportError:
            log.error("[MDS] websockets 未安裝，請執行：pip install websockets")
            await asyncio.sleep(60)
            return

        log.info("[MDS] 連線至 %s", self._url)
        async with websockets.connect(
            self._url,
            ping_interval=20,
            ping_timeout=10,
            open_timeout=10,
        ) as ws:
            self._ws = ws
            # 連線後重新訂閱所有已記錄的幣種
            with self._lock:
                syms = list(self._subscribed_symbols)
            if syms:
                all_params = []
                for sym in syms:
                    all_params.extend(self._build_params(sym))
                await self._send_subscribe(all_params)

            async for raw in ws:
                if not self._running:
                    break
                try:
                    msg = json.loads(raw)
                    await self._handle_message(msg)
                except json.JSONDecodeError:
                    pass
                except Exception as e:
                    log.debug("[MDS] 訊息處理錯誤: %s", e)

        self._ws = None

    async def _disconnect(self) -> None:
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass

    # ── 訊息分發 ─────────────────────────────────────────────
    async def _handle_message(self, msg: dict) -> None:
        stream = msg.get("stream", "")
        data   = msg.get("data", msg)   # combined vs single stream

        if "@kline_" in stream:
            self._handle_kline(data)
        elif "@depth" in stream:
            self._handle_depth(data)
        # subscription ACK / ping — 靜默忽略

    def _handle_kline(self, data: dict) -> None:
        k   = data.get("k", {})
        sym = data.get("s", k.get("s", "")).upper()
        iv  = k.get("i", "1m")
        if not sym:
            return
        key = f"{sym}_{iv}"
        bar = KlineBar(k)
        buf = self._klines.get(key)
        if buf is None:
            return
        if buf and buf[-1].open_time == bar.open_time:
            buf[-1] = bar   # 更新同一根未收盤 K 棒
        else:
            buf.append(bar)

    def _handle_depth(self, data: dict) -> None:
        sym = data.get("s", "").upper()
        if not sym:
            return
        snap = self._depth.get(sym)
        if snap is None:
            return
        snap.bids = data.get("b", snap.bids)
        snap.asks = data.get("a", snap.asks)
        snap.ts   = time.time()

    # ── 訂閱管理訊息 ─────────────────────────────────────────
    def _build_params(self, symbol: str,
                      intervals: Optional[tuple] = None,
                      depth: Optional[str] = None) -> List[str]:
        sym_lower = symbol.lower()
        ivs = intervals or self._intervals
        params = [f"{sym_lower}@kline_{iv}" for iv in ivs]
        params.append(f"{sym_lower}@{depth or self._depth_sub}")
        return params

    async def _send_subscribe(self, params: list) -> None:
        if not self._ws or not params:
            return
        self._msg_id += 1
        msg = {"method": "SUBSCRIBE", "params": params, "id": self._msg_id}
        try:
            await self._ws.send(json.dumps(msg))
            log.debug("[MDS] SUBSCRIBE → %s", params)
        except Exception as e:
            log.warning("[MDS] SUBSCRIBE 送出失敗: %s", e)

    async def _send_unsubscribe(self, params: list) -> None:
        if not self._ws or not params:
            return
        self._msg_id += 1
        msg = {"method": "UNSUBSCRIBE", "params": params, "id": self._msg_id}
        try:
            await self._ws.send(json.dumps(msg))
            log.debug("[MDS] UNSUBSCRIBE → %s", params)
        except Exception as e:
            log.warning("[MDS] UNSUBSCRIBE 送出失敗: %s", e)

    # ── Weight 監控 ──────────────────────────────────────────
    def _check_weight(self, add: int = 1) -> bool:
        now = time.time()
        if now > self._weight_reset:
            self._weight_count = 0
            self._weight_reset = now + 60.0
        self._weight_count += add
        return self._weight_count <= _MAX_WEIGHT
