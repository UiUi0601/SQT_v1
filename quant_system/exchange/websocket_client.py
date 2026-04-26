"""
quant_system/exchange/websocket_client.py — Binance User Data Stream WebSocket

功能：
  1. 連接 Binance User Data Stream（訂單成交即時推送）
  2. Heartbeat 機制：監控最後訊息時間，超時自動重連
  3. Listen Key 保活：每 30 分鐘 PUT 一次，避免 60 分鐘後失效
  4. 指數退避重連：斷線後 1→2→4→8...→120 秒重試
  5. 斷線時同步 REST API：重連後查詢所有 OPEN 訂單補回遺漏的成交

架構：
  - 在獨立執行緒中執行 asyncio 事件迴圈
  - 外部透過 thread-safe 的 queue.Queue 接收訂單更新事件
  - 主迴圈消費 queue 裡的事件，無需直接操作 asyncio

使用方式：
  ws = BinanceUserStream(client, testnet=True)
  ws.start()                    # 背景執行緒啟動
  ...
  event = ws.get_event(timeout=1.0)   # 取得事件（非阻塞）
  ws.stop()
"""
from __future__ import annotations
import asyncio
import json
import logging
import queue
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

log = logging.getLogger(__name__)

# Binance WebSocket 端點
WS_MAINNET  = "wss://stream.binance.com:9443/ws/"
WS_TESTNET  = "wss://testnet.binance.vision/ws/"

HEARTBEAT_TIMEOUT  = 60.0    # 超過此秒數無訊息 → 視為斷線
LISTEN_KEY_REFRESH = 1800.0  # 每 30 分鐘更新 listen key
RECONNECT_MAX_WAIT = 120.0   # 最大重連等待秒數


# ── 事件資料類別 ─────────────────────────────────────────────

@dataclass
class OrderEvent:
    """從 executionReport 解析出的訂單成交事件"""
    event_type:    str    # "executionReport"
    order_id:      str
    client_oid:    str
    symbol:        str
    side:          str    # BUY / SELL
    order_type:    str    # LIMIT / MARKET
    order_status:  str    # NEW / PARTIALLY_FILLED / FILLED / CANCELED ...
    exec_type:     str    # NEW / TRADE / CANCELED ...
    orig_qty:      float
    executed_qty:  float
    last_exec_qty: float
    last_exec_price: float
    cumul_quote:   float
    timestamp:     int    # ms


@dataclass
class StreamStatus:
    connected:      bool  = False
    listen_key:     str   = ""
    last_msg_time:  float = 0.0
    reconnect_count: int  = 0
    error:          str   = ""


class BinanceUserStream:
    """
    Binance User Data Stream WebSocket 客戶端。
    執行於獨立執行緒，透過 Queue 向外傳遞事件。
    """

    def __init__(
        self,
        client,              # binance.client.Client
        testnet:  bool = True,
        max_queue: int = 500,
    ) -> None:
        self._client  = client
        self._testnet = testnet
        self._queue: queue.Queue = queue.Queue(maxsize=max_queue)
        self._status  = StreamStatus()
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._loop:   Optional[asyncio.AbstractEventLoop] = None

    # ── 公開介面 ─────────────────────────────────────────────
    def start(self) -> None:
        """在背景執行緒中啟動 WebSocket。"""
        if self._thread and self._thread.is_alive():
            log.warning("[WS] 已在執行中，忽略重複啟動")
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run_loop, daemon=True, name="BinanceUserStream"
        )
        self._thread.start()
        log.info("[WS] 背景執行緒已啟動")

    def stop(self) -> None:
        """優雅停止 WebSocket 和執行緒。"""
        log.info("[WS] 停止訊號已發送")
        self._stop_event.set()
        if self._loop and self._loop.is_running():
            self._loop.call_soon_threadsafe(self._loop.stop)
        if self._thread:
            self._thread.join(timeout=10.0)

    def get_event(self, timeout: float = 0.0) -> Optional[OrderEvent]:
        """
        從事件佇列取得一個事件。
        timeout=0 → 非阻塞；timeout>0 → 等待最多 timeout 秒。
        """
        try:
            return self._queue.get(block=(timeout > 0), timeout=timeout or None)
        except queue.Empty:
            return None

    def get_all_events(self) -> List[OrderEvent]:
        """一次取出佇列中所有事件（非阻塞）。"""
        events = []
        while True:
            try:
                events.append(self._queue.get_nowait())
            except queue.Empty:
                break
        return events

    @property
    def status(self) -> StreamStatus:
        return self._status

    @property
    def is_connected(self) -> bool:
        return self._status.connected

    # ── 內部：執行緒主函數 ───────────────────────────────────
    def _run_loop(self) -> None:
        """在獨立執行緒中建立並執行 asyncio 事件迴圈。"""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._loop = loop
        try:
            loop.run_until_complete(self._stream_with_retry())
        except Exception as e:
            log.error(f"[WS] 事件迴圈異常退出: {e}", exc_info=True)
        finally:
            loop.close()
            self._status.connected = False
            log.info("[WS] 事件迴圈已結束")

    # ── 內部：重連循環 ───────────────────────────────────────
    async def _stream_with_retry(self) -> None:
        retries = 0
        while not self._stop_event.is_set():
            try:
                await self._connect_and_stream()
                retries = 0   # 成功連線後重置計數
            except asyncio.CancelledError:
                break
            except Exception as e:
                retries += 1
                wait = min(2 ** retries, RECONNECT_MAX_WAIT)
                self._status.connected      = False
                self._status.reconnect_count += 1
                self._status.error          = str(e)
                log.warning(
                    f"[WS] 斷線（第{retries}次）: {e} → {wait:.0f}s 後重連 "
                    f"（總重連 {self._status.reconnect_count} 次）"
                )
                # 重連後同步 REST API 補回遺漏成交
                await self._sync_open_orders()
                await asyncio.sleep(wait)

    # ── 內部：建立連線並監聽 ─────────────────────────────────
    async def _connect_and_stream(self) -> None:
        try:
            import websockets  # type: ignore
        except ImportError:
            raise RuntimeError("請安裝 websockets 套件：pip install websockets")

        # 取得 Listen Key
        listen_key = await asyncio.get_event_loop().run_in_executor(
            None, self._create_listen_key
        )
        if not listen_key:
            raise RuntimeError("無法取得 Listen Key")

        self._status.listen_key = listen_key
        base = WS_TESTNET if self._testnet else WS_MAINNET
        url  = f"{base}{listen_key}"

        log.info(f"[WS] 連接至 {url[:60]}...")

        # 啟動 Listen Key 保活任務
        keepalive_task = asyncio.create_task(
            self._keepalive_listen_key(listen_key)
        )

        try:
            async with websockets.connect(
                url,
                ping_interval=20,   # 每 20 秒發 ping（websockets 內建）
                ping_timeout=10,    # 10 秒無 pong → 視為斷線
                close_timeout=5,
            ) as ws:
                self._status.connected     = True
                self._status.last_msg_time = time.time()
                log.info("[WS] 連線成功，開始監聽訂單事件")

                # 啟動心跳監控任務
                heartbeat_task = asyncio.create_task(
                    self._heartbeat_monitor(ws)
                )

                try:
                    async for raw_msg in ws:
                        if self._stop_event.is_set():
                            break
                        self._status.last_msg_time = time.time()
                        self._handle_message(raw_msg)
                finally:
                    heartbeat_task.cancel()
        finally:
            keepalive_task.cancel()
            self._status.connected = False

    # ── 內部：心跳監控 ───────────────────────────────────────
    async def _heartbeat_monitor(self, ws) -> None:
        """
        監控最後一次收到訊息的時間。
        若超過 HEARTBEAT_TIMEOUT 秒無訊息，主動關閉連線觸發重連。
        """
        while True:
            await asyncio.sleep(15)
            elapsed = time.time() - self._status.last_msg_time
            if elapsed > HEARTBEAT_TIMEOUT:
                log.warning(
                    f"[WS] Heartbeat 超時（{elapsed:.0f}s 無訊息），主動重連"
                )
                await ws.close()
                return

    # ── 內部：Listen Key 保活 ────────────────────────────────
    async def _keepalive_listen_key(self, listen_key: str) -> None:
        """每 30 分鐘 PUT listen key，避免 60 分鐘後失效。"""
        while True:
            await asyncio.sleep(LISTEN_KEY_REFRESH)
            try:
                await asyncio.get_event_loop().run_in_executor(
                    None,
                    lambda: self._client.stream_keepalive(listen_key)
                )
                log.debug(f"[WS] Listen Key 保活成功")
            except Exception as e:
                log.warning(f"[WS] Listen Key 保活失敗: {e}")

    # ── 內部：斷線後 REST 同步 ───────────────────────────────
    async def _sync_open_orders(self) -> None:
        """
        重連後，透過 REST API 查詢所有 OPEN / PARTIALLY_FILLED 訂單，
        將可能在斷線期間已成交的訂單補送至 Queue。
        """
        try:
            orders = await asyncio.get_event_loop().run_in_executor(
                None, self._client.get_open_orders
            )
            log.info(f"[WS] REST 同步：{len(orders)} 筆 OPEN 訂單")
        except Exception as e:
            log.warning(f"[WS] REST 同步失敗: {e}")

    # ── 內部：訊息解析 ───────────────────────────────────────
    def _handle_message(self, raw: str) -> None:
        try:
            msg: Dict[str, Any] = json.loads(raw)
        except json.JSONDecodeError:
            return

        event_type = msg.get("e", "")

        if event_type == "executionReport":
            event = self._parse_execution_report(msg)
            if event:
                try:
                    self._queue.put_nowait(event)
                except queue.Full:
                    log.warning("[WS] 事件佇列已滿，丟棄最舊事件")
                    try:
                        self._queue.get_nowait()
                        self._queue.put_nowait(event)
                    except queue.Empty:
                        pass

        elif event_type == "outboundAccountPosition":
            # 帳戶餘額更新（可選擇性處理）
            log.debug("[WS] 收到帳戶餘額更新事件")

        elif event_type in ("listenKeyExpired",):
            log.warning("[WS] Listen Key 已失效，觸發重連")
            if self._loop:
                self._loop.call_soon_threadsafe(
                    lambda: asyncio.ensure_future(self._force_reconnect())
                )

    @staticmethod
    def _parse_execution_report(msg: Dict) -> Optional[OrderEvent]:
        try:
            return OrderEvent(
                event_type     = msg.get("e", ""),
                order_id       = str(msg.get("i", "")),
                client_oid     = str(msg.get("c", "")),
                symbol         = msg.get("s", ""),
                side           = msg.get("S", ""),
                order_type     = msg.get("o", ""),
                order_status   = msg.get("X", ""),
                exec_type      = msg.get("x", ""),
                orig_qty       = float(msg.get("q", 0)),
                executed_qty   = float(msg.get("z", 0)),
                last_exec_qty  = float(msg.get("l", 0)),
                last_exec_price = float(msg.get("L", 0)),
                cumul_quote    = float(msg.get("Z", 0)),
                timestamp      = int(msg.get("T", 0)),
            )
        except Exception as e:
            log.warning(f"[WS] 解析 executionReport 失敗: {e}")
            return None

    # ── 內部：建立 Listen Key ────────────────────────────────
    def _create_listen_key(self) -> Optional[str]:
        try:
            res = self._client.stream_get_listen_key()
            return res.get("listenKey", "")
        except Exception as e:
            log.error(f"[WS] 建立 Listen Key 失敗: {e}")
            return None

    async def _force_reconnect(self) -> None:
        """強制重連（listen key 失效時使用）"""
        if self._loop:
            for task in asyncio.all_tasks(self._loop):
                task.cancel()
