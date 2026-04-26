"""
quant_system/exchange/websocket_client.py -- 幣安三業務線 User Data Stream

支援 SPOT / FUTURES / MARGIN 三條串流，Listen Key 每 30 分鐘自動 PUT 保活。

修改記錄（TLS SNI / 簽章統一）：
  - _connect_and_stream：使用 ssl.create_default_context() + 明確 server_hostname，
    符合 2026 TLS SNI 規範，解決無數據流問題
  - 新增 async_client 參數：持有 AsyncExchangeClient 實例供 REST 補單同步使用
  - 移除 _signed_get 同步方法：簽章邏輯統一由 AsyncExchangeClient._sign() 負責，
    確保 HMAC-SHA256 與時間偏移 (time_offset) 和主客戶端完全一致
  - _sync_open_orders_rest：改呼叫 async_client 的對應方法（含 time_offset）
"""
from __future__ import annotations
import asyncio, json, logging, queue, ssl, threading, time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode, urlparse

log = logging.getLogger(__name__)


class StreamMarket(str, Enum):
    SPOT    = "SPOT"
    FUTURES = "FUTURES"
    MARGIN  = "MARGIN"


_WS_ENDPOINTS = {
    StreamMarket.SPOT:    "wss://stream.binance.com:9443/ws/",
    StreamMarket.FUTURES: "wss://fstream.binance.com/ws/",
    StreamMarket.MARGIN:  "wss://stream.binance.com:9443/ws/",
}
_WS_TESTNET = {
    StreamMarket.SPOT:    "wss://testnet.binance.vision/ws/",
    StreamMarket.FUTURES: "wss://stream.binancefuture.com/ws/",
    StreamMarket.MARGIN:  "wss://testnet.binance.vision/ws/",
}
_LISTEN_KEY_CREATE = {
    StreamMarket.SPOT:    "https://api.binance.com/api/v3/userDataStream",
    StreamMarket.FUTURES: "https://fapi.binance.com/fapi/v1/listenKey",
    StreamMarket.MARGIN:  "https://api.binance.com/sapi/v1/userDataStream",
}
_LISTEN_KEY_KEEPALIVE = {
    StreamMarket.SPOT:    "https://api.binance.com/api/v3/userDataStream",
    StreamMarket.FUTURES: "https://fapi.binance.com/fapi/v1/listenKey",
    StreamMarket.MARGIN:  "https://api.binance.com/sapi/v1/userDataStream",
}
HEARTBEAT_TIMEOUT  = 60.0
LISTEN_KEY_REFRESH = 1800.0
RECONNECT_MAX_WAIT = 120.0


@dataclass
class OrderEvent:
    event_type: str; order_id: str; client_oid: str; symbol: str
    side: str; order_type: str; order_status: str; exec_type: str
    orig_qty: float; executed_qty: float; last_exec_qty: float
    last_exec_price: float; cumul_quote: float; timestamp: int
    market: str = "SPOT"


@dataclass
class FuturesOrderEvent:
    event_type: str; order_id: str; client_oid: str; symbol: str
    side: str; order_type: str; order_status: str; exec_type: str
    orig_qty: float; avg_price: float; last_fill_qty: float
    last_fill_price: float; realized_pnl: float; position_side: str; timestamp: int


@dataclass
class AccountUpdateEvent:
    event_type: str
    balances:   List[Dict[str, Any]] = field(default_factory=list)
    positions:  List[Dict[str, Any]] = field(default_factory=list)
    timestamp:  int = 0


@dataclass
class BalanceEvent:
    event_type: str
    balances:   List[Dict[str, Any]] = field(default_factory=list)
    timestamp:  int = 0


@dataclass
class StreamStatus:
    connected: bool = False; listen_key: str = ""; last_msg_time: float = 0.0
    reconnect_count: int = 0; error: str = ""; market: str = "SPOT"


class BinanceUserStream:
    """幣安 User Data Stream WebSocket 客戶端（三業務線）。"""

    def __init__(self, api_key, api_secret, market=StreamMarket.SPOT,
                 testnet=False, max_queue=500, isolated_symbol=None,
                 async_client=None):
        """
        Parameters
        ----------
        async_client : AsyncExchangeClient（選用）
            若傳入，斷線重連時 REST 補單同步將透過它執行，
            確保 HMAC 簽章邏輯與時間偏移和主客戶端一致。
        """
        self._api_key         = api_key
        self._api_secret      = api_secret
        self._market          = market
        self._testnet         = testnet
        self._isolated_symbol = isolated_symbol
        self._async_client    = async_client   # AsyncExchangeClient 實例（可為 None）
        self._queue           = queue.Queue(maxsize=max_queue)
        self._status          = StreamStatus(market=market.value)
        self._stop_event      = threading.Event()
        self._thread          = None
        self._loop            = None

    def start(self):
        if self._thread and self._thread.is_alive():
            log.warning(f"[WS-{self._market.value}] 已在執行中")
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run_loop, daemon=True,
            name=f"BinanceStream-{self._market.value}")
        self._thread.start()
        log.info(f"[WS-{self._market.value}] 背景執行緒已啟動")

    def stop(self):
        self._stop_event.set()
        if self._loop and self._loop.is_running():
            self._loop.call_soon_threadsafe(self._loop.stop)
        if self._thread:
            self._thread.join(timeout=10.0)

    def get_event(self, timeout=0.0):
        try:
            return self._queue.get(block=(timeout > 0), timeout=timeout or None)
        except queue.Empty:
            return None

    def get_all_events(self):
        events = []
        while True:
            try:
                events.append(self._queue.get_nowait())
            except queue.Empty:
                break
        return events

    @property
    def status(self): return self._status
    @property
    def is_connected(self): return self._status.connected

    def _run_loop(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._loop = loop
        try:
            loop.run_until_complete(self._stream_with_retry())
        except Exception as e:
            log.error(f"[WS-{self._market.value}] 事件迴圈異常: {e}", exc_info=True)
        finally:
            loop.close()
            self._status.connected = False

    async def _stream_with_retry(self):
        retries = 0
        while not self._stop_event.is_set():
            try:
                await self._connect_and_stream()
                retries = 0
            except asyncio.CancelledError:
                break
            except Exception as e:
                retries += 1
                wait = min(2 ** retries, RECONNECT_MAX_WAIT)
                self._status.connected       = False
                self._status.reconnect_count += 1
                self._status.error           = str(e)
                log.warning(f"[WS-{self._market.value}] 斷線: {e} -> {wait:.0f}s 後重連")
                await self._sync_open_orders_rest()
                await asyncio.sleep(wait)

    async def _connect_and_stream(self):
        try:
            import websockets
        except ImportError:
            raise RuntimeError("請安裝 websockets：pip install websockets")

        listen_key = await asyncio.get_event_loop().run_in_executor(
            None, self._create_listen_key)
        if not listen_key:
            raise RuntimeError(f"[WS-{self._market.value}] 無法取得 Listen Key")
        self._status.listen_key = listen_key

        ep_map   = _WS_TESTNET if self._testnet else _WS_ENDPOINTS
        url      = f"{ep_map[self._market]}{listen_key}"
        log.info(f"[WS-{self._market.value}] 連接至 {url[:70]}...")

        # TLS SNI：明確傳入 server_hostname，符合 2026 TLS 規範
        ssl_ctx  = ssl.create_default_context()
        hostname = urlparse(url).hostname or ""

        keepalive_task = asyncio.create_task(self._keepalive_listen_key(listen_key))
        try:
            async with websockets.connect(
                url,
                ssl=ssl_ctx,
                server_hostname=hostname,
                ping_interval=20,
                ping_timeout=10,
                close_timeout=5,
            ) as ws:
                self._status.connected     = True
                self._status.last_msg_time = time.time()
                log.info(f"[WS-{self._market.value}] 連線成功")
                heartbeat_task = asyncio.create_task(self._heartbeat_monitor(ws))
                try:
                    async for raw_msg in ws:
                        if self._stop_event.is_set(): break
                        self._status.last_msg_time = time.time()
                        self._handle_message(raw_msg)
                finally:
                    heartbeat_task.cancel()
        finally:
            keepalive_task.cancel()
            self._status.connected = False

    async def _heartbeat_monitor(self, ws):
        while True:
            await asyncio.sleep(15)
            elapsed = time.time() - self._status.last_msg_time
            if elapsed > HEARTBEAT_TIMEOUT:
                log.warning(f"[WS-{self._market.value}] Heartbeat 超時，主動重連")
                await ws.close()
                return

    async def _keepalive_listen_key(self, listen_key):
        """每 30 分鐘 PUT listen key 以保活，避免 60 分鐘過期。"""
        while True:
            await asyncio.sleep(LISTEN_KEY_REFRESH)
            try:
                await asyncio.get_event_loop().run_in_executor(
                    None, lambda: self._put_listen_key(listen_key))
                log.debug(f"[WS-{self._market.value}] Listen Key 保活成功")
            except Exception as e:
                log.warning(f"[WS-{self._market.value}] Listen Key 保活失敗: {e}")

    def _put_listen_key(self, listen_key):
        """同步 PUT /xxx/userDataStream 續期 Listen Key。"""
        import urllib.request
        url  = _LISTEN_KEY_KEEPALIVE[self._market]
        data = urlencode({"listenKey": listen_key}).encode()
        req  = urllib.request.Request(url, data=data, method="PUT",
                                      headers={"X-MBX-APIKEY": self._api_key})
        with urllib.request.urlopen(req, timeout=5):
            pass

    async def _sync_open_orders_rest(self):
        """
        斷線重連後透過 REST 補抓 OPEN 訂單。
        若持有 AsyncExchangeClient 實例則透過它呼叫（簽章邏輯與時間偏移統一）；
        否則跳過（避免重複維護 HMAC 邏輯）。
        """
        if self._async_client is None:
            log.debug(f"[WS-{self._market.value}] 無 async_client，跳過 REST 補單同步")
            return
        try:
            if self._market == StreamMarket.FUTURES:
                orders = await self._async_client.get_futures_open_orders()
            elif self._market == StreamMarket.MARGIN:
                orders = await self._async_client.get_margin_open_orders()
            else:
                orders = await self._async_client.get_open_orders()
            log.info(f"[WS-{self._market.value}] REST 同步：{len(orders)} 筆 OPEN 訂單")
        except Exception as e:
            log.warning(f"[WS-{self._market.value}] REST 同步失敗: {e}")

    def _handle_message(self, raw):
        try:
            msg        = json.loads(raw)
        except json.JSONDecodeError:
            return
        event_type = msg.get("e", "")

        if event_type == "executionReport":
            ev = self._parse_execution_report(msg)
            if ev: self._enqueue(ev)
        elif event_type == "ORDER_TRADE_UPDATE":
            inner = msg.get("o", {})
            ev    = self._parse_futures_order(inner, msg.get("T", 0))
            if ev: self._enqueue(ev)
        elif event_type == "ACCOUNT_UPDATE":
            data = msg.get("a", {})
            self._enqueue(AccountUpdateEvent(
                event_type=event_type, balances=data.get("B", []),
                positions=data.get("P", []), timestamp=msg.get("T", 0)))
        elif event_type == "outboundAccountPosition":
            self._enqueue(BalanceEvent(
                event_type=event_type, balances=msg.get("B", []),
                timestamp=msg.get("u", 0)))
        elif event_type == "listenKeyExpired":
            log.warning(f"[WS-{self._market.value}] Listen Key 失效，觸發重連")
            if self._loop:
                self._loop.call_soon_threadsafe(
                    lambda: asyncio.ensure_future(self._force_reconnect()))

    def _enqueue(self, event):
        try:
            self._queue.put_nowait(event)
        except queue.Full:
            try:
                self._queue.get_nowait()
                self._queue.put_nowait(event)
            except queue.Empty:
                pass

    def _parse_execution_report(self, msg):
        try:
            return OrderEvent(
                event_type=msg.get("e",""), order_id=str(msg.get("i","")),
                client_oid=str(msg.get("c","")), symbol=msg.get("s",""),
                side=msg.get("S",""), order_type=msg.get("o",""),
                order_status=msg.get("X",""), exec_type=msg.get("x",""),
                orig_qty=float(msg.get("q",0)), executed_qty=float(msg.get("z",0)),
                last_exec_qty=float(msg.get("l",0)), last_exec_price=float(msg.get("L",0)),
                cumul_quote=float(msg.get("Z",0)), timestamp=int(msg.get("T",0)),
                market=self._market.value)
        except Exception as e:
            log.warning(f"[WS] executionReport 解析失敗: {e}"); return None

    @staticmethod
    def _parse_futures_order(o, ts):
        try:
            return FuturesOrderEvent(
                event_type="ORDER_TRADE_UPDATE", order_id=str(o.get("i","")),
                client_oid=str(o.get("c","")), symbol=o.get("s",""),
                side=o.get("S",""), order_type=o.get("o",""),
                order_status=o.get("X",""), exec_type=o.get("x",""),
                orig_qty=float(o.get("q",0)), avg_price=float(o.get("ap",0)),
                last_fill_qty=float(o.get("l",0)), last_fill_price=float(o.get("L",0)),
                realized_pnl=float(o.get("rp",0)), position_side=o.get("ps","BOTH"),
                timestamp=ts)
        except Exception as e:
            log.warning(f"[WS] ORDER_TRADE_UPDATE 解析失敗: {e}"); return None

    def _create_listen_key(self):
        import urllib.request
        url = _LISTEN_KEY_CREATE[self._market]
        if self._market == StreamMarket.MARGIN and self._isolated_symbol:
            params = urlencode({"isIsolated": "TRUE", "symbol": self._isolated_symbol})
            url  = f"https://api.binance.com/sapi/v1/userDataStream/isolated?{params}"
            data = None
        else:
            data = b""
        req = urllib.request.Request(url, data=data, method="POST",
                                     headers={"X-MBX-APIKEY": self._api_key})
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                result = json.loads(resp.read())
                return result.get("listenKey", "")
        except Exception as e:
            log.error(f"[WS-{self._market.value}] 建立 Listen Key 失敗: {e}"); return None

    async def _force_reconnect(self):
        if self._loop:
            for task in asyncio.all_tasks(self._loop):
                task.cancel()

    # _signed_get 已移除：簽章邏輯統一交由 AsyncExchangeClient._sign() 負責，
    # 確保時間偏移 (time_offset) 與 HMAC-SHA256 與主客戶端完全一致。
