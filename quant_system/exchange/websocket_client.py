"""
quant_system/exchange/websocket_client.py -- 幣安三業務線 User Data Stream

支援 SPOT / FUTURES / MARGIN 三條串流。

幣安 API 遷移記錄（2026-02-20）：
  - SPOT / MARGIN 的 REST listenKey 通道（POST /api/v3/userDataStream、
    POST /sapi/v1/userDataStream）已被幣安永久停用（HTTP 410 Gone）。
  - SPOT / MARGIN 改用幣安最新 WebSocket API 端點直連，連線後透過
    userDataStream.subscribe.signature 訊息完成 HMAC-SHA256 身分驗證，
    不再需要 listenKey。
  - FUTURES 依然支援 REST listenKey（POST /fapi/v1/listenKey），保持原邏輯。

TLS SNI 修正（2026）：
  - 明確指定 ssl.create_default_context() + server_hostname。

簽章統一：
  - async_client 參數（選用）供 REST 補單同步用，確保 time_offset 一致。
"""
from __future__ import annotations
import asyncio, hashlib, hmac, json, logging, queue, ssl, threading, time, uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode, urlparse

log = logging.getLogger(__name__)


class StreamMarket(str, Enum):
    SPOT    = "SPOT"
    FUTURES = "FUTURES"
    MARGIN  = "MARGIN"


# ── FUTURES 舊式 User Data Stream（listenKey 流程，仍有效）─────
_WS_ENDPOINTS = {
    StreamMarket.FUTURES: "wss://fstream.binance.com/ws/",
}
_WS_TESTNET = {
    StreamMarket.FUTURES: "wss://stream.binancefuture.com/ws/",
}
_LISTEN_KEY_CREATE_FUTURES    = "https://fapi.binance.com/fapi/v1/listenKey"
_LISTEN_KEY_KEEPALIVE_FUTURES = "https://fapi.binance.com/fapi/v1/listenKey"

# ── SPOT / MARGIN 新式 WebSocket API（2026-02-20 起，listenKey 已廢棄）──
_WS_API_MAINNET = "wss://ws-api.binance.com:443/ws-api/v3"
_WS_API_TESTNET = "wss://ws-api.testnet.binance.vision/ws-api/v3"

HEARTBEAT_TIMEOUT  = 60.0
LISTEN_KEY_REFRESH = 1800.0   # Futures listenKey 保活間隔（30 分鐘）
WS_API_REAUTH_SEC  = 3000.0   # WS API 重新驗證間隔（~50 分鐘，session 有效 60 分鐘）
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
    """幣安 User Data Stream 客戶端（三業務線）。

    SPOT / MARGIN ── 新式 WebSocket API（ws-api.binance.com）
    FUTURES       ── 舊式 User Data Stream（listenKey）
    """

    def __init__(self, api_key, api_secret, market=StreamMarket.SPOT,
                 testnet=False, max_queue=500, isolated_symbol=None,
                 async_client=None, time_offset: int = 0):
        """
        Parameters
        ----------
        async_client : AsyncExchangeClient（選用）
            斷線重連時 REST 補單同步透過它執行，確保 HMAC 簽章與 time_offset 一致。
        time_offset  : int（毫秒）
            本機時間與幣安伺服器的偏移量（可從 AsyncExchangeClient.sync_time() 取得）。
            WS API 簽章的 timestamp 將加上此偏移。
        """
        self._api_key         = api_key
        self._api_secret      = api_secret
        self._market          = market
        self._testnet         = testnet
        self._isolated_symbol = isolated_symbol
        self._async_client    = async_client
        self._time_offset     = time_offset
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
                if self._market == StreamMarket.FUTURES:
                    await self._connect_futures()
                else:
                    await self._connect_ws_api()
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

    # ════════════════════════════════════════════════════════
    # FUTURES：舊式 User Data Stream（listenKey 流程）
    # ════════════════════════════════════════════════════════
    async def _connect_futures(self):
        try:
            import websockets
        except ImportError:
            raise RuntimeError("請安裝 websockets：pip install websockets")

        listen_key = await asyncio.get_event_loop().run_in_executor(
            None, self._create_futures_listen_key)
        if not listen_key:
            raise RuntimeError("[WS-FUTURES] 無法取得 Listen Key")
        self._status.listen_key = listen_key

        url      = f"{_WS_TESTNET[self._market] if self._testnet else _WS_ENDPOINTS[self._market]}{listen_key}"
        ssl_ctx  = ssl.create_default_context()
        hostname = urlparse(url).hostname or ""
        log.info(f"[WS-FUTURES] 連接至 {url[:70]}...")

        keepalive_task = asyncio.create_task(self._keepalive_futures_listen_key(listen_key))
        try:
            async with websockets.connect(
                url, ssl=ssl_ctx, server_hostname=hostname,
                ping_interval=20, ping_timeout=10, close_timeout=5,
            ) as ws:
                self._status.connected     = True
                self._status.last_msg_time = time.time()
                log.info("[WS-FUTURES] 連線成功")
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

    def _create_futures_listen_key(self) -> str:
        import urllib.request
        req = urllib.request.Request(
            _LISTEN_KEY_CREATE_FUTURES, data=b"", method="POST",
            headers={"X-MBX-APIKEY": self._api_key})
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                return json.loads(resp.read()).get("listenKey", "")
        except Exception as e:
            log.error(f"[WS-FUTURES] 建立 Listen Key 失敗: {e}"); return ""

    async def _keepalive_futures_listen_key(self, listen_key: str):
        """每 30 分鐘 PUT futures listenKey 保活。"""
        while True:
            await asyncio.sleep(LISTEN_KEY_REFRESH)
            try:
                await asyncio.get_event_loop().run_in_executor(
                    None, lambda: self._put_futures_listen_key(listen_key))
                log.debug("[WS-FUTURES] Listen Key 保活成功")
            except Exception as e:
                log.warning(f"[WS-FUTURES] Listen Key 保活失敗: {e}")

    def _put_futures_listen_key(self, listen_key: str):
        import urllib.request
        data = urlencode({"listenKey": listen_key}).encode()
        req  = urllib.request.Request(
            _LISTEN_KEY_KEEPALIVE_FUTURES, data=data, method="PUT",
            headers={"X-MBX-APIKEY": self._api_key})
        with urllib.request.urlopen(req, timeout=5):
            pass

    # ════════════════════════════════════════════════════════
    # SPOT / MARGIN：新式 WebSocket API（2026-02-20 起）
    # ════════════════════════════════════════════════════════
    async def _connect_ws_api(self):
        """
        連接幣安新式 WebSocket API 端點，連線後立即發送
        userDataStream.subscribe.signature 完成身分驗證。
        每 ~50 分鐘重新發送驗證訊息，避免 session 60 分鐘過期。
        """
        try:
            import websockets
        except ImportError:
            raise RuntimeError("請安裝 websockets：pip install websockets")

        url      = _WS_API_TESTNET if self._testnet else _WS_API_MAINNET
        ssl_ctx  = ssl.create_default_context()
        hostname = urlparse(url).hostname or ""
        log.info(f"[WS-{self._market.value}] 連接至 WS API {url}...")

        async with websockets.connect(
            url, ssl=ssl_ctx, server_hostname=hostname,
            ping_interval=20, ping_timeout=10, close_timeout=5,
        ) as ws:
            # ① 發送身分驗證訊息
            await self._ws_api_auth(ws)

            self._status.connected     = True
            self._status.last_msg_time = time.time()
            self._status.listen_key    = ""   # WS API 不使用 listenKey
            log.info(f"[WS-{self._market.value}] WS API 連線成功，等待事件...")

            reauth_task    = asyncio.create_task(self._ws_api_reauth_loop(ws))
            heartbeat_task = asyncio.create_task(self._heartbeat_monitor(ws))
            try:
                async for raw_msg in ws:
                    if self._stop_event.is_set(): break
                    self._status.last_msg_time = time.time()
                    self._handle_message(raw_msg)
            finally:
                reauth_task.cancel()
                heartbeat_task.cancel()
                self._status.connected = False

    async def _ws_api_auth(self, ws) -> None:
        """發送 userDataStream.subscribe.signature 驗證訊息。"""
        ts  = int(time.time() * 1000) + self._time_offset
        msg = self._build_ws_api_auth_msg(ts)
        await ws.send(json.dumps(msg))
        log.debug(f"[WS-{self._market.value}] WS API 驗證訊息已送出 (id={msg['id']})")

        # 等待驗證回應（最多 5 秒）
        try:
            resp_raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
            resp     = json.loads(resp_raw)
            status   = resp.get("status", -1)
            if status == 200:
                log.info(f"[WS-{self._market.value}] WS API 驗證成功")
            else:
                err = resp.get("error", {})
                raise RuntimeError(
                    f"[WS-{self._market.value}] WS API 驗證失敗: "
                    f"status={status} msg={err.get('msg','')}"
                )
        except asyncio.TimeoutError:
            log.warning(f"[WS-{self._market.value}] WS API 驗證回應逾時，繼續等待事件")

    def _build_ws_api_auth_msg(self, timestamp: int) -> Dict:
        """
        建立 userDataStream.subscribe.signature 訊息。
        簽章對象：apiKey=<key>&timestamp=<ts>
        """
        params_to_sign = urlencode({
            "apiKey":    self._api_key,
            "timestamp": timestamp,
        })
        sig = hmac.new(
            self._api_secret.encode(),
            params_to_sign.encode(),
            hashlib.sha256,
        ).hexdigest()
        return {
            "id":     uuid.uuid4().hex[:16],
            "method": "userDataStream.subscribe.signature",
            "params": {
                "apiKey":    self._api_key,
                "timestamp": timestamp,
                "signature": sig,
            },
        }

    async def _ws_api_reauth_loop(self, ws):
        """每 ~50 分鐘重新送出驗證訊息，防止 WS API session 60 分鐘過期。"""
        while True:
            await asyncio.sleep(WS_API_REAUTH_SEC)
            try:
                await self._ws_api_auth(ws)
                log.info(f"[WS-{self._market.value}] WS API session 重新驗證成功")
            except Exception as e:
                log.warning(f"[WS-{self._market.value}] WS API 重新驗證失敗: {e}")

    # ════════════════════════════════════════════════════════
    # 共用：Heartbeat、訊息解析、事件佇列
    # ════════════════════════════════════════════════════════
    async def _heartbeat_monitor(self, ws):
        while True:
            await asyncio.sleep(15)
            elapsed = time.time() - self._status.last_msg_time
            if elapsed > HEARTBEAT_TIMEOUT:
                log.warning(f"[WS-{self._market.value}] Heartbeat 超時，主動重連")
                await ws.close()
                return

    async def _sync_open_orders_rest(self):
        """
        斷線重連後透過 REST 補抓 OPEN 訂單。
        若持有 AsyncExchangeClient 實例則透過它執行（含 time_offset）。
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

    def _handle_message(self, raw: str) -> None:
        """
        解析收到的 WebSocket 訊息。

        相容兩種格式：
          1. 舊式 User Data Stream（FUTURES）：事件直接在頂層
             {"e": "executionReport", ...}
          2. 新式 WebSocket API（SPOT / MARGIN）：
             a. 驗證/訂閱回應：{"id": "...", "status": 200, "result": ...}
                → 略過（已在 _ws_api_auth 處理）
             b. 推送事件可能帶有外層 wrapper：
                {"stream": "...", "data": {"e": "executionReport", ...}}
                或直接推送：{"e": "executionReport", ...}
        """
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            return

        # WS API 回應訊息（含 id 欄位）：已由 _ws_api_auth 處理，此處略過
        if "id" in msg and "status" in msg:
            return

        # 相容外層 wrapper（新式 WS API 推送）
        if "data" in msg and isinstance(msg["data"], dict):
            inner = msg["data"]
        else:
            inner = msg

        event_type = inner.get("e", "")

        if event_type == "executionReport":
            ev = self._parse_execution_report(inner)
            if ev: self._enqueue(ev)
        elif event_type == "ORDER_TRADE_UPDATE":
            o  = inner.get("o", {})
            ev = self._parse_futures_order(o, inner.get("T", 0))
            if ev: self._enqueue(ev)
        elif event_type == "ACCOUNT_UPDATE":
            data = inner.get("a", {})
            self._enqueue(AccountUpdateEvent(
                event_type=event_type, balances=data.get("B", []),
                positions=data.get("P", []), timestamp=inner.get("T", 0)))
        elif event_type == "outboundAccountPosition":
            self._enqueue(BalanceEvent(
                event_type=event_type, balances=inner.get("B", []),
                timestamp=inner.get("u", 0)))
        elif event_type == "listenKeyExpired":
            # 僅 FUTURES 可能觸發
            log.warning(f"[WS-{self._market.value}] Listen Key 失效，觸發重連")
            if self._loop:
                self._loop.call_soon_threadsafe(
                    lambda: asyncio.ensure_future(self._force_reconnect()))
        elif event_type:
            log.debug(f"[WS-{self._market.value}] 未處理事件類型: {event_type}")

    def _enqueue(self, event) -> None:
        try:
            self._queue.put_nowait(event)
        except queue.Full:
            try:
                self._queue.get_nowait()
                self._queue.put_nowait(event)
            except queue.Empty:
                pass

    def _parse_execution_report(self, msg: Dict):
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
    def _parse_futures_order(o: Dict, ts: int):
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

    async def _force_reconnect(self):
        if self._loop:
            for task in asyncio.all_tasks(self._loop):
                task.cancel()
