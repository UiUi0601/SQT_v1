"""
quant_system/exchange/async_client.py -- 幣安非同步客戶端（三業務線版）

支援業務線（MarketType）：
  SPOT    -> https://api.binance.com
  FUTURES -> https://fapi.binance.com        (U本位永續合約)
  MARGIN  -> https://api.binance.com/sapi/v1 (槓桿交易)

修改記錄（TLS / 時間偏移）：
  - 新增 sync_time()：啟動時對齊幣安伺服器時間，計算 _time_offset (ms)
  - _sign() 中 timestamp 加上 _time_offset，確保簽章不因本機時鐘偏移而被拒
"""
from __future__ import annotations
import asyncio, hashlib, hmac, json, logging, os, threading, time
from enum import Enum
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode, urlparse

log = logging.getLogger(__name__)

try:
    import aiohttp
    _AIOHTTP_AVAILABLE = True
except ImportError:
    aiohttp = None
    _AIOHTTP_AVAILABLE = False
    log.warning("[AsyncClient] aiohttp 未安裝，請執行：pip install aiohttp")


class MarketType(str, Enum):
    SPOT    = "SPOT"
    FUTURES = "FUTURES"
    MARGIN  = "MARGIN"


_BASE_URLS = {
    MarketType.SPOT:    "https://api.binance.com",
    MarketType.FUTURES: "https://fapi.binance.com",
    MarketType.MARGIN:  "https://api.binance.com",
}
_TESTNET_URLS = {
    MarketType.SPOT:    "https://testnet.binance.vision",
    MarketType.FUTURES: "https://testnet.binancefuture.com",
    MarketType.MARGIN:  "https://testnet.binance.vision",
}
_TIMEOUT_SEC = 10.0
_MAX_CONNS   = 20

_loop        = None
_loop_thread = None
_loop_lock   = threading.Lock()


def _get_loop():
    global _loop, _loop_thread
    if _loop is not None and _loop.is_running():
        return _loop
    with _loop_lock:
        if _loop is None or not _loop.is_running():
            _loop = asyncio.new_event_loop()
            def _start():
                asyncio.set_event_loop(_loop)
                _loop.run_forever()
            _loop_thread = threading.Thread(target=_start, name="async-loop", daemon=True)
            _loop_thread.start()
            for _ in range(50):
                if _loop.is_running():
                    break
                time.sleep(0.01)
    return _loop


def run_async(coro):
    """同步橋接器：在同步程式碼中阻塞執行 async 協程。"""
    loop   = _get_loop()
    future = asyncio.run_coroutine_threadsafe(coro, loop)
    return future.result(timeout=_TIMEOUT_SEC * 2)


class AsyncExchangeClient:
    """幣安非同步客戶端，支援 SPOT / FUTURES / MARGIN 三業務線。"""

    def __init__(self, api_key=None, api_secret=None,
                 market=MarketType.SPOT, testnet=False, timeout=_TIMEOUT_SEC):
        if not _AIOHTTP_AVAILABLE:
            raise ImportError("aiohttp 未安裝，請執行：pip install aiohttp")
        self._api_key      = api_key    or os.getenv("BINANCE_API_KEY",    "")
        self._api_secret   = api_secret or os.getenv("BINANCE_API_SECRET", "")
        self._market       = market
        self._testnet      = testnet
        self._timeout      = aiohttp.ClientTimeout(total=timeout)
        self._session      = None
        self._time_offset: int = 0   # 本機時間與幣安伺服器的偏移（毫秒）
        url_map            = _TESTNET_URLS if testnet else _BASE_URLS
        self._base_url     = url_map[market]

    async def __aenter__(self):
        await self.open(); return self

    async def __aexit__(self, *_):
        await self.close()

    async def open(self):
        if self._session is None or self._session.closed:
            connector = aiohttp.TCPConnector(limit=_MAX_CONNS, limit_per_host=10, ssl=True)
            self._session = aiohttp.ClientSession(
                base_url=self._base_url, timeout=self._timeout,
                connector=connector, headers={"X-MBX-APIKEY": self._api_key})

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    async def sync_time(self) -> int:
        """
        同步幣安伺服器時間，計算本機偏移量（毫秒）。

        做法：記錄請求前後本機時間取中間值，與伺服器 serverTime 相減。
        後續 _sign() 中 timestamp = local_ms + self._time_offset。

        建議：在 open() 之後、首次簽名 API 呼叫之前執行一次。
        """
        await self._ensure_session()
        path = "/fapi/v1/time" if self._market == MarketType.FUTURES else "/api/v3/time"
        local_before = int(time.time() * 1000)
        data = await self._get(path)
        local_after  = int(time.time() * 1000)
        server_time  = int(data.get("serverTime", 0))
        local_mid    = (local_before + local_after) // 2
        self._time_offset = server_time - local_mid
        log.info(
            f"[AsyncClient-{self._market.value}] 時間同步完成，"
            f"offset={self._time_offset:+d} ms "
            f"（server={server_time}, local_mid={local_mid}）"
        )
        return self._time_offset

    # ── SPOT 公開 API ─────────────────────────────────────
    async def get_ticker(self, symbol):
        path = "/fapi/v1/ticker/price" if self._market == MarketType.FUTURES else "/api/v3/ticker/price"
        return await self._get(path, {"symbol": symbol})

    async def get_tickers_bulk(self, symbols=None):
        path = "/fapi/v1/ticker/price" if self._market == MarketType.FUTURES else "/api/v3/ticker/price"
        params = {}
        if symbols:
            params["symbols"] = json.dumps(symbols)
        return await self._get(path, params)

    async def get_klines(self, symbol, interval="1m", limit=500, start_ms=None, end_ms=None):
        path = "/fapi/v1/klines" if self._market == MarketType.FUTURES else "/api/v3/klines"
        params = {"symbol": symbol, "interval": interval, "limit": limit}
        if start_ms: params["startTime"] = start_ms
        if end_ms:   params["endTime"]   = end_ms
        return await self._get(path, params)

    async def get_order_book(self, symbol, limit=20):
        path = "/fapi/v1/depth" if self._market == MarketType.FUTURES else "/api/v3/depth"
        return await self._get(path, {"symbol": symbol, "limit": limit})

    async def get_exchange_info(self):
        path = "/fapi/v1/exchangeInfo" if self._market == MarketType.FUTURES else "/api/v3/exchangeInfo"
        return await self._get(path)

    # ── SPOT 私有 API ─────────────────────────────────────
    async def get_account(self):
        return await self._signed_get("/api/v3/account")

    async def get_open_orders(self, symbol=None):
        params = {"symbol": symbol} if symbol else {}
        return await self._signed_get("/api/v3/openOrders", params)

    async def place_spot_order(self, symbol, side, order_type, quantity,
                                price=None, client_order_id=None, time_in_force="GTC"):
        params = {"symbol": symbol, "side": side, "type": order_type, "quantity": str(quantity)}
        if order_type == "LIMIT":
            params["timeInForce"] = time_in_force
            params["price"]       = str(price)
        if client_order_id:
            params["newClientOrderId"] = client_order_id
        return await self._signed_post("/api/v3/order", params)

    async def cancel_order(self, symbol, order_id=None):
        params = {"symbol": symbol}
        if order_id: params["orderId"] = order_id
        return await self._signed_delete("/api/v3/order", params)

    async def cancel_all_orders(self, symbol):
        return await self._signed_delete("/api/v3/openOrders", {"symbol": symbol})

    # ── FUTURES 專用 API ──────────────────────────────────
    async def get_funding_rate(self, symbol):
        return await self._get("/fapi/v1/premiumIndex", {"symbol": symbol})

    async def set_margin_type(self, symbol, margin_type="CROSSED"):
        return await self._signed_post("/fapi/v1/marginType",
                                       {"symbol": symbol, "marginType": margin_type})

    async def set_leverage(self, symbol, leverage):
        return await self._signed_post("/fapi/v1/leverage",
                                       {"symbol": symbol, "leverage": leverage})

    async def place_futures_order(self, symbol, side, order_type, quantity,
                                   price=None, position_side=None, client_order_id=None,
                                   time_in_force="GTC", reduce_only=False):
        params = {"symbol": symbol, "side": side, "type": order_type, "quantity": str(quantity)}
        if order_type == "LIMIT":
            params["timeInForce"] = time_in_force
            params["price"]       = str(price)
        if position_side:       params["positionSide"] = position_side
        if reduce_only:         params["reduceOnly"]   = "true"
        if client_order_id:     params["newClientOrderId"] = client_order_id
        return await self._signed_post("/fapi/v1/order", params)

    async def get_futures_account(self):
        return await self._signed_get("/fapi/v2/account")

    async def get_futures_position_risk(self, symbol=None):
        params = {"symbol": symbol} if symbol else {}
        return await self._signed_get("/fapi/v2/positionRisk", params)

    async def get_futures_open_orders(self, symbol=None):
        params = {"symbol": symbol} if symbol else {}
        return await self._signed_get("/fapi/v1/openOrders", params)

    async def cancel_futures_order(self, symbol, order_id=None):
        params = {"symbol": symbol}
        if order_id: params["orderId"] = order_id
        return await self._signed_delete("/fapi/v1/order", params)

    async def cancel_all_futures_orders(self, symbol):
        return await self._signed_delete("/fapi/v1/allOpenOrders", {"symbol": symbol})

    # ── MARGIN 專用 API ───────────────────────────────────
    async def place_margin_order(self, symbol, side, order_type, quantity,
                                  price=None, is_isolated=False,
                                  side_effect_type="NO_SIDE_EFFECT",
                                  client_order_id=None, time_in_force="GTC"):
        """
        POST /sapi/v1/margin/order
        side_effect_type: NO_SIDE_EFFECT | MARGIN_BUY | AUTO_REPAY
        is_isolated: True=逐倉; False=全倉
        """
        params = {
            "symbol": symbol, "side": side, "type": order_type,
            "quantity": str(quantity), "sideEffectType": side_effect_type,
            "isIsolated": "TRUE" if is_isolated else "FALSE",
        }
        if order_type == "LIMIT":
            params["timeInForce"] = time_in_force
            params["price"]       = str(price)
        if client_order_id:
            params["newClientOrderId"] = client_order_id
        return await self._signed_post("/sapi/v1/margin/order", params)

    async def get_margin_account(self):
        return await self._signed_get("/sapi/v1/margin/account")

    async def get_isolated_margin_account(self, symbols=None):
        params = {"symbols": symbols} if symbols else {}
        return await self._signed_get("/sapi/v1/margin/isolated/account", params)

    async def margin_borrow(self, asset, amount, isolated_symbol=None):
        params = {"asset": asset, "amount": str(amount)}
        if isolated_symbol:
            params["isIsolated"] = "TRUE"
            params["symbol"]     = isolated_symbol
        return await self._signed_post("/sapi/v1/margin/loan", params)

    async def margin_repay(self, asset, amount, isolated_symbol=None):
        params = {"asset": asset, "amount": str(amount)}
        if isolated_symbol:
            params["isIsolated"] = "TRUE"
            params["symbol"]     = isolated_symbol
        return await self._signed_post("/sapi/v1/margin/repay", params)

    async def get_margin_open_orders(self, symbol=None, is_isolated=False):
        params = {"isIsolated": "TRUE" if is_isolated else "FALSE"}
        if symbol: params["symbol"] = symbol
        return await self._signed_get("/sapi/v1/margin/openOrders", params)

    # ── 並發工具 ──────────────────────────────────────────
    async def get_tickers_parallel(self, symbols):
        tasks   = [self.get_ticker(sym) for sym in symbols]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        prices  = {}
        for sym, res in zip(symbols, results):
            if isinstance(res, Exception):
                log.warning(f"[AsyncClient] {sym} 取價失敗: {res}")
            else:
                try:
                    prices[sym] = float(res["price"])
                except (KeyError, TypeError, ValueError):
                    pass
        return prices

    # ── 內部 HTTP ─────────────────────────────────────────
    async def _ensure_session(self):
        if self._session is None or self._session.closed:
            await self.open()

    async def _get(self, path, params=None):
        await self._ensure_session()
        async with self._session.get(path, params=params) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def _signed_get(self, path, params=None):
        await self._ensure_session()
        p = self._sign(params or {})
        async with self._session.get(path, params=p) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def _signed_post(self, path, params=None):
        await self._ensure_session()
        p = self._sign(params or {})
        async with self._session.post(path, data=p) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def _signed_delete(self, path, params=None):
        await self._ensure_session()
        p = self._sign(params or {})
        async with self._session.delete(path, params=p) as resp:
            resp.raise_for_status()
            return await resp.json()

    def _sign(self, params):
        params = dict(params)
        # 加上伺服器時間偏移，確保 timestamp 與幣安伺服器同步
        params["timestamp"] = int(time.time() * 1000) + self._time_offset
        query = urlencode(params)
        sig   = hmac.new(self._api_secret.encode(), query.encode(), hashlib.sha256).hexdigest()
        params["signature"] = sig
        return params
