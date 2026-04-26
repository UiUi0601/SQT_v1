"""
quant_system/exchange/async_client.py — 非同步交易所客戶端雛型（⑤）

設計目標：
  ─ 為未來全面 asyncio 化預留介面
  ─ 以 aiohttp 替代 python-binance 的同步 HTTP，大幅提升並發取價效率
  ─ 提供 `run_async()` bridge，讓現有同步程式碼可立即呼叫 async 方法
  ─ `AsyncExchangeClient` 實作最常用的幾個 API；其他 API 依此模板擴充

架構說明：
  ┌─────────────────────────────────────────────────────┐
  │   同步呼叫端（crypto_main_grid.py 等）              │
  │        ↓ run_async(coro)                            │
  │   asyncio event loop（背景執行緒）                  │
  │        ↓ aiohttp ClientSession                      │
  │   Binance REST API                                  │
  └─────────────────────────────────────────────────────┘

  現階段：run_async() 讓舊同步程式碼無縫使用 async 方法
  未來：將 GridBotInstance.tick() 改為 async def tick()，
        完全移除 time.sleep()，以 asyncio.sleep() 替代

依賴：
  pip install aiohttp  （requirements.txt 已包含）

使用範例：
  from quant_system.exchange.async_client import AsyncExchangeClient, run_async

  async def main():
      async with AsyncExchangeClient() as client:
          ticker = await client.get_ticker("BTCUSDT")
          klines = await client.get_klines("ETHUSDT", interval="1m", limit=100)

  # 從同步程式碼呼叫
  ticker = run_async(client.get_ticker("BTCUSDT"))
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import logging
import os
import threading
import time
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode

log = logging.getLogger(__name__)

# ── aiohttp 延遲匯入（避免未安裝時整個模組無法載入）────────
try:
    import aiohttp
    _AIOHTTP_AVAILABLE = True
except ImportError:
    aiohttp = None          # type: ignore[assignment]
    _AIOHTTP_AVAILABLE = False
    log.warning(
        "[AsyncClient] aiohttp 未安裝，AsyncExchangeClient 不可用。\n"
        "  請執行：pip install aiohttp"
    )

# Binance API 端點
_BASE_URL      = "https://api.binance.com"
_TESTNET_URL   = "https://testnet.binance.vision"

# 預設請求逾時（秒）
_TIMEOUT_SEC   = 10.0
# 最大並發連線數（避免觸發 Binance IP 封鎖）
_MAX_CONNS     = 20


# ════════════════════════════════════════════════════════════
# ⑤ 全域事件迴圈（背景執行緒，供 run_async bridge 使用）
# ════════════════════════════════════════════════════════════
_loop:        Optional[asyncio.AbstractEventLoop] = None
_loop_thread: Optional[threading.Thread]          = None
_loop_lock    = threading.Lock()


def _get_loop() -> asyncio.AbstractEventLoop:
    """
    取得（或建立）運行在背景執行緒的事件迴圈。
    執行緒設為 daemon，主程式退出時自動終止。
    """
    global _loop, _loop_thread
    if _loop is not None and _loop.is_running():
        return _loop

    with _loop_lock:
        if _loop is None or not _loop.is_running():
            _loop = asyncio.new_event_loop()

            def _start():
                asyncio.set_event_loop(_loop)
                _loop.run_forever()

            _loop_thread = threading.Thread(
                target = _start,
                name   = "async-loop",
                daemon = True,
            )
            _loop_thread.start()
            # 等待迴圈啟動
            for _ in range(50):
                if _loop.is_running():
                    break
                time.sleep(0.01)

    return _loop


def run_async(coro) -> Any:
    """
    ⑤ 同步橋接器（Bridge）：在現有同步程式碼中呼叫 async 協程。

    原理：將協程提交到背景事件迴圈，阻塞等待結果。
    注意：不可在已有 event loop 的非同步環境中呼叫（如 asyncio.run() 內部）。

    範例：
        ticker = run_async(client.get_ticker("BTCUSDT"))
    """
    loop = _get_loop()
    future = asyncio.run_coroutine_threadsafe(coro, loop)
    return future.result(timeout=_TIMEOUT_SEC * 2)


# ════════════════════════════════════════════════════════════
# ⑤ AsyncExchangeClient
# ════════════════════════════════════════════════════════════
class AsyncExchangeClient:
    """
    ⑤ 非同步 Binance 交易所客戶端雛型。

    以 aiohttp.ClientSession 取代 python-binance 的 requests，
    支援真正的 async/await 並發，適合同時查詢多個交易對的場景。

    使用方式（async context manager）：
        async with AsyncExchangeClient() as client:
            btc = await client.get_ticker("BTCUSDT")
            eth = await client.get_ticker("ETHUSDT")

        # 或並發取多個 ticker（比串行快 N 倍）
        results = await asyncio.gather(
            client.get_ticker("BTCUSDT"),
            client.get_ticker("ETHUSDT"),
            client.get_ticker("SOLUSDT"),
        )

    同步橋接（現有程式碼無需改寫）：
        client = AsyncExchangeClient()
        await client.open()
        ticker = run_async(client.get_ticker("BTCUSDT"))
        await client.close()
    """

    def __init__(
        self,
        api_key:    Optional[str] = None,
        api_secret: Optional[str] = None,
        testnet:    bool          = False,
        timeout:    float         = _TIMEOUT_SEC,
    ) -> None:
        if not _AIOHTTP_AVAILABLE:
            raise ImportError(
                "aiohttp 未安裝。請執行：pip install aiohttp"
            )

        self._api_key    = api_key    or os.getenv("BINANCE_API_KEY",    "")
        self._api_secret = api_secret or os.getenv("BINANCE_API_SECRET", "")
        self._base_url   = _TESTNET_URL if testnet else _BASE_URL
        self._timeout    = aiohttp.ClientTimeout(total=timeout)
        self._session:   Optional[aiohttp.ClientSession] = None

    # ── Context manager ──────────────────────────────────────
    async def __aenter__(self) -> "AsyncExchangeClient":
        await self.open()
        return self

    async def __aexit__(self, *_) -> None:
        await self.close()

    async def open(self) -> None:
        """建立 aiohttp Session（複用 TCP 連線池）。"""
        if self._session is None or self._session.closed:
            connector = aiohttp.TCPConnector(
                limit     = _MAX_CONNS,
                limit_per_host = 10,
                ssl       = True,
            )
            self._session = aiohttp.ClientSession(
                base_url  = self._base_url,
                timeout   = self._timeout,
                connector = connector,
                headers   = {
                    "X-MBX-APIKEY": self._api_key,
                    "Content-Type": "application/json",
                },
            )
            log.debug(f"[AsyncClient] Session 已建立，base_url={self._base_url}")

    async def close(self) -> None:
        """關閉 aiohttp Session，釋放連線資源。"""
        if self._session and not self._session.closed:
            await self._session.close()
            log.debug("[AsyncClient] Session 已關閉")

    # ════════════════════════════════════════════════════════
    # 公開 API（無需簽名）
    # ════════════════════════════════════════════════════════
    async def get_ticker(self, symbol: str) -> Dict[str, Any]:
        """
        取得單一交易對的最新成交價。
        等同 GET /api/v3/ticker/price

        >>> ticker = await client.get_ticker("BTCUSDT")
        >>> float(ticker["price"])
        50000.0
        """
        return await self._get("/api/v3/ticker/price", {"symbol": symbol})

    async def get_tickers_bulk(
        self,
        symbols: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """
        批量取得所有（或指定）交易對的最新成交價。
        等同 GET /api/v3/ticker/price（不帶 symbol 參數）

        並發優勢：一次請求所有 ticker，比串行快 N 倍。
        """
        params: Dict[str, Any] = {}
        if symbols:
            import json
            params["symbols"] = json.dumps(symbols)
        return await self._get("/api/v3/ticker/price", params)

    async def get_klines(
        self,
        symbol:    str,
        interval:  str = "1m",
        limit:     int = 500,
        start_ms:  Optional[int] = None,
        end_ms:    Optional[int] = None,
    ) -> List[List]:
        """
        取得 K 線資料。等同 GET /api/v3/klines

        回傳格式：[[open_time, open, high, low, close, volume, ...], ...]
        """
        params: Dict[str, Any] = {
            "symbol":   symbol,
            "interval": interval,
            "limit":    limit,
        }
        if start_ms:
            params["startTime"] = start_ms
        if end_ms:
            params["endTime"] = end_ms
        return await self._get("/api/v3/klines", params)

    async def get_order_book(
        self,
        symbol: str,
        limit:  int = 5,
    ) -> Dict[str, Any]:
        """取得委買/委賣盤口。等同 GET /api/v3/depth"""
        return await self._get("/api/v3/depth", {"symbol": symbol, "limit": limit})

    async def get_exchange_info(
        self,
        symbol: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        取得交易對規則（精度、最小下單量等）。
        等同 GET /api/v3/exchangeInfo
        """
        params = {"symbol": symbol} if symbol else {}
        return await self._get("/api/v3/exchangeInfo", params)

    # ════════════════════════════════════════════════════════
    # 私有 API（需要 HMAC-SHA256 簽名）
    # ════════════════════════════════════════════════════════
    async def get_account(self) -> Dict[str, Any]:
        """
        取得帳戶資訊（餘額、持倉）。
        等同 GET /api/v3/account（需簽名）
        """
        return await self._signed_get("/api/v3/account")

    async def get_open_orders(self, symbol: Optional[str] = None) -> List[Dict]:
        """
        取得當前掛單。等同 GET /api/v3/openOrders（需簽名）
        """
        params = {"symbol": symbol} if symbol else {}
        return await self._signed_get("/api/v3/openOrders", params)

    async def place_limit_order(
        self,
        symbol:           str,
        side:             str,   # "BUY" | "SELL"
        quantity:         float,
        price:            float,
        client_order_id:  Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        掛限價單。等同 POST /api/v3/order（需簽名）

        量化機器人未來全面 async 化後，GridManager._place_limit()
        可直接改用此方法。
        """
        params: Dict[str, Any] = {
            "symbol":      symbol,
            "side":        side,
            "type":        "LIMIT",
            "timeInForce": "GTC",
            "quantity":    f"{quantity:.8f}",
            "price":       f"{price:.8f}",
        }
        if client_order_id:
            params["newClientOrderId"] = client_order_id
        return await self._signed_post("/api/v3/order", params)

    async def cancel_order(
        self,
        symbol:   str,
        order_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """撤銷單一訂單。等同 DELETE /api/v3/order（需簽名）"""
        params: Dict[str, Any] = {"symbol": symbol}
        if order_id:
            params["orderId"] = order_id
        return await self._signed_delete("/api/v3/order", params)

    async def cancel_all_orders(self, symbol: str) -> List[Dict]:
        """撤銷所有掛單。等同 DELETE /api/v3/openOrders（需簽名）"""
        return await self._signed_delete(
            "/api/v3/openOrders", {"symbol": symbol}
        )

    # ════════════════════════════════════════════════════════
    # 並發工具方法
    # ════════════════════════════════════════════════════════
    async def get_tickers_parallel(
        self,
        symbols: List[str],
    ) -> Dict[str, float]:
        """
        ⑤ 並發取得多個交易對的最新價。

        相比 python-binance 串行呼叫，速度提升約 N 倍（N = symbol 數量）。
        適合同時監控多個網格的快速取價場景。

        回傳：{symbol: price}
        """
        tasks   = [self.get_ticker(sym) for sym in symbols]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        prices: Dict[str, float] = {}
        for sym, res in zip(symbols, results):
            if isinstance(res, Exception):
                log.warning(f"[AsyncClient] {sym} 取價失敗: {res}")
            else:
                try:
                    prices[sym] = float(res["price"])
                except (KeyError, TypeError, ValueError):
                    pass
        return prices

    # ════════════════════════════════════════════════════════
    # 內部 HTTP 方法
    # ════════════════════════════════════════════════════════
    async def _ensure_session(self) -> None:
        if self._session is None or self._session.closed:
            await self.open()

    async def _get(
        self,
        path:   str,
        params: Optional[Dict] = None,
    ) -> Any:
        await self._ensure_session()
        async with self._session.get(path, params=params) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def _signed_get(
        self,
        path:   str,
        params: Optional[Dict] = None,
    ) -> Any:
        await self._ensure_session()
        full_params = self._sign(params or {})
        async with self._session.get(path, params=full_params) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def _signed_post(
        self,
        path:   str,
        params: Optional[Dict] = None,
    ) -> Any:
        await self._ensure_session()
        full_params = self._sign(params or {})
        async with self._session.post(path, data=full_params) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def _signed_delete(
        self,
        path:   str,
        params: Optional[Dict] = None,
    ) -> Any:
        await self._ensure_session()
        full_params = self._sign(params or {})
        async with self._session.delete(path, params=full_params) as resp:
            resp.raise_for_status()
            return await resp.json()

    def _sign(self, params: Dict) -> Dict:
        """
        附加 timestamp 與 HMAC-SHA256 signature。
        Binance 所有私有 API 必需。
        """
        params = dict(params)
        params["timestamp"] = int(time.time() * 1000)
        query   = urlencode(params)
        sig     = hmac.new(
            self._api_secret.encode(),
            query.encode(),
            hashlib.sha256,
        ).hexdigest()
        params["signature"] = sig
        return params
