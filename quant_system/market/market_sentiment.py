"""
quant_system/market/market_sentiment.py — 市場情緒採集器

監控兩種市場情緒訊號：

① 資金費率（Funding Rate）
   ─ 輪詢 /fapi/v1/premiumIndex，獲取即時費率、指數價、標記價
   ─ 極端費率（> 0.05% 或 < -0.03%）轉化為反向操作因子
   ─ 支援多符號並發批量查詢

② 強制爆倉訂單（Liquidation Orders）
   ─ 監聽 WebSocket 串流 wss://fstream.binance.com/ws/{symbol}@forceOrder
   ─ 統計滑動視窗（預設 5 分鐘）內的爆倉金額與方向
   ─ 大額爆倉（> 1M USD）作為短期動能因子輸入評分引擎

使用範例：
    sentiment = MarketSentiment(
        api_key="...", api_secret="...",
        symbols=["BTCUSDT", "ETHUSDT"]
    )

    # 啟動爆倉監聽
    sentiment.start_liquidation_monitor()

    # 批量查詢資金費率
    rates = await sentiment.get_funding_rates()
    # {"BTCUSDT": FundingData(rate=0.0003, mark_price=...), ...}

    # 取得最近 5 分鐘爆倉統計
    liq = sentiment.get_liquidation_summary("BTCUSDT")
    # LiquidationSummary(buy_volume=2.5e6, sell_volume=0.3e6, ...)

    # 輸出 ScoringEngine 的 market_ctx
    ctx = sentiment.to_scoring_ctx("BTCUSDT")
    # {"funding_rate": 0.0003, "liq_volume_usd": 2.5e6, "liq_side": "BUY"}
"""
from __future__ import annotations

import asyncio
import json
import logging
import queue
import threading
import time
from collections import deque, defaultdict
from dataclasses import dataclass, field
from typing import Any, Deque, Dict, List, Optional

log = logging.getLogger(__name__)

# ════════════════════════════════════════════════════════════
# 資料類別
# ════════════════════════════════════════════════════════════
@dataclass
class FundingData:
    """單一合約的資金費率快照"""
    symbol:         str
    funding_rate:   float    # 最新資金費率（如 0.0003 = 0.03%）
    next_funding_time: int   # 下次結算時間（ms）
    mark_price:     float    # 標記價格
    index_price:    float    # 指數價格
    fetched_at:     float = field(default_factory=time.time)

    @property
    def is_extreme_long(self) -> bool:
        """多頭過熱（費率高）→ 反向做空訊號"""
        return self.funding_rate > 0.0005

    @property
    def is_extreme_short(self) -> bool:
        """空頭過熱（費率極負）→ 反向做多訊號"""
        return self.funding_rate < -0.0003

    @property
    def sentiment_score(self) -> float:
        """
        將費率轉換為 [-1, 1] 情緒評分（反向指標）。
        費率越高 → 評分越負（多頭過熱 → 鼓勵空）
        費率越負 → 評分越正（空頭過熱 → 鼓勵多）
        """
        if self.is_extreme_long:
            ratio = min(1.0, (self.funding_rate - 0.0005) / 0.0015)
            return -(0.2 + ratio * 0.8)
        elif self.is_extreme_short:
            ratio = min(1.0, (abs(self.funding_rate) - 0.0003) / 0.001)
            return 0.2 + ratio * 0.8
        else:
            return -self.funding_rate / 0.0005 * 0.2


@dataclass
class LiquidationEvent:
    """單筆爆倉訂單事件"""
    symbol:       str
    side:         str    # "BUY"（空頭被爆）或 "SELL"（多頭被爆）
    price:        float
    qty:          float
    notional_usd: float
    timestamp:    float = field(default_factory=time.time)  # Unix time


@dataclass
class LiquidationSummary:
    """滑動視窗內的爆倉統計"""
    symbol:          str
    window_sec:      float
    buy_volume_usd:  float   # 空頭爆倉量（USD），暗示軋空動能
    sell_volume_usd: float   # 多頭爆倉量（USD），暗示拋售壓力
    event_count:     int
    computed_at:     float = field(default_factory=time.time)

    @property
    def dominant_side(self) -> str:
        """爆倉主導方向：'BUY'（空頭被爆更多）或 'SELL'"""
        if self.buy_volume_usd >= self.sell_volume_usd:
            return "BUY"
        return "SELL"

    @property
    def total_volume_usd(self) -> float:
        return self.buy_volume_usd + self.sell_volume_usd

    @property
    def is_significant(self) -> bool:
        """總爆倉量 > 1M USD 才視為有意義的訊號"""
        return self.total_volume_usd >= 1_000_000


# ════════════════════════════════════════════════════════════
# MarketSentiment
# ════════════════════════════════════════════════════════════
class MarketSentiment:
    """
    市場情緒採集器。

    包含：
      1. 資金費率批量輪詢（REST + 快取，預設 30 秒更新一次）
      2. 爆倉訂單 WebSocket 監聽（獨立執行緒，滑動視窗統計）
    """

    _FUTURES_BASE = "https://fapi.binance.com"
    _WS_FUTURES   = "wss://fstream.binance.com/ws/"

    def __init__(
        self,
        api_key:         str             = "",
        api_secret:      str             = "",
        symbols:         Optional[List[str]] = None,
        liq_window_sec:  float           = 300.0,   # 爆倉統計滑動視窗（5 分鐘）
        funding_ttl_sec: float           = 30.0,    # 資金費率快取有效期
        max_liq_events:  int             = 500,     # 每個 symbol 最多保存的爆倉事件數
    ) -> None:
        self._api_key         = api_key
        self._api_secret      = api_secret
        self._symbols: List[str] = [s.upper() for s in (symbols or [])]
        self._liq_window      = liq_window_sec
        self._funding_ttl     = funding_ttl_sec

        # 資金費率快取：{symbol: FundingData}
        self._funding_cache: Dict[str, FundingData] = {}
        self._funding_lock   = threading.Lock()

        # 爆倉事件滑動視窗：{symbol: deque[LiquidationEvent]}
        self._liq_events: Dict[str, Deque[LiquidationEvent]] = defaultdict(
            lambda: deque(maxlen=max_liq_events)
        )
        self._liq_lock = threading.Lock()

        # WebSocket 執行緒
        self._ws_thread:    Optional[threading.Thread] = None
        self._stop_event    = threading.Event()
        self._ws_loop:      Optional[asyncio.AbstractEventLoop] = None

    # ════════════════════════════════════════════════════════
    # 資金費率（REST）
    # ════════════════════════════════════════════════════════
    async def get_funding_rates(
        self,
        symbols: Optional[List[str]] = None,
    ) -> Dict[str, FundingData]:
        """
        並發查詢多個合約的資金費率。
        結果快取 funding_ttl_sec 秒，避免頻繁呼叫 API。

        回傳 {symbol: FundingData}
        """
        targets = [s.upper() for s in (symbols or self._symbols)]
        if not targets:
            return {}

        now   = time.time()
        fresh: Dict[str, FundingData] = {}
        stale: List[str] = []

        with self._funding_lock:
            for sym in targets:
                cached = self._funding_cache.get(sym)
                if cached and (now - cached.fetched_at) < self._funding_ttl:
                    fresh[sym] = cached
                else:
                    stale.append(sym)

        if stale:
            new_data = await self._fetch_funding_batch(stale)
            with self._funding_lock:
                self._funding_cache.update(new_data)
            fresh.update(new_data)

        return fresh

    async def _fetch_funding_batch(
        self, symbols: List[str]
    ) -> Dict[str, FundingData]:
        """並發抓取多個交易對的 /fapi/v1/premiumIndex。"""
        try:
            import aiohttp
        except ImportError:
            log.error("[Sentiment] aiohttp 未安裝，無法查詢資金費率")
            return {}

        timeout = aiohttp.ClientTimeout(total=10.0)
        result: Dict[str, FundingData] = {}

        async with aiohttp.ClientSession(timeout=timeout) as session:
            tasks = [
                self._fetch_single_funding(session, sym)
                for sym in symbols
            ]
            responses = await asyncio.gather(*tasks, return_exceptions=True)

        for sym, resp in zip(symbols, responses):
            if isinstance(resp, Exception):
                log.warning(f"[Sentiment] {sym} 資金費率查詢失敗: {resp}")
            elif resp:
                result[sym] = resp

        return result

    @staticmethod
    async def _fetch_single_funding(
        session, symbol: str
    ) -> Optional[FundingData]:
        url = f"https://fapi.binance.com/fapi/v1/premiumIndex?symbol={symbol}"
        try:
            async with session.get(url) as resp:
                resp.raise_for_status()
                data = await resp.json()
                return FundingData(
                    symbol          = symbol,
                    funding_rate    = float(data.get("lastFundingRate", 0)),
                    next_funding_time = int(data.get("nextFundingTime", 0)),
                    mark_price      = float(data.get("markPrice", 0)),
                    index_price     = float(data.get("indexPrice", 0)),
                )
        except Exception as e:
            log.debug(f"[Sentiment] {symbol} premiumIndex 失敗: {e}")
            return None

    # ════════════════════════════════════════════════════════
    # 爆倉監聽（WebSocket）
    # ════════════════════════════════════════════════════════
    def start_liquidation_monitor(
        self,
        symbols: Optional[List[str]] = None,
    ) -> None:
        """
        啟動背景執行緒監聽指定交易對的強制爆倉事件。
        每個 symbol 對應一個 @forceOrder WebSocket 串流。
        """
        targets = [s.upper() for s in (symbols or self._symbols)]
        if not targets:
            log.warning("[Sentiment] 未指定監聽 symbol，爆倉監聽未啟動")
            return

        if self._ws_thread and self._ws_thread.is_alive():
            log.warning("[Sentiment] 爆倉監聽已在執行中")
            return

        self._stop_event.clear()
        self._ws_thread = threading.Thread(
            target=self._ws_run_loop,
            args=(targets,),
            daemon=True,
            name="LiquidationMonitor",
        )
        self._ws_thread.start()
        log.info(f"[Sentiment] 爆倉監聽啟動：{targets}")

    def stop_liquidation_monitor(self) -> None:
        self._stop_event.set()
        if self._ws_loop and self._ws_loop.is_running():
            self._ws_loop.call_soon_threadsafe(self._ws_loop.stop)
        if self._ws_thread:
            self._ws_thread.join(timeout=5.0)
        log.info("[Sentiment] 爆倉監聽已停止")

    def _ws_run_loop(self, symbols: List[str]) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._ws_loop = loop
        try:
            loop.run_until_complete(self._ws_connect_all(symbols))
        except Exception as e:
            log.error(f"[Sentiment] WS 迴圈異常: {e}", exc_info=True)
        finally:
            loop.close()

    async def _ws_connect_all(self, symbols: List[str]) -> None:
        """同時監聽所有 symbol 的 @forceOrder 串流。"""
        # 組合成多串流 URL（Combined Stream）
        streams = "/".join(
            f"{s.lower()}@forceOrder" for s in symbols
        )
        url = f"wss://fstream.binance.com/stream?streams={streams}"

        try:
            import websockets
        except ImportError:
            log.error("[Sentiment] websockets 未安裝，爆倉監聽無法啟動")
            return

        while not self._stop_event.is_set():
            try:
                async with websockets.connect(
                    url, ping_interval=20, ping_timeout=10, close_timeout=5
                ) as ws:
                    log.info(f"[Sentiment] 已連接爆倉串流：{len(symbols)} 個交易對")
                    async for raw_msg in ws:
                        if self._stop_event.is_set():
                            break
                        self._handle_force_order(raw_msg)
            except Exception as e:
                log.warning(f"[Sentiment] 爆倉 WS 斷線: {e}，5s 後重連...")
                await asyncio.sleep(5)

    def _handle_force_order(self, raw: str) -> None:
        """解析 @forceOrder 爆倉訊息。"""
        try:
            msg = json.loads(raw)
            # Combined Stream 格式：{"stream": "btcusdt@forceOrder", "data": {...}}
            data = msg.get("data", msg)
            if data.get("e") != "forceOrder":
                return

            order = data.get("o", {})
            symbol  = order.get("s", "")
            side    = order.get("S", "BUY")   # BUY = 空頭爆倉；SELL = 多頭爆倉
            price   = float(order.get("ap", order.get("p", 0)))   # 成交均價
            qty     = float(order.get("q", 0))
            notional = price * qty

            if notional < 1000:   # 小額爆倉忽略（雜訊）
                return

            event = LiquidationEvent(
                symbol       = symbol,
                side         = side,
                price        = price,
                qty          = qty,
                notional_usd = notional,
            )
            with self._liq_lock:
                self._liq_events[symbol].append(event)

            if notional >= 1_000_000:
                log.info(
                    f"[Sentiment] ⚡ 大額爆倉 {symbol} {side} "
                    f"${notional/1e6:.2f}M @ {price:.2f}"
                )
        except Exception as e:
            log.debug(f"[Sentiment] 爆倉訊息解析失敗: {e}")

    # ════════════════════════════════════════════════════════
    # 統計查詢
    # ════════════════════════════════════════════════════════
    def get_liquidation_summary(
        self,
        symbol:     str,
        window_sec: Optional[float] = None,
    ) -> LiquidationSummary:
        """
        取得指定交易對在滑動視窗內的爆倉統計。
        回傳 LiquidationSummary（buy_volume_usd, sell_volume_usd 等）。
        """
        sym     = symbol.upper()
        window  = window_sec or self._liq_window
        cutoff  = time.time() - window
        buy_vol = sell_vol = 0.0
        count   = 0

        with self._liq_lock:
            for ev in self._liq_events.get(sym, []):
                if ev.timestamp < cutoff:
                    continue
                count += 1
                if ev.side == "BUY":
                    buy_vol  += ev.notional_usd
                else:
                    sell_vol += ev.notional_usd

        return LiquidationSummary(
            symbol          = sym,
            window_sec      = window,
            buy_volume_usd  = buy_vol,
            sell_volume_usd = sell_vol,
            event_count     = count,
        )

    def get_funding_rate_sync(self, symbol: str) -> Optional[float]:
        """同步查詢快取中的資金費率（不觸發 HTTP 請求）。"""
        with self._funding_lock:
            cached = self._funding_cache.get(symbol.upper())
            return cached.funding_rate if cached else None

    # ════════════════════════════════════════════════════════
    # ScoringEngine 輸出介面
    # ════════════════════════════════════════════════════════
    def to_scoring_ctx(self, symbol: str) -> dict:
        """
        整合資金費率與爆倉統計，輸出可直接傳入 ScoringEngine.evaluate()
        的 market_ctx 子字典。
        """
        ctx: dict = {}

        # 資金費率
        rate = self.get_funding_rate_sync(symbol)
        if rate is not None:
            ctx["funding_rate"] = rate

        # 爆倉統計
        liq = self.get_liquidation_summary(symbol)
        if liq.is_significant:
            ctx["liq_volume_usd"] = liq.total_volume_usd
            ctx["liq_side"]       = liq.dominant_side

        return ctx
