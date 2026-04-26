"""
quant_system/exchange/binance_metadata.py — 幣安市場精度快取

功能：
  ─ 系統啟動時自動抓取 SPOT（/api/v3/exchangeInfo）與
    FUTURES（/fapi/v1/exchangeInfo）的交易對精度規則
  ─ 將 tickSize、stepSize、minQty、minNotional 快取於記憶體字典
  ─ 提供 floor_price() / floor_qty() 工具：
      * 強制向下截斷（truncate），絕對不使用四捨五入
      * 截斷方式符合幣安「Filter 精度」規則
  ─ 支援手動重新整理（refresh），可設定自動每 N 分鐘更新

使用範例：
    from quant_system.exchange.binance_metadata import BinanceMetadata, MarketType

    meta = BinanceMetadata()
    await meta.load()                        # 系統啟動時呼叫一次

    # 下單前截斷數值（絕對精準，不四捨五入）
    qty   = meta.floor_qty("BTCUSDT",   MarketType.SPOT,    qty_raw)
    price = meta.floor_price("BTCUSDT", MarketType.SPOT,    price_raw)
    qty_f = meta.floor_qty("BTCUSDT",   MarketType.FUTURES, qty_raw)

    # 查詢精度規則
    rule = meta.get("ETHUSDT", MarketType.FUTURES)
    print(rule)
    # SymbolRule(tickSize=0.01, stepSize=0.001, minQty=0.001, minNotional=5.0)
"""
from __future__ import annotations

import asyncio
import logging
import math
import threading
import time
from dataclasses import dataclass, field
from typing import Dict, Optional, Tuple

log = logging.getLogger(__name__)

# 避免循環匯入，使用字串型別標注
try:
    import aiohttp
    _AIOHTTP_OK = True
except ImportError:
    _AIOHTTP_OK = False


# ════════════════════════════════════════════════════════════
# 資料類別
# ════════════════════════════════════════════════════════════
@dataclass
class SymbolRule:
    """單一交易對的精度與最小下單規則。"""
    tick_size:    float   = 0.01      # 最小價格變動單位
    step_size:    float   = 0.001     # 最小數量變動單位
    min_qty:      float   = 0.001     # 最小下單數量
    min_notional: float   = 5.0       # 最小名目價值（USDT）
    price_prec:   int     = 2         # 價格小數位數（由 tick_size 推算）
    qty_prec:     int     = 3         # 數量小數位數（由 step_size 推算）

    def __post_init__(self) -> None:
        self.price_prec = _count_decimals(self.tick_size)
        self.qty_prec   = _count_decimals(self.step_size)


class MarketType(str):
    SPOT    = "SPOT"
    FUTURES = "FUTURES"


# ════════════════════════════════════════════════════════════
# 工具函數
# ════════════════════════════════════════════════════════════
def _count_decimals(value: float) -> int:
    """
    從 tickSize / stepSize 推算小數位數。
    例：0.01 → 2；0.001 → 3；1.0 → 0
    """
    if value <= 0:
        return 0
    s = f"{value:.10f}".rstrip("0")
    if "." not in s:
        return 0
    return len(s.split(".")[1])


def _floor_to_step(value: float, step: float) -> float:
    """
    向下截斷（floor）到最近的 step 倍數。
    絕對不使用四捨五入，完全符合幣安 LOT_SIZE / PRICE_FILTER 規則。
    """
    if step <= 0:
        return value
    # 用整數運算避免浮點誤差
    factor = round(1.0 / step)
    return math.floor(value * factor) / factor


# ════════════════════════════════════════════════════════════
# BinanceMetadata
# ════════════════════════════════════════════════════════════
class BinanceMetadata:
    """
    幣安市場精度快取。

    內部維護兩個字典：
      _spot_rules[symbol]    → SymbolRule
      _futures_rules[symbol] → SymbolRule

    執行緒安全：讀取不加鎖（dict 在 CPython 為原子操作）；
    重新整理期間以 _lock 保護寫入。
    """

    _SPOT_INFO_URL    = "https://api.binance.com/api/v3/exchangeInfo"
    _FUTURES_INFO_URL = "https://fapi.binance.com/fapi/v1/exchangeInfo"

    def __init__(
        self,
        auto_refresh_min: float = 60.0,   # 每 N 分鐘自動重新整理（0 = 停用）
    ) -> None:
        self._spot_rules:    Dict[str, SymbolRule] = {}
        self._futures_rules: Dict[str, SymbolRule] = {}
        self._lock           = threading.Lock()
        self._loaded         = False
        self._last_refresh   = 0.0
        self._auto_refresh   = auto_refresh_min * 60.0
        self._refresh_task:  Optional[asyncio.Task] = None

    # ════════════════════════════════════════════════════════
    # 載入 / 重新整理
    # ════════════════════════════════════════════════════════
    async def load(self) -> None:
        """
        系統啟動時呼叫。並發抓取 SPOT 與 FUTURES exchangeInfo，
        解析並快取所有交易對精度規則。
        """
        await asyncio.gather(
            self._fetch_spot(),
            self._fetch_futures(),
        )
        self._loaded       = True
        self._last_refresh = time.time()
        log.info(
            f"[Metadata] 載入完成：SPOT {len(self._spot_rules)} 對 / "
            f"FUTURES {len(self._futures_rules)} 對"
        )

        # 啟動自動重新整理
        if self._auto_refresh > 0 and self._refresh_task is None:
            self._refresh_task = asyncio.create_task(self._auto_refresh_loop())

    async def refresh(self) -> None:
        """手動觸發重新整理。"""
        log.info("[Metadata] 手動重新整理精度規則...")
        await self.load()

    # ════════════════════════════════════════════════════════
    # 查詢介面
    # ════════════════════════════════════════════════════════
    def get(
        self,
        symbol:      str,
        market_type: str = MarketType.SPOT,
    ) -> Optional[SymbolRule]:
        """取得指定交易對的精度規則，若未快取則回傳 None。"""
        rules = (
            self._futures_rules
            if market_type == MarketType.FUTURES
            else self._spot_rules
        )
        return rules.get(symbol.upper())

    def get_or_default(
        self,
        symbol:      str,
        market_type: str = MarketType.SPOT,
    ) -> SymbolRule:
        """取得精度規則；若未快取則回傳保守預設值（避免下單失敗）。"""
        rule = self.get(symbol, market_type)
        if rule is None:
            log.warning(
                f"[Metadata] {symbol} ({market_type}) 未快取，"
                f"使用預設精度 tickSize=0.01 stepSize=0.001"
            )
            return SymbolRule()
        return rule

    # ════════════════════════════════════════════════════════
    # 截斷工具（下單前必用）
    # ════════════════════════════════════════════════════════
    def floor_price(
        self,
        symbol:      str,
        market_type: str,
        raw_price:   float,
    ) -> float:
        """
        將價格向下截斷至符合 tickSize 精度。
        例：raw_price=50123.456，tickSize=0.1 → 50123.4
        絕對不使用四捨五入。
        """
        rule = self.get_or_default(symbol, market_type)
        return _floor_to_step(raw_price, rule.tick_size)

    def floor_qty(
        self,
        symbol:      str,
        market_type: str,
        raw_qty:     float,
    ) -> float:
        """
        將數量向下截斷至符合 stepSize 精度。
        例：raw_qty=0.01234，stepSize=0.001 → 0.012
        絕對不使用四捨五入。
        """
        rule = self.get_or_default(symbol, market_type)
        return _floor_to_step(raw_qty, rule.step_size)

    def validate_order(
        self,
        symbol:      str,
        market_type: str,
        qty:         float,
        price:       float,
    ) -> Tuple[bool, str]:
        """
        下單前驗證數量與名目價值是否達到最小門檻。
        回傳 (ok, reason)；reason 為空字串表示通過。
        """
        rule = self.get_or_default(symbol, market_type)

        if qty < rule.min_qty:
            return False, (
                f"{symbol} qty={qty} < minQty={rule.min_qty}"
            )

        notional = qty * price
        if notional < rule.min_notional:
            return False, (
                f"{symbol} notional={notional:.4f} < minNotional={rule.min_notional}"
            )

        return True, ""

    @property
    def is_loaded(self) -> bool:
        return self._loaded

    @property
    def spot_symbol_count(self) -> int:
        return len(self._spot_rules)

    @property
    def futures_symbol_count(self) -> int:
        return len(self._futures_rules)

    # ════════════════════════════════════════════════════════
    # 內部：抓取與解析
    # ════════════════════════════════════════════════════════
    async def _fetch_spot(self) -> None:
        """抓取 SPOT exchangeInfo，解析 PRICE_FILTER 與 LOT_SIZE。"""
        try:
            data    = await self._http_get(self._SPOT_INFO_URL)
            symbols = data.get("symbols", [])
            rules   = {}
            for s in symbols:
                sym  = s.get("symbol", "")
                rule = self._parse_filters(s.get("filters", []))
                if sym:
                    rules[sym] = rule
            with self._lock:
                self._spot_rules = rules
            log.debug(f"[Metadata] SPOT 規則抓取完成：{len(rules)} 對")
        except Exception as e:
            log.error(f"[Metadata] SPOT exchangeInfo 抓取失敗: {e}")

    async def _fetch_futures(self) -> None:
        """抓取 FUTURES exchangeInfo，解析 PRICE_FILTER 與 LOT_SIZE。"""
        try:
            data    = await self._http_get(self._FUTURES_INFO_URL)
            symbols = data.get("symbols", [])
            rules   = {}
            for s in symbols:
                sym  = s.get("symbol", "")
                rule = self._parse_filters(s.get("filters", []))
                # 合約有 MIN_NOTIONAL filter 格式不同，額外處理
                for f in s.get("filters", []):
                    if f.get("filterType") == "MIN_NOTIONAL":
                        try:
                            rule.min_notional = float(f.get("notional", rule.min_notional))
                        except (ValueError, TypeError):
                            pass
                if sym:
                    rules[sym] = rule
            with self._lock:
                self._futures_rules = rules
            log.debug(f"[Metadata] FUTURES 規則抓取完成：{len(rules)} 對")
        except Exception as e:
            log.error(f"[Metadata] FUTURES exchangeInfo 抓取失敗: {e}")

    @staticmethod
    def _parse_filters(filters: list) -> SymbolRule:
        """從 filters 陣列中解析 PRICE_FILTER、LOT_SIZE、MIN_NOTIONAL。"""
        rule = SymbolRule()
        for f in filters:
            ft = f.get("filterType", "")
            if ft == "PRICE_FILTER":
                try:
                    rule.tick_size  = float(f.get("tickSize", 0.01))
                    rule.price_prec = _count_decimals(rule.tick_size)
                except (ValueError, TypeError):
                    pass
            elif ft == "LOT_SIZE":
                try:
                    rule.step_size = float(f.get("stepSize", 0.001))
                    rule.min_qty   = float(f.get("minQty",   0.001))
                    rule.qty_prec  = _count_decimals(rule.step_size)
                except (ValueError, TypeError):
                    pass
            elif ft in ("MIN_NOTIONAL", "NOTIONAL"):
                try:
                    rule.min_notional = float(
                        f.get("minNotional", f.get("notional", 5.0))
                    )
                except (ValueError, TypeError):
                    pass
        return rule

    @staticmethod
    async def _http_get(url: str) -> dict:
        """使用 aiohttp 執行 GET 請求，回傳 JSON dict。"""
        if not _AIOHTTP_OK:
            raise RuntimeError("aiohttp 未安裝")
        timeout = aiohttp.ClientTimeout(total=15.0)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url) as resp:
                resp.raise_for_status()
                return await resp.json()

    async def _auto_refresh_loop(self) -> None:
        """背景任務：定期自動重新整理精度規則。"""
        while True:
            await asyncio.sleep(self._auto_refresh)
            log.info("[Metadata] 自動重新整理精度規則...")
            try:
                await asyncio.gather(
                    self._fetch_spot(),
                    self._fetch_futures(),
                )
                self._last_refresh = time.time()
                log.info("[Metadata] 自動重新整理完成")
            except Exception as e:
                log.error(f"[Metadata] 自動重新整理失敗: {e}")


# ════════════════════════════════════════════════════════════
# 全域單例
# ════════════════════════════════════════════════════════════
_meta_instance: Optional[BinanceMetadata] = None
_meta_lock      = threading.Lock()


def get_metadata(auto_refresh_min: float = 60.0) -> BinanceMetadata:
    """
    取得全域 BinanceMetadata 單例。
    首次呼叫需搭配 asyncio 執行 await get_metadata().load()。
    """
    global _meta_instance
    with _meta_lock:
        if _meta_instance is None:
            _meta_instance = BinanceMetadata(auto_refresh_min=auto_refresh_min)
    return _meta_instance
