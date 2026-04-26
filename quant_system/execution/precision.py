"""
quant_system/execution/precision.py — Binance 下單精度處理

負責從 Binance exchange_info 讀取每個幣種的精度規格，
並提供正確的截斷（floor）函數，避免 API 報錯或產生粉塵。

規格來源：
  LOT_SIZE      → stepSize（數量精度）、minQty、maxQty
  PRICE_FILTER  → tickSize（價格精度）、minPrice、maxPrice
  MIN_NOTIONAL  → minNotional（最小名義金額）
  MARKET_LOT_SIZE → 市價單的最小/最大數量

精度處理原則：
  - 一律使用 floor（向下截斷），非四捨五入
  - 原因：四捨五入可能使實際數量超過餘額或超過最大限制
"""
from __future__ import annotations
import logging
import math
import time
from dataclasses import dataclass, field
from typing import Dict, Optional, Tuple

log = logging.getLogger(__name__)

# 快取有效期（秒）——精度規格基本不變，每小時刷新一次即可
_CACHE_TTL = 3600


@dataclass
class SymbolFilter:
    # LOT_SIZE（限價單數量）
    step_size:    float = 1e-8
    min_qty:      float = 1e-8
    max_qty:      float = 9e9
    # PRICE_FILTER
    tick_size:    float = 0.01
    min_price:    float = 0.0
    max_price:    float = 9e9
    # MIN_NOTIONAL
    min_notional: float = 10.0
    # MARKET_LOT_SIZE（市價單）
    market_step:  float = 1e-8
    market_min:   float = 1e-8
    market_max:   float = 9e9
    # 精度位數（由 step_size / tick_size 推算）
    qty_decimals:   int = 8
    price_decimals: int = 2
    # 載入時間
    loaded_at:    float = field(default_factory=time.time)


def _decimals(step: float) -> int:
    """由 step_size 推算小數位數，e.g. 0.001 → 3，0.1 → 1"""
    if step <= 0:
        return 8
    s = f"{step:.10f}".rstrip("0")
    if "." not in s:
        return 0
    return len(s.split(".")[1])


class PrecisionManager:
    """
    全域精度管理器（單例模式）。
    在 GridManager.setup() 時呼叫 load()，後續直接使用 fmt_qty / fmt_price。
    """

    def __init__(self) -> None:
        self._filters: Dict[str, SymbolFilter] = {}

    # ── 載入 ─────────────────────────────────────────────────
    def load(self, client, symbol: str, force: bool = False) -> SymbolFilter:
        """
        從 Binance 載入 symbol 的精度規格。
        若快取未過期且非強制刷新，直接回傳快取。
        """
        cached = self._filters.get(symbol)
        if cached and not force and (time.time() - cached.loaded_at < _CACHE_TTL):
            return cached

        try:
            info = client.get_symbol_info(symbol)
            if not info:
                log.warning(f"[Precision] {symbol} 查無 symbol_info，使用預設值")
                return self._default(symbol)

            filters: Dict[str, dict] = {
                f["filterType"]: f for f in info.get("filters", [])
            }

            lot   = filters.get("LOT_SIZE", {})
            price = filters.get("PRICE_FILTER", {})
            notio = filters.get("MIN_NOTIONAL", {})
            mkt   = filters.get("MARKET_LOT_SIZE", {})

            step  = float(lot.get("stepSize",    "0.00000001"))
            tick  = float(price.get("tickSize",  "0.01"))

            sf = SymbolFilter(
                step_size    = step,
                min_qty      = float(lot.get("minQty",     "0.00000001")),
                max_qty      = float(lot.get("maxQty",     "9000000")),
                tick_size    = tick,
                min_price    = float(price.get("minPrice", "0")),
                max_price    = float(price.get("maxPrice", "9000000")),
                min_notional = float(notio.get("minNotional", "10")),
                market_step  = float(mkt.get("stepSize",   step)),
                market_min   = float(mkt.get("minQty",     "0.00000001")),
                market_max   = float(mkt.get("maxQty",     "9000000")),
                qty_decimals   = _decimals(step),
                price_decimals = _decimals(tick),
                loaded_at    = time.time(),
            )
            self._filters[symbol] = sf
            log.info(
                f"[Precision] {symbol} 載入完成 "
                f"step={step} tick={tick} minNotional={sf.min_notional}"
            )
            return sf

        except Exception as e:
            log.error(f"[Precision] {symbol} 載入失敗: {e}")
            return self._default(symbol)

    # ── 格式化 ───────────────────────────────────────────────
    def fmt_qty(self, symbol: str, qty: float,
                market_order: bool = False) -> float:
        """
        對數量做 floor 截斷至 step_size 精度。
        市價單使用 market_step（通常相同，但部分幣種不同）。
        """
        sf   = self._get(symbol)
        step = sf.market_step if market_order else sf.step_size
        if step <= 0:
            return qty
        result = math.floor(qty / step) * step
        # 避免浮點數尾數問題（e.g. 0.009999999 → round to decimals）
        return round(result, sf.qty_decimals)

    def fmt_price(self, symbol: str, price: float) -> float:
        """對價格做 floor 截斷至 tick_size 精度。"""
        sf   = self._get(symbol)
        tick = sf.tick_size
        if tick <= 0:
            return price
        result = math.floor(price / tick) * tick
        return round(result, sf.price_decimals)

    # ── 驗證 ─────────────────────────────────────────────────
    def validate(
        self,
        symbol:       str,
        qty:          float,
        price:        float,
        market_order: bool = False,
    ) -> Tuple[bool, str]:
        """
        驗證 qty / price 是否符合所有 Binance 規格。
        回傳 (is_valid, reason)。
        """
        sf = self._get(symbol)

        min_q = sf.market_min if market_order else sf.min_qty
        max_q = sf.market_max if market_order else sf.max_qty

        if qty < min_q:
            return False, f"qty={qty:.8f} < minQty={min_q:.8f}"
        if qty > max_q:
            return False, f"qty={qty:.8f} > maxQty={max_q:.8f}"
        if price > 0 and sf.min_price > 0 and price < sf.min_price:
            return False, f"price={price:.8f} < minPrice={sf.min_price:.8f}"
        if price > 0 and price > sf.max_price:
            return False, f"price={price:.8f} > maxPrice={sf.max_price:.8f}"
        if price > 0:
            notional = qty * price
            if notional < sf.min_notional:
                return False, (
                    f"notional={notional:.4f} < minNotional={sf.min_notional:.4f} "
                    f"（建議 qty >= {sf.min_notional/price:.8f}）"
                )
        return True, ""

    # ── 取得規格 ─────────────────────────────────────────────
    def get_filter(self, symbol: str) -> SymbolFilter:
        return self._get(symbol)

    # ── 內部 ─────────────────────────────────────────────────
    def _get(self, symbol: str) -> SymbolFilter:
        return self._filters.get(symbol) or self._default(symbol)

    def _default(self, symbol: str) -> SymbolFilter:
        sf = SymbolFilter()
        self._filters[symbol] = sf
        return sf


# 全域單例
precision_manager = PrecisionManager()
