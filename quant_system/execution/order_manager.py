"""
quant_system/execution/order_manager.py — 三業務線訂單管理器

支援業務線：
  SPOT    — 現貨限價/市價掛單
  FUTURES — U 本位永續合約（含預設 marginType + leverage、Hedge Mode）
  MARGIN  — 全倉 / 逐倉槓桿（含自動借幣 / 自動還幣）
"""
from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

log = logging.getLogger(__name__)


# ── 列舉 ────────────────────────────────────────────────────
class OrderStatus(str, Enum):
    PENDING   = "PENDING"
    SUBMITTED = "SUBMITTED"
    FILLED    = "FILLED"
    PARTIAL   = "PARTIAL"
    CANCELLED = "CANCELLED"
    REJECTED  = "REJECTED"
    ERROR     = "ERROR"


class OrderSide(str, Enum):
    BUY  = "BUY"
    SELL = "SELL"


class OrderType(str, Enum):
    LIMIT  = "LIMIT"
    MARKET = "MARKET"


# ── 訂單資料類別 ──────────────────────────────────────────────
@dataclass
class Order:
    symbol:         str
    side:           str                     # BUY / SELL
    order_type:     str                     # LIMIT / MARKET
    quantity:       float
    price:          Optional[float] = None

    # 多業務線擴充欄位
    market_type:    str   = "SPOT"          # SPOT / FUTURES / MARGIN
    position_side:  str   = "BOTH"          # BOTH / LONG / SHORT (Futures Hedge Mode)
    leverage:       int   = 1               # 槓桿倍數（Futures）
    margin_type:    str   = "CROSSED"       # CROSSED / ISOLATED（Futures）
    is_isolated:    bool  = False           # 逐倉槓桿（Margin）
    side_effect_type: str = "NO_SIDE_EFFECT"  # MARGIN_BUY / AUTO_REPAY（Margin）
    reduce_only:    bool  = False           # 只減倉（Futures）
    realized_pnl:   float = 0.0             # 實現盈虧（Futures 回報）

    # 系統欄位
    client_order_id: str  = field(default_factory=lambda: f"sqt_{uuid.uuid4().hex[:12]}")
    order_id:        str  = ""
    status:          str  = OrderStatus.PENDING
    filled_qty:      float = 0.0
    avg_price:       float = 0.0
    created_at:      float = field(default_factory=time.time)
    updated_at:      float = field(default_factory=time.time)
    raw_response:    Optional[Dict] = field(default=None, repr=False)


# ── 訂單管理器 ───────────────────────────────────────────────
class OrderManager:
    """
    三業務線統一訂單管理器。

    Parameters
    ----------
    client          : AsyncExchangeClient 實例（已設定對應業務線）
    metadata        : BinanceMetadata 實例（用於精度校正）
    hedge_mode      : Futures 是否為雙向持倉（Hedge Mode）
    on_fill         : 訂單完全成交回調 (order) -> None
    on_reject       : 訂單被拒回調 (order, reason) -> None
    on_partial      : 部分成交回調 (order) -> None
    max_orders      : 最大追蹤訂單數（超過則清理最舊已終態訂單）
    """

    def __init__(
        self,
        client,
        metadata=None,
        hedge_mode: bool = False,
        on_fill:    Optional[Callable] = None,
        on_reject:  Optional[Callable] = None,
        on_partial: Optional[Callable] = None,
        max_orders: int = 500,
    ):
        self._client    = client
        self._metadata  = metadata
        self._hedge     = hedge_mode
        self._on_fill   = on_fill
        self._on_reject = on_reject
        self._on_partial = on_partial
        self._max_orders = max_orders

        self._orders: Dict[str, Order] = {}   # client_order_id -> Order
        self._stats = {
            "submitted": 0, "filled": 0, "cancelled": 0,
            "rejected": 0,  "errors": 0,
        }

    # ════════════════════════════════════════════════════════
    # 公開介面
    # ════════════════════════════════════════════════════════
    async def submit_order(self, order: Order) -> Order:
        """
        提交訂單至對應業務線。
        自動執行：精度校正 → 路由 → 狀態更新 → 回調觸發。
        """
        self._floor_values(order)
        self._register(order)

        try:
            if order.market_type == "FUTURES":
                resp = await self._submit_futures(order)
            elif order.market_type == "MARGIN":
                resp = await self._submit_margin(order)
            else:
                resp = await self._submit_spot(order)

            order.order_id    = str(resp.get("orderId", ""))
            order.status      = OrderStatus.SUBMITTED
            order.raw_response = resp
            order.updated_at  = time.time()
            self._stats["submitted"] += 1
            log.info(
                f"[OM] 訂單提交成功 [{order.market_type}] "
                f"{order.side} {order.quantity} {order.symbol} "
                f"@ {order.price} | id={order.order_id}"
            )
        except Exception as e:
            order.status     = OrderStatus.ERROR
            order.updated_at = time.time()
            self._stats["errors"] += 1
            log.error(f"[OM] 訂單提交失敗 {order.client_order_id}: {e}", exc_info=True)
            if self._on_reject:
                try:
                    self._on_reject(order, str(e))
                except Exception:
                    pass
            raise

        return order

    async def cancel_order(self, client_order_id: str) -> bool:
        """取消訂單，依業務線呼叫對應 API。"""
        order = self._orders.get(client_order_id)
        if not order:
            log.warning(f"[OM] 找不到訂單 {client_order_id}")
            return False

        try:
            if order.market_type == "FUTURES":
                await self._client.cancel_futures_order(
                    order.symbol, order_id=order.order_id)
            else:
                await self._client.cancel_order(
                    order.symbol, order_id=order.order_id)
            order.status     = OrderStatus.CANCELLED
            order.updated_at = time.time()
            self._stats["cancelled"] += 1
            log.info(f"[OM] 訂單已取消 {client_order_id}")
            return True
        except Exception as e:
            log.error(f"[OM] 取消訂單失敗 {client_order_id}: {e}")
            return False

    async def cancel_all(self, symbol: str) -> int:
        """取消指定交易對的所有掛單。"""
        cancelled = 0
        try:
            if self._client._market.value == "FUTURES":
                await self._client.cancel_all_futures_orders(symbol)
            else:
                await self._client.cancel_all_orders(symbol)
            for order in self._orders.values():
                if (order.symbol == symbol and
                        order.status in (OrderStatus.SUBMITTED, OrderStatus.PARTIAL)):
                    order.status     = OrderStatus.CANCELLED
                    order.updated_at = time.time()
                    cancelled += 1
            self._stats["cancelled"] += cancelled
            log.info(f"[OM] 批次取消 {symbol}：{cancelled} 筆")
        except Exception as e:
            log.error(f"[OM] 批次取消失敗 {symbol}: {e}")
        return cancelled

    def on_execution_report(self, event) -> None:
        """
        處理 WebSocket executionReport / ORDER_TRADE_UPDATE。
        由外部 UserStream 事件迴圈呼叫。
        """
        coid   = getattr(event, "client_oid", "")
        status = getattr(event, "order_status", "")
        order  = self._orders.get(coid)
        if not order:
            return

        order.updated_at = time.time()

        if hasattr(event, "realized_pnl"):
            order.realized_pnl = event.realized_pnl

        if status == "FILLED":
            order.filled_qty = getattr(event, "executed_qty",
                               getattr(event, "orig_qty", order.quantity))
            order.avg_price  = getattr(event, "avg_price",
                               getattr(event, "last_exec_price", order.avg_price))
            order.status     = OrderStatus.FILLED
            self._stats["filled"] += 1
            log.info(f"[OM] 訂單完全成交 {coid} qty={order.filled_qty} avg={order.avg_price}")
            if self._on_fill:
                try:
                    self._on_fill(order)
                except Exception:
                    pass

        elif status in ("PARTIALLY_FILLED",):
            order.filled_qty = getattr(event, "executed_qty", order.filled_qty)
            order.status     = OrderStatus.PARTIAL
            if self._on_partial:
                try:
                    self._on_partial(order)
                except Exception:
                    pass

        elif status in ("CANCELED", "CANCELLED", "EXPIRED", "REJECTED"):
            order.status = OrderStatus.CANCELLED if "CANCEL" in status else OrderStatus.REJECTED
            if status == "REJECTED":
                self._stats["rejected"] += 1
                if self._on_reject:
                    try:
                        self._on_reject(order, status)
                    except Exception:
                        pass

    # ── 查詢 ─────────────────────────────────────────────────
    def get_order(self, client_order_id: str) -> Optional[Order]:
        return self._orders.get(client_order_id)

    def list_orders(
        self,
        symbol:  Optional[str] = None,
        status:  Optional[str] = None,
        market:  Optional[str] = None,
    ) -> List[Order]:
        result = list(self._orders.values())
        if symbol: result = [o for o in result if o.symbol == symbol]
        if status: result = [o for o in result if o.status == status]
        if market: result = [o for o in result if o.market_type == market]
        return sorted(result, key=lambda o: o.created_at, reverse=True)

    @property
    def stats(self) -> Dict[str, int]:
        return dict(self._stats)

    # ════════════════════════════════════════════════════════
    # 內部路由（依業務線）
    # ════════════════════════════════════════════════════════
    async def _submit_spot(self, order: Order) -> Dict:
        return await self._client.place_spot_order(
            symbol          = order.symbol,
            side            = order.side,
            order_type      = order.order_type,
            quantity        = order.quantity,
            price           = order.price,
            client_order_id = order.client_order_id,
        )

    async def _submit_futures(self, order: Order) -> Dict:
        # ① 預設保證金模式（忽略 -4046：已設定，無需更改）
        try:
            await self._client.set_margin_type(order.symbol, order.margin_type)
        except Exception as e:
            raw = str(e)
            if "-4046" not in raw:
                log.warning(f"[OM] set_margin_type 失敗（非 -4046）: {e}")

        # ② 設定槓桿倍數
        try:
            await self._client.set_leverage(order.symbol, order.leverage)
        except Exception as e:
            log.warning(f"[OM] set_leverage 失敗: {e}")

        # ③ Hedge Mode：自動附加 positionSide
        pos_side = None
        if self._hedge:
            pos_side = "LONG" if order.side == "BUY" else "SHORT"
        elif order.position_side != "BOTH":
            pos_side = order.position_side

        return await self._client.place_futures_order(
            symbol          = order.symbol,
            side            = order.side,
            order_type      = order.order_type,
            quantity        = order.quantity,
            price           = order.price,
            position_side   = pos_side,
            client_order_id = order.client_order_id,
            reduce_only     = order.reduce_only,
        )

    async def _submit_margin(self, order: Order) -> Dict:
        return await self._client.place_margin_order(
            symbol           = order.symbol,
            side             = order.side,
            order_type       = order.order_type,
            quantity         = order.quantity,
            price            = order.price,
            is_isolated      = order.is_isolated,
            side_effect_type = order.side_effect_type,
            client_order_id  = order.client_order_id,
        )

    # ════════════════════════════════════════════════════════
    # 內部工具
    # ════════════════════════════════════════════════════════
    def _floor_values(self, order: Order) -> None:
        """利用 BinanceMetadata 對價格/數量做截斷（floor）處理。"""
        if self._metadata is None:
            return
        try:
            rule = self._metadata.get_rule(order.symbol)
            if rule is None:
                return
            if order.price is not None:
                order.price = self._metadata.floor_price(order.symbol, order.price)
            order.quantity = self._metadata.floor_qty(order.symbol, order.quantity)
        except Exception as e:
            log.warning(f"[OM] _floor_values 失敗 {order.symbol}: {e}")

    def _register(self, order: Order) -> None:
        """登記訂單；超過上限時清理已終態的最舊訂單。"""
        if len(self._orders) >= self._max_orders:
            self._cleanup_old_orders_locked()
        self._orders[order.client_order_id] = order

    def _cleanup_old_orders_locked(self) -> None:
        terminal = {
            OrderStatus.FILLED, OrderStatus.CANCELLED,
            OrderStatus.REJECTED, OrderStatus.ERROR,
        }
        done = sorted(
            [o for o in self._orders.values() if o.status in terminal],
            key=lambda o: o.updated_at,
        )
        remove_n = max(1, len(done) // 2)
        for o in done[:remove_n]:
            del self._orders[o.client_order_id]
        log.debug(f"[OM] 清理 {remove_n} 筆已終態訂單，剩餘 {len(self._orders)}")
