"""
quant_system/execution/order_manager.py — 訂單生命週期管理（強化版）

修復項目：
  ① UUID 碰撞防護  — 時間戳毫秒 + UUID hex 前綴，碰撞機率趨近於零
  ② 記憶體洩漏修復 — 自動清理 FILLED/CANCELLED 舊單，上限 1000 筆
  ③ PENDING 死鎖防護 — has_pending() 內建 30 秒超時自動解鎖
  ④ 冪等下單        — 每筆訂單的 client_order_id 供 Binance newClientOrderId 使用
  ⑤ 執行緒安全      — 所有公開方法持有 threading.Lock
"""
from __future__ import annotations

import logging
import threading
import time
import uuid
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, Optional, Tuple

log = logging.getLogger(__name__)

# ── 常數 ─────────────────────────────────────────────────────
_MAX_ORDERS:       int   = 1000      # _orders 字典上限
_COMPLETED_TTL:    float = 3600.0    # 已完成訂單保留時長（秒），1 小時後清除
_PENDING_TIMEOUT:  float = 30.0      # PENDING 狀態超時閾值（秒）


def _make_client_order_id() -> str:
    """
    生成不易碰撞的訂單 ID，格式：{ms_timestamp}_{uuid_hex8}
    長度約 22 字元，符合 Binance newClientOrderId ≤ 36 字元的規範。

    例：1745123456789_a1b2c3d4
    """
    ms  = int(time.time() * 1000)
    uid = uuid.uuid4().hex[:8]
    return f"{ms}_{uid}"


@dataclass
class Order:
    order_id:         str    # 內部 client_order_id（同時作為 Binance newClientOrderId）
    symbol:           str
    side:             str           # BUY | SELL
    order_type:       str           # MARKET | LIMIT
    quantity:         float
    strategy:         str   = ""
    status:           str   = "PENDING"  # PENDING | FILLED | REJECTED | CANCELLED | TIMEOUT
    fill_price:       float = 0.0
    fill_qty:         float = 0.0
    fee:              float = 0.0
    binance_order_id: str   = ""    # Binance 回傳的整數 orderId（字串化儲存）
    reject_reason:    str   = ""
    created_ts:       float = field(default_factory=time.time)   # 用於超時判斷
    created_at:       str   = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    filled_at:        str   = ""


class OrderManager:
    """
    管理訂單生命週期，確保同一標的不重複送單。

    設計要點：
      - 所有公開方法持有同一把 Lock，確保多執行緒安全
      - _orders 使用 OrderedDict 以插入順序追蹤，方便 LRU 清理
      - _pending 映射 symbol → client_order_id
    """

    def __init__(
        self,
        max_orders:      int   = _MAX_ORDERS,
        completed_ttl:   float = _COMPLETED_TTL,
        pending_timeout: float = _PENDING_TIMEOUT,
    ) -> None:
        self._lock    = threading.Lock()
        self._orders: OrderedDict[str, Order] = OrderedDict()
        self._pending: Dict[str, str] = {}   # symbol → client_order_id

        self._max_orders      = max_orders
        self._completed_ttl   = completed_ttl
        self._pending_timeout = pending_timeout

    # ════════════════════════════════════════════════════════
    # ① 建立訂單（含時間戳 ID 生成）
    # ════════════════════════════════════════════════════════
    def create_order(
        self,
        symbol:     str,
        side:       str,
        order_type: str,
        quantity:   float,
        strategy:   str = "",
    ) -> Optional[Order]:
        """
        建立新訂單。同一 symbol 有未超時的 PENDING 訂單時拒絕建立。
        回傳 Order（含 order_id 作為 Binance newClientOrderId）或 None。
        """
        with self._lock:
            # ③ PENDING 超時檢查（在 has_pending 內部邏輯）
            if self._has_pending_locked(symbol):
                log.warning(
                    f"[OrderMgr] {symbol} 已有 PENDING 訂單 "
                    f"{self._pending.get(symbol)}，拒絕重複送單"
                )
                return None

            # ① 時間戳 + UUID 複合 ID
            oid = _make_client_order_id()

            order = Order(
                order_id   = oid,
                symbol     = symbol,
                side       = side,
                order_type = order_type,
                quantity   = quantity,
                strategy   = strategy,
            )

            self._orders[oid] = order
            self._pending[symbol] = oid

            log.debug(
                f"[OrderMgr] 新訂單 client_oid={oid} "
                f"{side} {symbol} qty={quantity:.6f}"
            )

            # ② 建立後觸發一次清理（懶清理，不影響熱路徑效能）
            self._cleanup_old_orders_locked()

            return order

    # ════════════════════════════════════════════════════════
    # 成交回調
    # ════════════════════════════════════════════════════════
    def fill_order(
        self,
        order_id:         str,
        fill_price:       float,
        fill_qty:         float,
        fee:              float = 0.0,
        binance_order_id: str   = "",
    ) -> None:
        with self._lock:
            o = self._orders.get(order_id)
            if not o:
                return
            o.status           = "FILLED"
            o.fill_price       = fill_price
            o.fill_qty         = fill_qty
            o.fee              = fee
            o.binance_order_id = binance_order_id
            o.filled_at        = datetime.now(timezone.utc).isoformat()
            self._pending.pop(o.symbol, None)
            log.debug(
                f"[OrderMgr] 成交 {order_id} "
                f"px={fill_price:.4f} qty={fill_qty:.6f} fee={fee:.4f}"
            )

    def reject_order(self, order_id: str, reason: str = "") -> None:
        with self._lock:
            o = self._orders.get(order_id)
            if not o:
                return
            o.status        = "REJECTED"
            o.reject_reason = reason
            self._pending.pop(o.symbol, None)
            log.warning(f"[OrderMgr] 拒絕 {order_id}: {reason}")

    def cancel_pending(self, symbol: str) -> None:
        with self._lock:
            oid = self._pending.pop(symbol, None)
            if oid and oid in self._orders:
                self._orders[oid].status = "CANCELLED"
                log.info(f"[OrderMgr] 手動撤銷 {symbol} 的 PENDING 訂單 {oid}")

    # ════════════════════════════════════════════════════════
    # ③ PENDING 超時查詢（30 秒自動解鎖）
    # ════════════════════════════════════════════════════════
    def has_pending(self, symbol: str) -> bool:
        with self._lock:
            return self._has_pending_locked(symbol)

    def _has_pending_locked(self, symbol: str) -> bool:
        """需在 _lock 內呼叫。"""
        oid = self._pending.get(symbol)
        if not oid:
            return False

        order = self._orders.get(oid)
        if order is None:
            # 孤立的 pending 條目，清除
            del self._pending[symbol]
            log.warning(f"[OrderMgr] 清除孤立 pending entry: {symbol}")
            return False

        # 非 PENDING 狀態（已成交/撤銷但 _pending 未清除）
        if order.status != "PENDING":
            del self._pending[symbol]
            return False

        # ── 超時檢查 ──────────────────────────────────────
        age = time.time() - order.created_ts
        if age > self._pending_timeout:
            order.status = "TIMEOUT"
            del self._pending[symbol]
            log.warning(
                f"[OrderMgr] ⏱ {symbol} PENDING 訂單 {oid} "
                f"超時 {age:.0f}s（上限 {self._pending_timeout:.0f}s），"
                f"自動解鎖，允許重新送單"
            )
            return False

        return True

    # ════════════════════════════════════════════════════════
    # 查詢
    # ════════════════════════════════════════════════════════
    def get_order(self, order_id: str) -> Optional[Order]:
        with self._lock:
            return self._orders.get(order_id)

    def get_pending_order(self, symbol: str) -> Optional[Order]:
        """取得 symbol 目前 PENDING 中的訂單（若存在）。"""
        with self._lock:
            oid = self._pending.get(symbol)
            return self._orders.get(oid) if oid else None

    def stats(self) -> Dict:
        with self._lock:
            total     = len(self._orders)
            pending   = len(self._pending)
            completed = sum(
                1 for o in self._orders.values()
                if o.status in ("FILLED", "CANCELLED", "REJECTED", "TIMEOUT")
            )
            return {
                "total":     total,
                "pending":   pending,
                "completed": completed,
                "active":    total - completed,
            }

    # ════════════════════════════════════════════════════════
    # ② 記憶體清理（需在 _lock 內呼叫）
    # ════════════════════════════════════════════════════════
    def _cleanup_old_orders_locked(self) -> None:
        """
        保留策略：
          1. 清除 filled_at 距今超過 _completed_ttl 的已完成訂單
          2. 若仍超過 _max_orders，按插入順序刪除最舊的已完成訂單
        PENDING 訂單不會被清除（防止意外解鎖）。
        """
        if len(self._orders) <= self._max_orders:
            return

        now = time.time()
        terminal = {"FILLED", "CANCELLED", "REJECTED", "TIMEOUT"}

        # 第一輪：清除 TTL 已到期的已完成訂單
        to_remove = []
        for oid, order in self._orders.items():
            if order.status not in terminal:
                continue
            # 以 filled_at 或 created_ts 計算年齡
            ref_ts = order.created_ts
            if order.filled_at:
                try:
                    ref_ts = datetime.fromisoformat(order.filled_at).timestamp()
                except Exception:
                    pass
            if (now - ref_ts) > self._completed_ttl:
                to_remove.append(oid)

        for oid in to_remove:
            self._orders.pop(oid, None)

        # 第二輪：若仍超過上限，按插入順序刪除最舊的已完成訂單
        if len(self._orders) > self._max_orders:
            excess = len(self._orders) - self._max_orders
            removed = 0
            for oid in list(self._orders.keys()):
                if removed >= excess:
                    break
                if self._orders[oid].status in terminal:
                    self._orders.pop(oid, None)
                    removed += 1

        if to_remove or removed > 0:
            log.debug(
                f"[OrderMgr] 清理完成：移除 {len(to_remove) + (removed if 'removed' in dir() else 0)} 筆"
                f"，剩餘 {len(self._orders)} 筆"
            )

    # ════════════════════════════════════════════════════════
    # 手動觸發完整清理（可由外部排程呼叫）
    # ════════════════════════════════════════════════════════
    def cleanup(self) -> Tuple[int, int]:
        """
        手動觸發清理。回傳 (清理前筆數, 清理後筆數)。
        """
        with self._lock:
            before = len(self._orders)
            self._cleanup_old_orders_locked()
            after = len(self._orders)
        log.info(f"[OrderMgr] 手動清理：{before} → {after} 筆")
        return before, after
