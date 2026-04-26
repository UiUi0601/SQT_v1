"""
quant_system/grid/partial_fill_tracker.py — 部分成交追蹤器（強化版）

Binance 訂單狀態：
  NEW              → 已送出，尚未成交
  PARTIALLY_FILLED → 部分成交（executedQty < origQty）
  FILLED           → 完全成交
  CANCELED         → 已撤銷（可能帶有部分成交量）
  REJECTED         → 被拒絕（通常 executedQty = 0）
  EXPIRED          → 過期（FOK / IOC 訂單）

修復項目：
  ⑤ 部分成交超時主動撤單 — scan_timeouts(cancel_fn) 接收 cancel callback，
     超時時主動呼叫 Binance API 撤銷剩餘數量，釋放被鎖定資金。
"""
from __future__ import annotations
import logging
import time
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Tuple

log = logging.getLogger(__name__)

# 訂單狀態常數（與 Binance API 回傳值一致）
STATUS_NEW      = "NEW"
STATUS_PARTIAL  = "PARTIALLY_FILLED"
STATUS_FILLED   = "FILLED"
STATUS_CANCELED = "CANCELED"
STATUS_REJECTED = "REJECTED"
STATUS_EXPIRED  = "EXPIRED"

TERMINAL_STATUSES = {STATUS_FILLED, STATUS_CANCELED, STATUS_REJECTED, STATUS_EXPIRED}


@dataclass
class FillEvent:
    """一次成交事件（可能是部分或完整）"""
    order_id:       str
    side:           str        # BUY / SELL
    orig_qty:       float      # 原始下單量
    executed_qty:   float      # 本次累積成交量
    last_exec_qty:  float      # 本次新增成交量
    avg_price:      float      # 加權均價
    status:         str
    is_complete:    bool       # True = 已完全成交；False = 殘單（超時/撤銷）
    cancel_attempted: bool = False   # True = 超時時已嘗試主動撤銷
    timestamp:      float = field(default_factory=time.time)


@dataclass
class _OrderState:
    order_id:         str
    side:             str
    orig_qty:         float
    executed_qty:     float = 0.0
    cumulative_quote: float = 0.0   # 累積成交金額（用於加權均價計算）
    status:           str   = STATUS_NEW
    fills: List[Tuple[float, float]] = field(default_factory=list)  # [(qty, price), ...]
    created_at:       float = field(default_factory=time.time)


class PartialFillTracker:
    """
    追蹤每個 order_id 的成交進度。
    只在完全成交（或殘單超時主動撤銷後）才通知外部系統。
    """

    def __init__(
        self,
        partial_timeout_sec:   float = 300.0,
        act_on_partial_cancel: bool  = False,
        min_partial_pct:       float = 0.5,
    ) -> None:
        self._orders: Dict[str, _OrderState] = {}
        self.timeout         = partial_timeout_sec
        self.act_on_cancel   = act_on_partial_cancel
        self.min_partial_pct = min_partial_pct

    # ════════════════════════════════════════════════════════
    # 外部呼叫：更新訂單狀態
    # ════════════════════════════════════════════════════════
    def update(
        self,
        order_id:      str,
        side:          str,
        orig_qty:      float,
        executed_qty:  float,
        last_exec_qty: float,
        avg_price:     float,
        status:        str,
    ) -> Optional[FillEvent]:
        """
        更新訂單狀態，回傳 FillEvent（若需要觸發）或 None（繼續等待）。

        呼叫時機：
          - REST API 輪詢（poll_fills）
          - WebSocket executionReport 事件
        """
        state = self._orders.get(order_id)
        if state is None:
            state = _OrderState(
                order_id = order_id,
                side     = side,
                orig_qty = orig_qty,
            )
            self._orders[order_id] = state

        # 防止網路重試送來舊狀態導致倒退
        if executed_qty <= state.executed_qty and status == STATUS_PARTIAL:
            return None

        # 累積成交量（增量更新）
        new_qty = executed_qty - state.executed_qty
        if new_qty > 0 and avg_price > 0:
            state.cumulative_quote += new_qty * avg_price
            state.fills.append((new_qty, avg_price))

        state.executed_qty = executed_qty
        state.status       = status

        # ── 完全成交 ─────────────────────────────────────────
        if status == STATUS_FILLED:
            event = self._make_event(state, is_complete=True)
            self._cleanup(order_id)
            log.info(
                f"[FillTracker] 完全成交 {order_id} side={side} "
                f"qty={executed_qty:.6f} avg_px={event.avg_price:.4f}"
            )
            return event

        # ── 撤銷 / 拒絕 / 到期（含部分成交殘單）───────────────
        if status in (STATUS_CANCELED, STATUS_EXPIRED, STATUS_REJECTED):
            fill_ratio = executed_qty / (orig_qty + 1e-10)
            should_act = (
                self.act_on_cancel
                and executed_qty > 0
                and fill_ratio >= self.min_partial_pct
            )
            if should_act:
                event = self._make_event(state, is_complete=False)
                self._cleanup(order_id)
                log.warning(
                    f"[FillTracker] 撤銷帶殘單 {order_id} "
                    f"filled={executed_qty:.6f}/{orig_qty:.6f} ({fill_ratio:.0%})"
                )
                return event
            else:
                log.info(
                    f"[FillTracker] 訂單終結 {order_id} status={status} "
                    f"filled={executed_qty:.6f}/{orig_qty:.6f} → 忽略殘單"
                )
                self._cleanup(order_id)
                return None

        # ── 部分成交，繼續等待 ────────────────────────────────
        if status == STATUS_PARTIAL:
            fill_ratio = executed_qty / (orig_qty + 1e-10)
            log.debug(
                f"[FillTracker] 部分成交 {order_id} "
                f"{fill_ratio:.1%} ({executed_qty:.6f}/{orig_qty:.6f})"
            )
            return None

        return None

    # ════════════════════════════════════════════════════════
    # ⑤ 超時掃描（主動撤銷剩餘掛單）
    # ════════════════════════════════════════════════════════
    def scan_timeouts(
        self,
        cancel_fn: Optional[Callable[[str], bool]] = None,
    ) -> List[FillEvent]:
        """
        掃描超時訂單（長時間停留在 PARTIALLY_FILLED）。

        cancel_fn — 接收 order_id 字串，嘗試撤銷後回傳 True/False。
                    若提供，超時時會主動呼叫以釋放被鎖定的資金。
                    若為 None，僅產生 FillEvent 但不主動撤銷。

        回傳需要處理的殘單 FillEvent 清單。
        """
        now    = time.time()
        events: List[FillEvent] = []

        for oid, state in list(self._orders.items()):
            age = now - state.created_at
            if age <= self.timeout:
                continue
            if state.executed_qty <= 0:
                # 完全未成交的超時訂單：直接清除，無需回調
                log.warning(
                    f"[FillTracker] 訂單 {oid} 超時 {age/60:.1f} 分鐘且無任何成交，清除追蹤"
                )
                self._cleanup(oid)
                if cancel_fn:
                    self._try_cancel(oid, cancel_fn, "完全未成交超時")
                continue

            # 有部分成交量的超時訂單
            log.warning(
                f"[FillTracker] 訂單 {oid} 超時 {age/60:.1f} 分鐘，"
                f"成交 {state.executed_qty:.6f}/{state.orig_qty:.6f} — "
                f"{'主動撤銷剩餘數量' if cancel_fn else '僅產生殘單事件'}"
            )

            # ⑤ 主動撤銷剩餘掛單
            cancel_ok = False
            if cancel_fn:
                cancel_ok = self._try_cancel(oid, cancel_fn, "部分成交超時")

            event = self._make_event(state, is_complete=False)
            event.cancel_attempted = cancel_fn is not None
            self._cleanup(oid)
            events.append(event)

        return events

    # ════════════════════════════════════════════════════════
    # 查詢
    # ════════════════════════════════════════════════════════
    def get_state(self, order_id: str) -> Optional[_OrderState]:
        return self._orders.get(order_id)

    def is_tracking(self, order_id: str) -> bool:
        return order_id in self._orders

    def pending_count(self) -> int:
        return len(self._orders)

    # ════════════════════════════════════════════════════════
    # 內部工具
    # ════════════════════════════════════════════════════════
    def _make_event(self, state: _OrderState, is_complete: bool) -> FillEvent:
        avg_price = (
            state.cumulative_quote / state.executed_qty
            if state.executed_qty > 0 else 0.0
        )
        return FillEvent(
            order_id      = state.order_id,
            side          = state.side,
            orig_qty      = state.orig_qty,
            executed_qty  = state.executed_qty,
            last_exec_qty = state.executed_qty,
            avg_price     = avg_price,
            status        = state.status,
            is_complete   = is_complete,
        )

    def _cleanup(self, order_id: str) -> None:
        self._orders.pop(order_id, None)

    @staticmethod
    def _try_cancel(
        order_id:  str,
        cancel_fn: Callable[[str], bool],
        reason:    str,
    ) -> bool:
        """安全呼叫 cancel_fn，捕捉所有例外，回傳是否成功。"""
        try:
            ok = cancel_fn(order_id)
            if ok:
                log.info(f"[FillTracker] ✅ 主動撤銷成功 {order_id}（{reason}），資金已釋放")
            else:
                log.warning(f"[FillTracker] ⚠ 主動撤銷回傳失敗 {order_id}（{reason}）")
            return bool(ok)
        except Exception as e:
            log.error(f"[FillTracker] ❌ 主動撤銷例外 {order_id}（{reason}）: {e}")
            return False
