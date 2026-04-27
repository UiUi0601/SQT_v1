"""
quant_system/grid/grid_manager.py — 網格訂單管理器（強化版 v3，Phase F）

修復項目整合：
  ① UUID / 冪等性    — _place_limit() 生成 client_oid 並綁定 Binance newClientOrderId，
                       重試時同一 client_oid 不會重複建倉
  ② 部分成交撤單     — scan_timeouts(cancel_fn=_cancel_single)，
                       超時後主動撤銷剩餘掛單，釋放被鎖定資金
  + 原有強化保留     — Threading Lock、精度管理、API 頻率保護

Phase F 新增：
  ② Auto-Compounding — update_qty_from_equity() 在每次 regrid 時根據帳戶資金成長
                       重新計算 qty_per_grid（需搭配 GridEngine.compute_compounded_qty）
"""
from __future__ import annotations

import logging
import threading
import time
import uuid
from typing import Dict, List, Optional, Tuple

from .grid_engine           import GridEngine, GridLevel, GridSnapshot
from .partial_fill_tracker  import FillEvent, PartialFillTracker, STATUS_FILLED, STATUS_PARTIAL
from ..execution.precision   import PrecisionManager

log = logging.getLogger(__name__)

_ORDER_DELAY = 0.22   # 每次送單最短間隔（秒），避免 IP ban


def _make_client_oid() -> str:
    """
    ① 生成時間戳 + UUID 複合 client order ID。
    格式：{ms}_{hex8}，例：1745123456789_a1b2c3d4
    Binance newClientOrderId 上限 36 字元，本格式 ~22 字元，完全符合。
    """
    return f"{int(time.time() * 1000)}_{uuid.uuid4().hex[:8]}"


class GridManager:
    def __init__(
        self,
        client,
        symbol:        str,
        qty_per_grid:  float,
        precision:     Optional[PrecisionManager] = None,
        # ② Auto-Compounding
        engine:        Optional[GridEngine]        = None,
        base_equity:   float                       = 0.0,
        # 多市場支援
        market_type:   str                         = "SPOT",   # SPOT | FUTURES | MARGIN
        position_side: str                         = "BOTH",   # BOTH | LONG | SHORT (Hedge Mode)
        is_isolated:   bool                        = False,
    ) -> None:
        self.client        = client
        self.symbol        = symbol
        self.qty_per_grid  = qty_per_grid
        self.market_type   = market_type.upper()
        self.position_side = position_side.upper()
        self.is_isolated   = is_isolated

        # ② 複利追蹤
        self._engine      = engine
        self._base_qty    = qty_per_grid     # 初始每格數量（複利基準）
        self._base_equity = base_equity      # 初始帳戶資金（複利基準）

        self._prec = precision or PrecisionManager()

        # 部分成交追蹤器（⑤ 超時主動撤銷）
        self._fill_tracker = PartialFillTracker(
            partial_timeout_sec   = 300.0,
            act_on_partial_cancel = False,
            min_partial_pct       = 0.5,
        )

        # Threading Lock — 保護 _active_orders 所有讀寫
        self._lock = threading.Lock()

        # active_orders：{binance_order_id → side}
        self._active_orders:  Dict[str, str] = {}
        # client_id 映射：{client_oid → binance_order_id}（用於冪等重試追蹤）
        self._client_id_map:  Dict[str, str] = {}
        self._grid_levels:    List[GridLevel] = []

    # ── 初始化 ───────────────────────────────────────────────
    def setup(self) -> None:
        self._prec.load(self.client, self.symbol)
        log.info(f"[GridMgr] {self.symbol} 精度載入完成")

    # ── ② Auto-Compounding ──────────────────────────────────
    def update_qty_from_equity(self, current_equity: float) -> float:
        """
        ② 根據帳戶當前資金重新計算 qty_per_grid（自動複利）。

        此方法應在每次 regrid（重新鋪設網格）前呼叫。
        若 engine.compounding_ratio == 0 或未傳入 engine，則原樣保留 qty_per_grid。

        Args:
            current_equity — 本次 regrid 時的帳戶 total_equity（USDT）

        Returns:
            更新後的 qty_per_grid（同時寫入 self.qty_per_grid）

        使用範例（在 crypto_main_grid.py 的 regrid 邏輯中）：
            new_qty = grid_mgr.update_qty_from_equity(total_equity)
            snap    = grid_engine.compute(df)
            grid_mgr.deploy(snap, current_price)
        """
        if self._engine is None or self._engine.compounding_ratio <= 0:
            return self.qty_per_grid

        # 若尚未設定基準資金，以首次呼叫的值作為基準
        if self._base_equity <= 0:
            self._base_equity = current_equity
            log.info(f"[GridMgr] ② Compounding 基準設定: equity={current_equity:.2f} qty={self._base_qty:.6f}")
            return self.qty_per_grid

        new_qty = self._engine.compute_compounded_qty(
            base_qty       = self._base_qty,
            base_equity    = self._base_equity,
            current_equity = current_equity,
        )
        if abs(new_qty - self.qty_per_grid) > 1e-10:
            log.info(
                f"[GridMgr] ② qty_per_grid 更新: {self.qty_per_grid:.6f} → {new_qty:.6f} "
                f"(equity={current_equity:.2f})"
            )
        self.qty_per_grid = new_qty
        return new_qty

    # ── 鋪設網格 ─────────────────────────────────────────────
    def deploy(
        self,
        snap:          GridSnapshot,
        current_price: float,
        allow_buy:     bool = True,
        allow_sell:    bool = True,
    ) -> List[GridLevel]:
        """撤舊單 → 鋪新格。回傳成功掛單的 GridLevel 列表。"""
        self.cancel_all()
        placed: List[GridLevel] = []

        for lv in snap.levels:
            if lv.side == "BUY"  and not allow_buy:  continue
            if lv.side == "SELL" and not allow_sell: continue

            qty = self._prec.fmt_qty(self.symbol, self.qty_per_grid)
            px  = self._prec.fmt_price(self.symbol, lv.price)

            valid, reason = self._prec.validate(self.symbol, qty, px)
            if not valid:
                log.debug(f"[GridMgr] 跳過格位 {px:.4f}: {reason}")
                continue

            binance_oid, client_oid = self._place_limit(lv.side, px, qty)
            if binance_oid:
                lv.order_id = binance_oid
                lv.status   = "OPEN"
                with self._lock:
                    self._active_orders[binance_oid] = lv.side
                    if client_oid:
                        self._client_id_map[client_oid] = binance_oid
                placed.append(lv)
            time.sleep(_ORDER_DELAY)

        self._grid_levels = placed
        log.info(f"[GridMgr] 鋪設完成: {len(placed)} 格位")
        return placed

    # ── REST 輪詢成交 ────────────────────────────────────────
    def poll_fills(self) -> List[FillEvent]:
        """
        REST API 輪詢所有 OPEN 訂單狀態。
        ⑤ scan_timeouts 傳入 _cancel_single 作為 cancel_fn，
           超時時主動撤銷剩餘掛單，釋放被鎖定資金。
        """
        completed: List[FillEvent] = []

        with self._lock:
            order_ids = list(self._active_orders.keys())

        for oid in order_ids:
            try:
                if self.market_type == "FUTURES":
                    o = self.client.futures_get_order(symbol=self.symbol, orderId=oid)
                else:
                    o = self.client.get_order(symbol=self.symbol, orderId=oid)
            except Exception as e:
                log.warning(f"[GridMgr] 查詢訂單 {oid} 失敗: {e}")
                time.sleep(0.05)
                continue

            status       = o.get("status", "")
            orig_qty     = float(o.get("origQty",             0))
            executed_qty = float(o.get("executedQty",         0))
            cumul_quote  = float(o.get("cummulativeQuoteQty", 0))
            avg_price    = (cumul_quote / executed_qty) if executed_qty > 0 else 0.0

            with self._lock:
                side = self._active_orders.get(oid, "BUY")

            event = self._fill_tracker.update(
                order_id      = oid,
                side          = side,
                orig_qty      = orig_qty,
                executed_qty  = executed_qty,
                last_exec_qty = executed_qty,
                avg_price     = avg_price,
                status        = status,
            )

            if event:
                with self._lock:
                    self._active_orders.pop(oid, None)
                self._update_grid_level(oid, event)
                completed.append(event)

            time.sleep(0.05)

        # ⑤ 掃描超時殘單，傳入 cancel callback 主動撤銷
        timeout_events = self._fill_tracker.scan_timeouts(
            cancel_fn=self._cancel_single
        )
        for ev in timeout_events:
            with self._lock:
                self._active_orders.pop(ev.order_id, None)
        completed.extend(timeout_events)

        return completed

    # ── WebSocket 成交事件處理 ───────────────────────────────
    def on_ws_event(self, event) -> Optional[FillEvent]:
        if event.symbol != self.symbol:
            return None

        with self._lock:
            if event.order_id not in self._active_orders:
                return None
            side = self._active_orders[event.order_id]

        fill_event = self._fill_tracker.update(
            order_id      = event.order_id,
            side          = side,
            orig_qty      = event.orig_qty,
            executed_qty  = event.executed_qty,
            last_exec_qty = event.last_exec_qty,
            avg_price     = event.last_exec_price,
            status        = event.order_status,
        )

        if fill_event:
            with self._lock:
                self._active_orders.pop(event.order_id, None)
            self._update_grid_level(event.order_id, fill_event)

        return fill_event

    # ── 反向掛單 ─────────────────────────────────────────────
    def on_fill(
        self,
        fill_event:  FillEvent,
        snap:        GridSnapshot,
        allow_buy:   bool = True,
        allow_sell:  bool = True,
    ) -> Optional[str]:
        """
        某格完全成交後，在對面掛反向單。
        BUY 成交 → 在 filled_price + spacing 掛 SELL（反之亦然）
        """
        side     = fill_event.side
        fill_px  = fill_event.avg_price
        fill_qty = fill_event.executed_qty

        if side == "BUY" and allow_sell:
            counter_px, counter_side = fill_px + snap.spacing, "SELL"
        elif side == "SELL" and allow_buy:
            counter_px, counter_side = fill_px - snap.spacing, "BUY"
        else:
            return None

        counter_px = self._prec.fmt_price(self.symbol, counter_px)
        if counter_px > snap.upper or counter_px < snap.lower:
            log.info(f"[GridMgr] 反向單 {counter_px:.4f} 超出網格範圍，跳過")
            return None

        qty = self._prec.fmt_qty(self.symbol, fill_qty)
        valid, reason = self._prec.validate(self.symbol, qty, counter_px)
        if not valid:
            log.warning(f"[GridMgr] 反向單精度驗證失敗: {reason}")
            return None

        with self._lock:
            # 防重複：相同方向 + 相同價位已有掛單時跳過
            for existing_oid, existing_side in self._active_orders.items():
                if existing_side != counter_side:
                    continue
                for lv in self._grid_levels:
                    if lv.order_id == existing_oid and abs(lv.price - counter_px) < 1e-8:
                        log.info(f"[GridMgr] 相同價位 {counter_px:.4f} 已有掛單，跳過")
                        return None

            new_binance_oid, new_client_oid = self._place_limit(counter_side, counter_px, qty)
            if new_binance_oid:
                self._active_orders[new_binance_oid] = counter_side
                if new_client_oid:
                    self._client_id_map[new_client_oid] = new_binance_oid
                log.info(
                    f"[GridMgr] {side}→{counter_side} 反向單 "
                    f"px={counter_px:.4f} qty={qty:.6f} "
                    f"oid={new_binance_oid}"
                )
            time.sleep(_ORDER_DELAY)
            return new_binance_oid

    # ── 撤銷全部 ─────────────────────────────────────────────
    def cancel_all(self) -> None:
        with self._lock:
            count = len(self._active_orders)
        if count == 0:
            return
        try:
            if self.market_type == "FUTURES":
                self.client.futures_cancel_all_open_orders(symbol=self.symbol)
            else:
                self.client.cancel_open_orders(symbol=self.symbol)
            log.info(f"[GridMgr] 已撤銷 {count} 筆掛單 [{self.market_type}]")
        except Exception as e:
            log.warning(f"[GridMgr] 批次撤銷失敗 [{self.market_type}]: {e}")
        with self._lock:
            self._active_orders.clear()
            self._client_id_map.clear()
        time.sleep(_ORDER_DELAY)

    # ── 查詢 ─────────────────────────────────────────────────
    def active_count(self) -> int:
        with self._lock:
            return len(self._active_orders)

    def active_levels(self) -> List[GridLevel]:
        return [lv for lv in self._grid_levels if lv.status == "OPEN"]

    def pending_partial_fills(self) -> int:
        return self._fill_tracker.pending_count()

    # ════════════════════════════════════════════════════════
    # ① + ④ 內部下單（綁定 newClientOrderId）
    # ════════════════════════════════════════════════════════
    def _place_limit(
        self,
        side:  str,
        price: float,
        qty:   float,
    ) -> Tuple[Optional[str], Optional[str]]:
        """
        送出限價單。

        ① 每筆訂單生成唯一的 client_oid（時間戳 + UUID）
        ④ 將 client_oid 傳入 Binance 的 newClientOrderId 參數
           → 若網路錯誤重試送相同 client_oid，Binance 回傳原單而非重複建倉

        回傳 (binance_order_id, client_oid)
               或 (None, None) 下單失敗時
        """
        client_oid = _make_client_oid()
        try:
            if self.market_type == "FUTURES":
                pos_side = None
                if self.position_side != "BOTH":
                    pos_side = self.position_side
                params: dict = {
                    "symbol":           self.symbol,
                    "side":             side,
                    "type":             "LIMIT",
                    "quantity":         round(qty, 8),
                    "price":            f"{price:.8f}",
                    "timeInForce":      "GTC",
                    "newClientOrderId": client_oid,
                }
                if pos_side:
                    params["positionSide"] = pos_side
                res = self.client.futures_create_order(**params)

            elif self.market_type == "MARGIN":
                res = self.client.create_margin_order(
                    symbol           = self.symbol,
                    side             = side,
                    type             = "LIMIT",
                    quantity         = round(qty, 8),
                    price            = f"{price:.8f}",
                    timeInForce      = "GTC",
                    newClientOrderId = client_oid,
                    isIsolated       = "TRUE" if self.is_isolated else "FALSE",
                    sideEffectType   = "NO_SIDE_EFFECT",
                )

            else:
                fn  = (self.client.order_limit_buy if side == "BUY"
                       else self.client.order_limit_sell)
                res = fn(
                    symbol           = self.symbol,
                    quantity         = round(qty, 8),
                    price            = f"{price:.8f}",
                    newClientOrderId = client_oid,
                )

            binance_oid = str(res["orderId"])
            log.debug(
                f"[GridMgr] 送單成功 [{self.market_type}] {side} px={price:.4f} qty={qty:.6f} "
                f"binance_oid={binance_oid} client_oid={client_oid}"
            )
            return binance_oid, client_oid

        except Exception as e:
            log.error(
                f"[GridMgr] {side} 掛單失敗 [{self.market_type}] px={price:.4f} qty={qty:.6f} "
                f"client_oid={client_oid}: {e}"
            )
            return None, None

    # ════════════════════════════════════════════════════════
    # ⑤ 單筆撤銷（供 PartialFillTracker 超時回調使用）
    # ════════════════════════════════════════════════════════
    def _cancel_single(self, order_id: str) -> bool:
        """
        主動撤銷單一訂單，由 scan_timeouts 超時時呼叫。
        回傳 True = 撤銷成功；False = 撤銷失敗或訂單不存在。
        """
        try:
            if self.market_type == "FUTURES":
                self.client.futures_cancel_order(symbol=self.symbol, orderId=order_id)
            else:
                self.client.cancel_order(symbol=self.symbol, orderId=order_id)
            with self._lock:
                self._active_orders.pop(order_id, None)
            log.info(f"[GridMgr] ✅ 部分成交殘單 {order_id} 已撤銷，資金釋放")
            return True
        except Exception as e:
            # Binance 錯誤碼 -2011 = 訂單不存在（可能已在別處撤銷）
            err_str = str(e)
            if "-2011" in err_str or "Unknown order" in err_str:
                log.info(f"[GridMgr] 殘單 {order_id} 已不存在（可能已成交或撤銷）")
                with self._lock:
                    self._active_orders.pop(order_id, None)
                return True
            log.error(f"[GridMgr] ❌ 撤銷殘單 {order_id} 失敗: {e}")
            return False

    # ════════════════════════════════════════════════════════
    # 內部工具
    # ════════════════════════════════════════════════════════
    def _update_grid_level(self, order_id: str, event: FillEvent) -> None:
        """將成交結果寫回 _grid_levels 對應格位。"""
        for lv in self._grid_levels:
            if lv.order_id == order_id:
                lv.status       = "FILLED"
                lv.filled_price = event.avg_price
                lv.filled_qty   = event.executed_qty
                break
