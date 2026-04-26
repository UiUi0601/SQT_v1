"""
quant_system/exchange/reconciler.py — 啟動對帳器 (Reconciliation)

功能：
  程式啟動時，將 SQLite DB 中的格位狀態與 Binance 即時掛單進行比對，
  修復三種不一致情況：
    A. DB=OPEN  但 Binance 無此單  → 可能已成交或已撤銷 → 查成交歷史確認
    B. DB=OPEN  且 Binance 有此單  → 狀態一致，保留
    C. DB=FILLED/CANCELLED 但 Binance 仍有掛單 → 撤銷 Binance 殘留單

使用方式：
  from quant_system.exchange.reconciler import Reconciler

  rec = Reconciler(client=binance_client, db=trade_db)
  report = rec.run()          # 同步對帳，回傳 ReconcileReport
  print(report.summary)
"""
from __future__ import annotations
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Set

log = logging.getLogger(__name__)


@dataclass
class ReconcileAction:
    """單筆對帳動作記錄。"""
    grid_id:   int
    symbol:    str
    order_id:  str
    action:    str    # "mark_filled" | "mark_cancelled" | "cancel_binance" | "ok"
    detail:    str
    success:   bool = True


@dataclass
class ReconcileReport:
    """完整對帳報告。"""
    started_at:     float = field(default_factory=time.time)
    finished_at:    float = 0.0
    actions:        List[ReconcileAction] = field(default_factory=list)
    errors:         List[str]            = field(default_factory=list)

    @property
    def duration_ms(self) -> float:
        return (self.finished_at - self.started_at) * 1000

    @property
    def summary(self) -> str:
        ts = datetime.fromtimestamp(self.started_at, tz=timezone.utc).strftime("%H:%M:%S UTC")
        ok_cnt    = sum(1 for a in self.actions if a.action == "ok")
        fixed_cnt = sum(1 for a in self.actions if a.action != "ok" and a.success)
        err_cnt   = len(self.errors) + sum(1 for a in self.actions if not a.success)
        return (
            f"[{ts}] 對帳完成 ({self.duration_ms:.0f}ms) — "
            f"一致 {ok_cnt} 筆，修復 {fixed_cnt} 筆，錯誤 {err_cnt} 筆"
        )


class Reconciler:
    """
    啟動對帳器。

    每次機器人重啟時呼叫 run()，確保 DB 狀態與 Binance 實際狀態一致，
    避免「幽靈掛單」或「遺漏成交」導致策略邏輯混亂。
    """

    def __init__(self, client, db) -> None:
        """
        client — python-binance Client 實例
        db     — TradeDB 實例
        """
        self._client = client
        self._db     = db

    # ── 主入口 ───────────────────────────────────────────────
    def run(self) -> ReconcileReport:
        """執行完整對帳流程，回傳對帳報告。"""
        report = ReconcileReport()
        log.info("[Reconciler] 開始對帳...")

        try:
            grids = self._db.get_all_grid_configs()
            for g in grids:
                if g.get("status") not in ("RUNNING", "STOPPED"):
                    continue
                self._reconcile_grid(g, report)
        except Exception as e:
            msg = f"[Reconciler] 對帳主流程失敗: {e}"
            log.error(msg, exc_info=True)
            report.errors.append(msg)

        report.finished_at = time.time()
        log.info(f"[Reconciler] {report.summary}")
        return report

    # ── 單一網格對帳 ─────────────────────────────────────────
    def _reconcile_grid(self, grid: Dict, report: ReconcileReport) -> None:
        gid    = grid["id"]
        symbol = grid["symbol"]

        # 取 DB 中所有 OPEN 格位
        db_levels = self._db.get_grid_levels(gid)
        db_open   = [lv for lv in db_levels if lv["status"] == "OPEN" and lv.get("order_id")]

        if not db_open:
            return  # 無需對帳

        # 取 Binance 即時掛單（以 orderId 為 key）
        try:
            live_orders_raw = self._client.get_open_orders(symbol=symbol)
            live_ids: Set[str] = {str(o["orderId"]) for o in live_orders_raw}
            live_map:  Dict[str, dict] = {str(o["orderId"]): o for o in live_orders_raw}
        except Exception as e:
            msg = f"[Reconciler] {symbol} 查詢即時掛單失敗: {e}"
            log.error(msg)
            report.errors.append(msg)
            return

        for lv in db_open:
            oid = str(lv["order_id"])
            self._check_level(gid, symbol, lv, oid, live_ids, live_map, report)

        # 反向：Binance 有但 DB 無記錄的掛單 → 視為遺漏，記錄警告
        db_oids = {str(lv["order_id"]) for lv in db_open}
        for oid, order in live_map.items():
            if oid not in db_oids:
                log.warning(
                    f"[Reconciler] {symbol} Binance 有掛單 {oid} "
                    f"但 DB 無對應格位 (可能為手動單，不做處理)"
                )

    # ── 單一格位檢查 ─────────────────────────────────────────
    def _check_level(
        self,
        gid:      int,
        symbol:   str,
        lv:       Dict,
        oid:      str,
        live_ids: Set[str],
        live_map: Dict[str, dict],
        report:   ReconcileReport,
    ) -> None:
        """
        情況 A：DB=OPEN，Binance 無此單 → 查成交歷史
        情況 B：DB=OPEN，Binance 有此單 → 一致
        """
        if oid in live_ids:
            # B：狀態一致
            report.actions.append(ReconcileAction(
                grid_id=gid, symbol=symbol, order_id=oid,
                action="ok", detail="DB=OPEN 且 Binance 有此掛單，一致"
            ))
            return

        # A：Binance 無此掛單，查成交歷史
        log.info(f"[Reconciler] {symbol} 訂單 {oid} 在 Binance 不存在，查詢成交歷史...")
        try:
            order_info = self._client.get_order(symbol=symbol, orderId=oid)
            status     = order_info.get("status", "UNKNOWN")
            exec_qty   = float(order_info.get("executedQty", 0))
            avg_price  = self._calc_avg_price(order_info)

            if status in ("FILLED", "PARTIALLY_FILLED") and exec_qty > 0:
                # 標記為已成交
                self._db.update_grid_level_filled(gid, oid, avg_price, exec_qty)
                detail = f"Binance status={status} qty={exec_qty:.6f} @ {avg_price:.4f} → DB 標記 FILLED"
                log.info(f"[Reconciler] {symbol} {detail}")
                report.actions.append(ReconcileAction(
                    grid_id=gid, symbol=symbol, order_id=oid,
                    action="mark_filled", detail=detail
                ))
            else:
                # 標記為已撤銷（CANCELED / EXPIRED / REJECTED）
                self._db.update_grid_level_status(gid, oid, "CANCELLED")
                detail = f"Binance status={status} → DB 標記 CANCELLED"
                log.info(f"[Reconciler] {symbol} {detail}")
                report.actions.append(ReconcileAction(
                    grid_id=gid, symbol=symbol, order_id=oid,
                    action="mark_cancelled", detail=detail
                ))

        except Exception as e:
            msg = f"[Reconciler] {symbol} 查詢訂單 {oid} 失敗: {e}"
            log.error(msg)
            report.errors.append(msg)
            report.actions.append(ReconcileAction(
                grid_id=gid, symbol=symbol, order_id=oid,
                action="mark_cancelled",
                detail=f"查詢失敗，保守標記 CANCELLED: {e}",
                success=False,
            ))

    # ── 工具 ─────────────────────────────────────────────────
    @staticmethod
    def _calc_avg_price(order_info: Dict) -> float:
        """從訂單資料計算實際成交均價（優先用 cummulativeQuoteQty / executedQty）。"""
        try:
            quote = float(order_info.get("cummulativeQuoteQty", 0))
            base  = float(order_info.get("executedQty",          0))
            if base > 0 and quote > 0:
                return quote / base
        except Exception:
            pass
        return float(order_info.get("price", 0))
