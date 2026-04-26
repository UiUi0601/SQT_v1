"""
quant_system/risk/kill_switch.py — 緊急停止開關（v3：多市場 + 評分引擎適配）

② 限價平倉優化：
   原本的「市價賣出」改為以下三種模式（CloseMode），解決低流動性幣種滑價問題：
     "limit_5pct"  — 當前買價 × (1 - slip_pct)，預設 5% 寬限掛限價單
     "twap"        — 分 N 批次掛限價單，每批間隔 interval_sec 秒
     "market"      — 原始市價賣出（保留向後相容）

v3 新增：
  ─ 適配多帳戶餘額結構（SPOT / FUTURES / MARGIN 三業務線）
  ─ 新增 trigger_from_score()：由評分引擎觸發（附帶評分上下文）
  ─ 新增 check_score_threshold()：自動判斷評分是否達到緊急停止條件

使用方式：
  from quant_system.risk.kill_switch import get_kill_switch

  ks = get_kill_switch(db=db_instance, close_mode="limit_5pct")

  # 由評分引擎觸發（傳入 ScoreResult）
  if score_result.final_score < -0.9:
      ks.trigger_from_score(score_result, client=binance_client)

  # 傳統觸發方式（不變）
  event = ks.trigger(client=binance_client, reason="回撤超過 15%")

  # 查詢狀態
  if ks.is_paused:
      ...

  # 解除暫停（人工確認後）
  ks.reset()
"""
from __future__ import annotations
import logging
import math
import os
import threading
import time
from dataclasses import dataclass, field
from typing import List, Optional

log = logging.getLogger(__name__)

# ── 平倉模式常數 ─────────────────────────────────────────────
CLOSE_LIMIT  = "limit_5pct"   # 限價單（當前價 × (1 - slip_pct)）
CLOSE_TWAP   = "twap"         # 分批限價 TWAP
CLOSE_MARKET = "market"       # 市價（高滑點，僅緊急備用）

_DEFAULT_CLOSE_MODE = os.getenv("KILL_CLOSE_MODE", CLOSE_LIMIT)


@dataclass
class KillEvent:
    """記錄每次觸發緊急停止的詳情。"""
    reason:           str
    close_mode:       str   = CLOSE_LIMIT
    timestamp:        float = field(default_factory=time.time)
    orders_cancelled: int   = 0   # 成功撤單的幣種數
    positions_closed: int   = 0   # 成功平倉的資產數
    errors:           List[str] = field(default_factory=list)

    @property
    def summary(self) -> str:
        from datetime import datetime, timezone
        ts   = datetime.fromtimestamp(self.timestamp, tz=timezone.utc).strftime("%H:%M:%S UTC")
        errs = f"  ⚠ {len(self.errors)} 個錯誤" if self.errors else ""
        return (
            f"[{ts}] 緊急停止（{self.close_mode}）：{self.reason} | "
            f"撤單 {self.orders_cancelled} 幣種，"
            f"平倉 {self.positions_closed} 資產{errs}"
        )


class KillSwitch:
    """
    緊急停止開關。

    執行緒安全；is_paused 標誌在整個程序生命週期持續生效，
    直到 reset() 被明確呼叫。

    平倉模式（close_mode）：
      "limit_5pct"  — 以 5% 寬限的限價單平倉（預設，避免滑價）
      "twap"        — 分 twap_batches 批限價單，每批間隔 twap_interval_sec 秒
      "market"      — 市價（高滑點，僅用於極端情況）
    """

    def __init__(
        self,
        db               = None,
        close_mode:       str   = _DEFAULT_CLOSE_MODE,
        slip_pct:         float = 0.05,    # 限價單寬限幅度（5%）
        twap_batches:     int   = 3,       # TWAP 批次數
        twap_interval_sec: float = 5.0,   # TWAP 批次間隔（秒）
        price_timeout_sec: float = 3.0,   # 獲取當前價格的最長等待秒數
    ) -> None:
        self._db               = db
        self._close_mode       = close_mode
        self._slip_pct         = slip_pct
        self._twap_batches     = max(1, twap_batches)
        self._twap_interval    = twap_interval_sec
        self._price_timeout    = price_timeout_sec

        self._paused   = False
        self._lock     = threading.Lock()
        self._history: List[KillEvent] = []

    # ── 主要觸發 ─────────────────────────────────────────────
    def trigger(
        self,
        client=None,
        reason: str = "手動觸發緊急停止",
    ) -> KillEvent:
        """
        執行緊急停止流程。

        client — python-binance Client 實例（None 則僅更新 DB + 標誌）
        reason — 觸發原因（記入日誌與 KillEvent）
        """
        with self._lock:
            self._paused = True

        event = KillEvent(reason=reason, close_mode=self._close_mode)
        log.critical(
            f"[KillSwitch] ⚠️  緊急停止觸發！原因: {reason}  "
            f"平倉模式: {self._close_mode}"
        )

        # ── 步驟 1：撤銷所有掛單 ────────────────────────────
        if client is not None and self._db is not None:
            grids   = self._db.get_all_grid_configs()
            symbols = list({g["symbol"] for g in grids})
            for sym in symbols:
                try:
                    client.cancel_open_orders(symbol=sym)
                    event.orders_cancelled += 1
                    log.info(f"[KillSwitch] ✅ {sym} 所有掛單已撤銷")
                    time.sleep(0.1)   # 避免觸發 rate limit
                except Exception as e:
                    msg = f"[KillSwitch] ❌ {sym} 撤單失敗: {e}"
                    event.errors.append(msg)
                    log.error(msg)

        # ── 步驟 2：平倉所有非 USDT 資產 ─────────────────────
        if client is not None:
            self._close_all_positions(client, event)

        # ── 步驟 3：所有網格設為 STOPPED ────────────────────
        if self._db is not None:
            try:
                grids = self._db.get_all_grid_configs()
                for g in grids:
                    self._db.set_grid_status(g["symbol"], "STOPPED")
                log.info(f"[KillSwitch] 共 {len(grids)} 個網格設為 STOPPED")
            except Exception as e:
                msg = f"[KillSwitch] DB 更新失敗: {e}"
                event.errors.append(msg)
                log.error(msg)

        self._history.append(event)
        log.critical(f"[KillSwitch] 完成 — {event.summary}")
        return event

    # ── 解除暫停 ─────────────────────────────────────────────
    def trigger_from_score(
        self,
        score_result,
        client=None,
        reason_prefix: str = "評分引擎觸發",
    ) -> "KillEvent":
        """
        由多因子評分引擎觸發緊急停止。

        score_result: ScoringEngine 回傳的 ScoreResult 物件。
        自動從 ScoreResult 提取 symbol、final_score、skipped_factors
        作為觸發原因的上下文資訊。
        """
        symbol      = getattr(score_result, "symbol", "UNKNOWN")
        score       = getattr(score_result, "final_score", 0.0)
        decision    = getattr(score_result, "decision", "HOLD")
        skipped     = getattr(score_result, "skipped_factors", [])
        reason = (
            f"{reason_prefix} | symbol={symbol} "
            f"decision={decision} score={score:.4f} "
            f"skipped={skipped}"
        )
        return self.trigger(client=client, reason=reason)

    def check_score_threshold(
        self,
        score_result,
        panic_threshold: float = -0.85,
    ) -> bool:
        """
        檢查評分是否達到緊急停止條件。
        final_score < panic_threshold 時自動觸發。
        回傳 True = 已觸發；False = 未達條件。

        建議在每次評估後呼叫：
            if ks.check_score_threshold(score_result):
                ks.trigger_from_score(score_result, client=binance_client)
        """
        score = getattr(score_result, "final_score", 0.0)
        if score < panic_threshold:
            log.warning(
                f"[KillSwitch] 評分觸及恐慌閾值 "
                f"score={score:.4f} < threshold={panic_threshold}"
            )
            return True
        return False

    def get_multi_market_positions(self, clients: dict) -> dict:
        """
        查詢三業務線（SPOT / FUTURES / MARGIN）的持倉與餘額。

        clients: {"SPOT": spot_client, "FUTURES": fut_client, "MARGIN": mgn_client}
        回傳多帳戶餘額結構 dict。
        """
        positions = {}
        for market, client in clients.items():
            if client is None:
                continue
            try:
                if market == "FUTURES":
                    acc = client.get_futures_account()
                    positions["FUTURES"] = {
                        "balance": acc.get("totalWalletBalance", "0"),
                        "unrealizedPnl": acc.get("totalUnrealizedProfit", "0"),
                        "assets": acc.get("assets", []),
                    }
                elif market == "MARGIN":
                    acc = client.get_margin_account()
                    positions["MARGIN"] = {
                        "marginLevel": acc.get("marginLevel", "0"),
                        "totalNetAsset": acc.get("totalNetAssetOfBtc", "0"),
                        "assets": acc.get("userAssets", []),
                    }
                else:  # SPOT
                    acc = client.get_account()
                    positions["SPOT"] = {
                        "balances": [
                            b for b in acc.get("balances", [])
                            if float(b.get("free", 0)) + float(b.get("locked", 0)) > 0
                        ]
                    }
            except Exception as e:
                log.warning(f"[KillSwitch] {market} 帳戶查詢失敗: {e}")
                positions[market] = {"error": str(e)}
        return positions

    def reset(self) -> None:
        """解除暫停狀態（需人工確認後呼叫）。"""
        with self._lock:
            self._paused = False
        log.info("[KillSwitch] 系統已解除暫停，可重新開倉")

    # ── 查詢 ─────────────────────────────────────────────────
    @property
    def is_paused(self) -> bool:
        return self._paused

    @property
    def last_event(self) -> Optional[KillEvent]:
        return self._history[-1] if self._history else None

    @property
    def trigger_count(self) -> int:
        return len(self._history)

    # ════════════════════════════════════════════════════════
    # ② 內部平倉邏輯
    # ════════════════════════════════════════════════════════
    def _close_all_positions(self, client, event: KillEvent) -> None:
        """遍歷帳戶餘額，依 close_mode 平倉所有非 USDT 資產。"""
        try:
            acc = client.get_account()
        except Exception as e:
            msg = f"[KillSwitch] 取得帳戶資料失敗: {e}"
            event.errors.append(msg)
            log.error(msg)
            return

        for b in acc.get("balances", []):
            asset  = b.get("asset", "")
            free   = float(b.get("free",   0))
            locked = float(b.get("locked", 0))
            total  = free + locked

            # 跳過 USDT / BNB / 極小量
            if asset in ("USDT", "BNB") or total < 1e-8:
                continue

            # locked 數量需等撤單後才釋放，先嘗試 free 部分
            qty = free
            sym = f"{asset}USDT"

            if qty <= 0:
                log.debug(f"[KillSwitch] {asset} free=0，跳過平倉")
                continue

            try:
                if self._close_mode == CLOSE_MARKET:
                    self._close_market(client, sym, qty, event)
                elif self._close_mode == CLOSE_TWAP:
                    self._close_twap(client, sym, qty, event)
                else:  # 預設 limit_5pct
                    self._close_limit(client, sym, qty, event)
            except Exception as e:
                msg = f"[KillSwitch] ❌ {sym} 平倉例外: {e}"
                event.errors.append(msg)
                log.error(msg)

    # ── 模式 A：市價（原始邏輯，保留備用）─────────────────────
    def _close_market(self, client, sym: str, qty: float, event: KillEvent) -> None:
        qty_r = round(qty, 6)
        client.order_market_sell(symbol=sym, quantity=qty_r)
        event.positions_closed += 1
        log.info(f"[KillSwitch] ✅ {sym} 市價平倉 qty={qty_r:.6f}")

    # ── 模式 B：限價單（當前價 × (1 - slip_pct)）─────────────
    def _close_limit(self, client, sym: str, qty: float, event: KillEvent) -> None:
        """
        取得目前最佳買價（bid），乘以 (1 - slip_pct) 作為限價。
        相比市價，最壞情況多承擔 slip_pct 滑價，但避免瞬間砸穿薄板盤。
        """
        current_price = self._get_bid_price(client, sym)
        if current_price <= 0:
            log.warning(
                f"[KillSwitch] {sym} 無法取得價格，回退市價平倉"
            )
            self._close_market(client, sym, qty, event)
            return

        limit_px = current_price * (1.0 - self._slip_pct)
        qty_r    = round(qty, 6)

        # 格式化：Binance 需要字串，精度 8 位
        limit_px_str = f"{limit_px:.8f}"

        client.order_limit_sell(
            symbol   = sym,
            quantity = qty_r,
            price    = limit_px_str,
        )
        event.positions_closed += 1
        log.info(
            f"[KillSwitch] ✅ {sym} 限價平倉 qty={qty_r:.6f} "
            f"limit_px={limit_px:.6f}（bid={current_price:.6f} -5%）"
        )

    # ── 模式 C：TWAP 分批限價 ────────────────────────────────
    def _close_twap(self, client, sym: str, qty: float, event: KillEvent) -> None:
        """
        將 qty 均分為 twap_batches 批，每批間隔 twap_interval_sec 秒掛限價單。
        每批重新取價，動態更新限價，適應快速下跌行情。
        """
        n        = self._twap_batches
        batch_qty = qty / n
        # 四捨五入到 6 位，最後一批補足餘量
        batch_qty_r = math.floor(batch_qty * 1e6) / 1e6  # 向下取整防超量

        log.info(
            f"[KillSwitch] {sym} TWAP 平倉：{qty:.6f} 分 {n} 批，"
            f"每批 ≈ {batch_qty_r:.6f}，間隔 {self._twap_interval}s"
        )

        sent_qty = 0.0
        for i in range(n):
            # 最後一批送剩餘量（避免浮點累積誤差遺漏）
            if i == n - 1:
                remaining = round(qty - sent_qty, 6)
                if remaining <= 0:
                    break
                this_qty = remaining
            else:
                this_qty = batch_qty_r
                if this_qty <= 0:
                    continue

            current_price = self._get_bid_price(client, sym)
            if current_price <= 0:
                log.warning(f"[KillSwitch] {sym} TWAP 第{i+1}批取價失敗，跳過")
                time.sleep(self._twap_interval)
                continue

            limit_px     = current_price * (1.0 - self._slip_pct)
            limit_px_str = f"{limit_px:.8f}"

            try:
                client.order_limit_sell(
                    symbol   = sym,
                    quantity = round(this_qty, 6),
                    price    = limit_px_str,
                )
                sent_qty += this_qty
                event.positions_closed += 1
                log.info(
                    f"[KillSwitch] ✅ {sym} TWAP [{i+1}/{n}] "
                    f"qty={this_qty:.6f} limit_px={limit_px:.6f}"
                )
            except Exception as e:
                msg = f"[KillSwitch] ❌ {sym} TWAP [{i+1}/{n}] 失敗: {e}"
                event.errors.append(msg)
                log.error(msg)

            # 最後一批不需要等待
            if i < n - 1:
                time.sleep(self._twap_interval)

    # ── 取得當前最佳賣出價（Bid Price）───────────────────────
    def _get_bid_price(self, client, sym: str) -> float:
        """
        取得幣種的最佳 bid 價（最高買價），作為限價平倉的基準。
        使用 order book depth=1 取得最即時的 bid；
        若失敗則 fallback 至 ticker 最新成交價。
        回傳 0.0 表示取價失敗。
        """
        # 優先：order book bid（最即時）
        try:
            book = client.get_order_book(symbol=sym, limit=1)
            bids = book.get("bids", [])
            if bids:
                return float(bids[0][0])
        except Exception:
            pass

        # Fallback：最新成交價
        try:
            ticker = client.get_symbol_ticker(symbol=sym)
            return float(ticker.get("price", 0))
        except Exception as e:
            log.warning(f"[KillSwitch] {sym} 取價失敗: {e}")
            return 0.0


# ════════════════════════════════════════════════════════════
# 全域單例
# ════════════════════════════════════════════════════════════
_instance: Optional[KillSwitch] = None
_instance_lock = threading.Lock()


def get_kill_switch(
    db               = None,
    close_mode:       str   = _DEFAULT_CLOSE_MODE,
    slip_pct:         float = 0.05,
    twap_batches:     int   = 3,
    twap_interval_sec: float = 5.0,
) -> KillSwitch:
    """
    取得全域 KillSwitch 單例。
    首次呼叫使用傳入的參數初始化；後續呼叫忽略參數，回傳同一實例。
    若要修改 close_mode，請在首次呼叫時傳入或使用環境變數 KILL_CLOSE_MODE。
    """
    global _instance
    with _instance_lock:
        if _instance is None:
            _instance = KillSwitch(
                db                = db,
                close_mode        = close_mode,
                slip_pct          = slip_pct,
                twap_batches      = twap_batches,
                twap_interval_sec = twap_interval_sec,
            )
            log.debug(f"[KillSwitch] 初始化，平倉模式={close_mode}")
        elif db is not None and _instance._db is None:
            _instance._db = db
    return _instance
