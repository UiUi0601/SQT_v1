"""
quant_system/risk/risk_manager.py — 風險管理器（強化版 v2）

本次優化項目：
  ① DB 效能瓶頸修復 — check_24h_drawdown 不再每次查詢 SQLite；
                       改用 collections.deque 的記憶體快取維護近期 PnL，
                       讀取複雜度 O(n_window)，窗口長度通常 < 200 筆。
                       啟動時可呼叫 hydrate() 從 DB 預熱快取，之後只需
                       呼叫 push_pnl() 逐筆推入即可。

  ③ 狀態 Hydration   — hydrate(db, client) 在機器人重啟時從：
                         DB : grid_trades（24h 內）→ 預熱 _pnl_cache
                         Binance: get_account()    → 還原 _exposure
                         DB : _paused_until 持久化（若有 bot_state 表）

  ④ 動態 VWAP 均價   — _Position 新增 total_cost / total_qty / avg_price，
                       每次成交呼叫 position.on_fill(exec_qty, exec_price)
                       自動更新均價，check_position() 以 avg_price 為停損基準。

使用範例：
  rm = RiskManager(RiskConfig(max_exposure_pct=0.20, max_24h_drawdown_pct=0.10))
  rm.hydrate(db=trade_db, client=binance_client)    # ③ 啟動還原

  # 有成交時推入 PnL（通常在 on_fill 裡呼叫）
  rm.push_pnl("BTCUSDT", net_pnl=12.5)              # ① 快取更新

  # 每格成交後更新均價
  pos = rm.get_position("BTCUSDT")
  if pos:
      pos.on_fill(exec_qty=0.01, exec_price=50000)  # ④ VWAP 更新

  # tick 時檢查 24h 回撤（O(n) 記憶體操作，無 DB I/O）
  ok, status = rm.check_24h_drawdown("BTCUSDT")

  # 下單前檢查曝險
  ok, reason, info = rm.check_exposure("BTCUSDT", proposed_usdt=500, total_equity=3000)
"""
from __future__ import annotations

import logging
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Deque, Dict, List, NamedTuple, Optional, Tuple

log = logging.getLogger(__name__)

# ① PnL 快取滑動窗口預設最大保留筆數（超過自動丟棄最舊）
_PNL_DEQUE_MAXLEN = 10_000


# ════════════════════════════════════════════════════════════
# 設定
# ════════════════════════════════════════════════════════════
@dataclass
class RiskConfig:
    # ── 傳統停損/止盈 ─────────────────────────────────────
    sl_pct:                 float = 0.02     # 停損百分比（以 avg_price 為基準）
    tp_pct:                 float = 0.04     # 止盈百分比
    trailing_stop_pct:      float = 0.015    # 移動停損
    max_risk_per_trade_pct: float = 0.01     # 單筆最大風險（佔總資金）
    max_open_positions:     int   = 5        # 最多同時持倉數
    min_cash_reserve_pct:   float = 0.10     # 最低現金保留比例
    max_drawdown_pct:       float = 0.20     # 最大回撤保護

    # ── 曝險上限 ──────────────────────────────────────────
    max_exposure_pct:       float = 0.30     # 單幣種曝險上限（佔總資金）

    # ── 24h 連續虧損暫停 ──────────────────────────────────
    max_24h_drawdown_pct:   float = 0.10     # 24h 最大允許回撤（10%）
    pause_duration_hours:   float = 4.0      # 暫停開倉時長（小時）

    # ① PnL 快取保留時長（秒），超過此時間的舊記錄不計入回撤
    pnl_window_sec:         float = 86_400.0  # 預設 24 小時


# ════════════════════════════════════════════════════════════
# ① PnL 快取記錄
# ════════════════════════════════════════════════════════════
class _PnLEntry(NamedTuple):
    ts:      float   # Unix timestamp
    net_pnl: float   # 本次成交淨損益


# ════════════════════════════════════════════════════════════
# ④ 動態均價持倉
# ════════════════════════════════════════════════════════════
@dataclass
class _Position:
    """
    記錄倉位資訊，並動態維護成交量加權平均價（VWAP）。

    每次網格分批成交後，呼叫 on_fill(exec_qty, exec_price) 更新：
      avg_price = total_cost / total_qty

    check_position() 以 avg_price 作為停損/止盈計算基準，
    比「只用第一筆 entry_price」更能反映真實持倉成本。
    """
    entry_price:  float        # 初始建倉價（首筆，供參考）
    quantity:     float        # 當前持倉總量
    strategy:     str
    peak_price:   float = 0.0  # 移動停損用：追蹤最高價

    # ④ VWAP 均價追蹤（post_init 自動初始化）
    total_cost:   float = field(init=False)
    total_qty:    float = field(init=False)
    avg_price:    float = field(init=False)

    def __post_init__(self) -> None:
        self.peak_price = self.entry_price
        self.total_cost = self.entry_price * self.quantity
        self.total_qty  = self.quantity
        self.avg_price  = self.entry_price

    def on_fill(self, exec_qty: float, exec_price: float) -> None:
        """
        ④ 成交後動態更新均價（VWAP）。

        呼叫時機：每次網格單成交（BUY 方向加倉）。
        SELL 方向平倉後應呼叫 remove_position()，不需呼叫此方法。

        Args:
            exec_qty   — 本次成交數量
            exec_price — 本次成交均價
        """
        if exec_qty <= 0 or exec_price <= 0:
            return
        self.total_cost += exec_qty * exec_price
        self.total_qty  += exec_qty
        self.quantity    = self.total_qty
        self.avg_price   = self.total_cost / self.total_qty
        if exec_price > self.peak_price:
            self.peak_price = exec_price
        log.debug(
            f"[Pos] on_fill qty={exec_qty:.6f} px={exec_price:.4f} "
            f"→ avg_price={self.avg_price:.4f} total_qty={self.total_qty:.6f}"
        )


# ════════════════════════════════════════════════════════════
# 快照資料類別
# ════════════════════════════════════════════════════════════
@dataclass
class ExposureInfo:
    """單幣種曝險快照。"""
    symbol:        str
    current_usdt:  float
    total_equity:  float
    max_usdt:      float
    pct_used:      float
    is_over:       bool


@dataclass
class DrawdownStatus:
    """24h 回撤狀態快照。"""
    symbol:           str
    drawdown_24h_pct: float           # 24h 回撤（負數 = 虧損，如 -8.5）
    threshold_pct:    float           # 設定上限（正數，如 10.0）
    is_paused:        bool
    paused_until:     Optional[float] # Unix timestamp；None = 未暫停
    reason:           str
    pnl_entries:      int = 0         # 快取中的 PnL 筆數（診斷用）


# ════════════════════════════════════════════════════════════
# 主類別
# ════════════════════════════════════════════════════════════
class RiskManager:
    def __init__(self, config: Optional[RiskConfig] = None) -> None:
        self.cfg   = config or RiskConfig()
        self._pos: Dict[str, _Position] = {}
        self._equity_peak:    float = 0.0
        self._current_equity: float = 0.0

        # 曝險追蹤：{symbol: locked_usdt}
        self._exposure: Dict[str, float] = {}

        # 24h 回撤暫停：{symbol: pause_until_ts}
        self._paused_until: Dict[str, float] = {}

        # ① PnL 記憶體快取：{symbol: deque[_PnLEntry]}
        # maxlen 限制記憶體上限；窗口外的舊資料由 check_24h_drawdown 動態過濾
        self._pnl_cache: Dict[str, Deque[_PnLEntry]] = {}

    # ════════════════════════════════════════════════════════
    # ③ 啟動狀態還原（Hydration）
    # ════════════════════════════════════════════════════════
    def hydrate(
        self,
        db     = None,
        client = None,
    ) -> None:
        """
        機器人重啟後呼叫此方法，從外部資料來源還原記憶體狀態。

        ③-A  db → 預熱 _pnl_cache（過去 24h 的 grid_trades 記錄）
        ③-B  client → 還原 _exposure（從 Binance 帳戶餘額估算各幣種曝險）
        ③-C  重算 24h 回撤，還原可能已觸發的 _paused_until

        Args:
            db     — TradeDB 實例（None 則跳過 DB 還原）
            client — python-binance Client（None 則跳過 Binance 還原）
        """
        log.info("[Risk] 🔄 開始狀態還原（Hydration）...")

        if db is not None:
            self._hydrate_pnl_cache(db)

        if client is not None:
            self._hydrate_exposure(client)

        # 重新計算各幣種回撤，補上可能的暫停狀態
        if db is not None:
            symbols = list(self._pnl_cache.keys())
            for sym in symbols:
                ok, status = self.check_24h_drawdown(sym)
                if not ok:
                    log.warning(
                        f"[Risk] Hydrate: {sym} 24h 回撤 {status.drawdown_24h_pct:.2f}% 超限，"
                        f"還原暫停至 {time.strftime('%H:%M:%S', time.localtime(status.paused_until))}"
                    )

        log.info(
            f"[Risk] ✅ 狀態還原完成：pnl_cache={len(self._pnl_cache)} 幣種，"
            f"exposure={len(self._exposure)} 幣種"
        )

    def _hydrate_pnl_cache(self, db) -> None:
        """③-A 從 DB grid_trades 讀取 24h 內記錄，預熱 PnL 快取。"""
        try:
            cutoff_iso = (datetime.now(timezone.utc) - timedelta(hours=25)).isoformat()
            # 取多 1 小時的資料以確保窗口完整
            df = db.get_grid_trades(limit=50_000)
            if df.empty or "net_pnl" not in df.columns:
                log.info("[Risk] DB PnL 快取：無歷史記錄")
                return

            if "closed_at" not in df.columns:
                log.warning("[Risk] grid_trades 缺少 closed_at 欄位，跳過 PnL 預熱")
                return

            df = df[df["closed_at"].astype(str) >= cutoff_iso].copy()
            df = df.sort_values("id")

            loaded = 0
            for _, row in df.iterrows():
                sym     = row.get("symbol", "")
                net_pnl = float(row.get("net_pnl", 0))
                closed  = row.get("closed_at", "")
                if not sym:
                    continue
                try:
                    ts = datetime.fromisoformat(str(closed)).timestamp()
                except Exception:
                    ts = time.time()
                self._push_pnl_raw(sym, net_pnl, ts)
                loaded += 1

            log.info(f"[Risk] DB PnL 快取預熱：共載入 {loaded} 筆")
        except Exception as e:
            log.warning(f"[Risk] PnL 快取預熱失敗（繼續運行）: {e}")

    def _hydrate_exposure(self, client) -> None:
        """
        ③-B 從 Binance get_account() 取得各幣種 free+locked 餘額，
        乘以當前價格估算 USDT 曝險。
        """
        try:
            acc = client.get_account()
        except Exception as e:
            log.warning(f"[Risk] Binance 帳戶查詢失敗，跳過曝險還原: {e}")
            return

        # 批量取價（避免逐筆 API 呼叫）
        prices: Dict[str, float] = {}
        try:
            all_tickers = client.get_all_tickers()   # [{symbol, price}, ...]
            prices = {t["symbol"]: float(t["price"]) for t in all_tickers}
        except Exception as e:
            log.warning(f"[Risk] 批量取價失敗: {e}")

        total_usdt = 0.0
        for b in acc.get("balances", []):
            asset  = b.get("asset", "")
            free   = float(b.get("free",   0))
            locked = float(b.get("locked", 0))
            total  = free + locked
            if total < 1e-8:
                continue
            if asset == "USDT":
                total_usdt += total
                continue
            sym   = f"{asset}USDT"
            price = prices.get(sym, 0.0)
            if price > 0:
                usdt_val = total * price
                self._exposure[sym] = usdt_val
                log.debug(f"[Risk] 還原曝險 {sym}: {usdt_val:.2f} USDT")

        log.info(
            f"[Risk] 曝險還原完成：{len(self._exposure)} 個幣種，"
            f"USDT 餘額 ≈ {total_usdt:.2f}"
        )

    # ════════════════════════════════════════════════════════
    # ① PnL 快取推入（公開）
    # ════════════════════════════════════════════════════════
    def push_pnl(self, symbol: str, net_pnl: float, ts: Optional[float] = None) -> None:
        """
        ① 每次網格成對成交後呼叫，將淨損益推入記憶體快取。

        此方法應在 record_grid_trade() 之後立即呼叫，
        以確保記憶體快取與 DB 保持同步。

        Args:
            symbol  — 交易對，如 "BTCUSDT"
            net_pnl — 本次淨損益（USDT）
            ts      — 成交時間戳（None = 當前時間）
        """
        self._push_pnl_raw(symbol, net_pnl, ts or time.time())

    def _push_pnl_raw(self, symbol: str, net_pnl: float, ts: float) -> None:
        if symbol not in self._pnl_cache:
            self._pnl_cache[symbol] = deque(maxlen=_PNL_DEQUE_MAXLEN)
        self._pnl_cache[symbol].append(_PnLEntry(ts=ts, net_pnl=net_pnl))

    # ════════════════════════════════════════════════════════
    # equity
    # ════════════════════════════════════════════════════════
    def update_equity(self, equity: float) -> None:
        self._current_equity = equity
        if equity > self._equity_peak:
            self._equity_peak = equity

    @property
    def open_count(self) -> int:
        return len(self._pos)

    # ════════════════════════════════════════════════════════
    # open guard
    # ════════════════════════════════════════════════════════
    def can_open_position(self, equity: float, open_count: int) -> Tuple[bool, str]:
        if open_count >= self.cfg.max_open_positions:
            return False, f"持倉數已達上限 {self.cfg.max_open_positions}"
        if self._equity_peak > 0:
            drawdown = (self._equity_peak - equity) / self._equity_peak
            if drawdown >= self.cfg.max_drawdown_pct:
                return False, f"最大回撤 {drawdown:.1%} ≥ 限制 {self.cfg.max_drawdown_pct:.1%}"
        return True, ""

    def available_cash_for_trade(self, equity: float, cash: float) -> float:
        reserve = equity * self.cfg.min_cash_reserve_pct
        return max(cash - reserve, 0.0)

    # ════════════════════════════════════════════════════════
    # positions（④ 使用 _Position.avg_price 為停損基準）
    # ════════════════════════════════════════════════════════
    def register_position(
        self,
        symbol:      str,
        entry_price: float,
        quantity:    float,
        strategy:    str = "",
    ) -> _Position:
        pos = _Position(entry_price, quantity, strategy)
        self._pos[symbol] = pos
        log.debug(
            f"[Risk] 新倉位: {symbol} @ {entry_price:.4f} "
            f"qty={quantity:.6f} avg_price={pos.avg_price:.4f}"
        )
        return pos

    def remove_position(self, symbol: str) -> None:
        self._pos.pop(symbol, None)

    def get_position(self, symbol: str) -> Optional[_Position]:
        return self._pos.get(symbol)

    def check_position(self, symbol: str, current_price: float) -> str:
        """
        ④ 以 avg_price（VWAP 動態均價）為基準計算停損/止盈/移動停損。
        回傳 'SELL' 或 'HOLD'。
        """
        p = self._pos.get(symbol)
        if not p:
            return "HOLD"

        if current_price > p.peak_price:
            p.peak_price = current_price

        # ④ 停損/止盈以 avg_price（非 entry_price）為基準
        sl_trigger    = p.avg_price * (1 - self.cfg.sl_pct)
        tp_trigger    = p.avg_price * (1 + self.cfg.tp_pct)
        trail_trigger = p.peak_price * (1 - self.cfg.trailing_stop_pct)

        if current_price <= sl_trigger:
            log.info(
                f"[Risk] {symbol} 停損 px={current_price:.4f} "
                f"sl={sl_trigger:.4f}（avg={p.avg_price:.4f}）"
            )
            return "SELL"

        if current_price >= tp_trigger:
            log.info(
                f"[Risk] {symbol} 止盈 px={current_price:.4f} "
                f"tp={tp_trigger:.4f}（avg={p.avg_price:.4f}）"
            )
            return "SELL"

        if current_price <= trail_trigger:
            log.info(
                f"[Risk] {symbol} 移動停損 px={current_price:.4f} "
                f"trail={trail_trigger:.4f}（peak={p.peak_price:.4f}）"
            )
            return "SELL"

        return "HOLD"

    # ════════════════════════════════════════════════════════
    # 曝險上限
    # ════════════════════════════════════════════════════════
    def update_exposure(self, symbol: str, locked_usdt: float) -> None:
        self._exposure[symbol] = max(locked_usdt, 0.0)

    def check_exposure(
        self,
        symbol:        str,
        proposed_usdt: float,
        total_equity:  float,
        max_pct:       Optional[float] = None,
    ) -> Tuple[bool, str, ExposureInfo]:
        pct      = max_pct if max_pct is not None else self.cfg.max_exposure_pct
        current  = self._exposure.get(symbol, 0.0)
        max_usdt = total_equity * pct
        after    = current + proposed_usdt
        pct_used = (after / total_equity * 100) if total_equity > 0 else 0.0
        is_over  = after > max_usdt

        info = ExposureInfo(
            symbol       = symbol,
            current_usdt = current,
            total_equity = total_equity,
            max_usdt     = max_usdt,
            pct_used     = round(pct_used, 2),
            is_over      = is_over,
        )

        if is_over:
            reason = (
                f"[Risk] {symbol} 曝險超限：加入 {proposed_usdt:.2f} USDT 後 "
                f"共 {after:.2f} ({pct_used:.1f}%) > 上限 {max_usdt:.2f} ({pct*100:.0f}%)"
            )
            log.warning(reason)
            return False, reason, info

        return True, "", info

    def get_exposure_info(self, symbol: str, total_equity: float) -> ExposureInfo:
        _, _, info = self.check_exposure(symbol, 0.0, total_equity)
        return info

    # ════════════════════════════════════════════════════════
    # ① 24h 回撤監控（記憶體 deque，無 DB I/O）
    # ════════════════════════════════════════════════════════
    def check_24h_drawdown(
        self,
        symbol:           str,
        max_drawdown_pct: Optional[float] = None,
        pause_hours:      Optional[float] = None,
    ) -> Tuple[bool, DrawdownStatus]:
        """
        ① 純記憶體操作：從 _pnl_cache[symbol] 的 deque 中，
           過濾最近 pnl_window_sec（預設 24h）的記錄，計算峰谷回撤。

        呼叫者需在每次成交後呼叫 push_pnl() 以保持快取最新。
        機器人啟動時呼叫 hydrate(db=...) 可預熱快取。

        回傳 (允許開倉, DrawdownStatus)。
        """
        threshold = max_drawdown_pct if max_drawdown_pct is not None else self.cfg.max_24h_drawdown_pct
        p_hours   = pause_hours      if pause_hours      is not None else self.cfg.pause_duration_hours
        window    = self.cfg.pnl_window_sec
        now       = time.time()

        # ── 暫停期內直接拒絕 ─────────────────────────────
        paused_until = self._paused_until.get(symbol, 0.0)
        if paused_until > now:
            remaining = (paused_until - now) / 3600
            return False, DrawdownStatus(
                symbol           = symbol,
                drawdown_24h_pct = 0.0,
                threshold_pct    = threshold * 100,
                is_paused        = True,
                paused_until     = paused_until,
                reason           = f"暫停中，還剩 {remaining:.1f} 小時解禁",
                pnl_entries      = 0,
            )

        # ① 從 deque 過濾窗口內的記錄（O(n_window)）
        cache  = self._pnl_cache.get(symbol)
        cutoff = now - window

        if not cache:
            return True, DrawdownStatus(
                symbol=symbol, drawdown_24h_pct=0.0,
                threshold_pct=threshold * 100,
                is_paused=False, paused_until=None,
                reason="無 PnL 快取記錄（尚未成交或未 hydrate）",
                pnl_entries=0,
            )

        # 僅保留時間窗口內的記錄（deque 按時間升序）
        entries = [e for e in cache if e.ts >= cutoff]

        if not entries:
            return True, DrawdownStatus(
                symbol=symbol, drawdown_24h_pct=0.0,
                threshold_pct=threshold * 100,
                is_paused=False, paused_until=None,
                reason=f"{window/3600:.0f}h 窗口內無成交記錄",
                pnl_entries=0,
            )

        # ① 計算累積 PnL 的峰谷回撤（O(n_window) = 通常 < 200 次迴圈）
        cum        = 0.0
        peak       = 0.0
        max_dd_abs = 0.0   # 以絕對 USDT 計的最大回撤

        for e in entries:
            cum += e.net_pnl
            if cum > peak:
                peak = cum
            dd = peak - cum            # 當前回撤金額（≥ 0）
            if dd > max_dd_abs:
                max_dd_abs = dd

        # 以「峰值的百分比」表示回撤
        peak_ref = abs(peak) + 1e-9
        dd_pct   = max_dd_abs / peak_ref * 100 if peak > 0 else 0.0

        if dd_pct >= threshold * 100:
            pause_until_ts = now + p_hours * 3600
            self._paused_until[symbol] = pause_until_ts
            reason = (
                f"24h 回撤 {dd_pct:.2f}% ≥ 上限 {threshold*100:.0f}%，"
                f"自動暫停 {p_hours:.0f} 小時"
            )
            log.warning(f"[Risk] {symbol} {reason}")
            return False, DrawdownStatus(
                symbol           = symbol,
                drawdown_24h_pct = -dd_pct,
                threshold_pct    = threshold * 100,
                is_paused        = True,
                paused_until     = pause_until_ts,
                reason           = reason,
                pnl_entries      = len(entries),
            )

        return True, DrawdownStatus(
            symbol           = symbol,
            drawdown_24h_pct = -dd_pct,
            threshold_pct    = threshold * 100,
            is_paused        = False,
            paused_until     = None,
            reason           = "正常",
            pnl_entries      = len(entries),
        )

    def is_symbol_paused(self, symbol: str) -> bool:
        return self._paused_until.get(symbol, 0.0) > time.time()

    def resume_symbol(self, symbol: str) -> None:
        self._paused_until.pop(symbol, None)
        log.info(f"[Risk] {symbol} 24h 暫停已手動解除")

    def get_all_drawdown_status(self, symbols: List[str]) -> List[DrawdownStatus]:
        """批次取得多個幣種的 24h 回撤狀態（用於 UI 顯示，無 DB I/O）。"""
        return [self.check_24h_drawdown(sym)[1] for sym in symbols]

    # ─── 舊介面相容（db 參數保留但忽略）──────────────────────
    def check_24h_drawdown_db(
        self,
        db,
        symbol:           str,
        max_drawdown_pct: Optional[float] = None,
        pause_hours:      Optional[float] = None,
    ) -> Tuple[bool, DrawdownStatus]:
        """
        向後相容方法：若呼叫端仍傳入 db 參數，忽略之並改用記憶體快取。
        若快取為空，先嘗試從 db 預熱，再計算。
        """
        if symbol not in self._pnl_cache:
            try:
                self._hydrate_pnl_cache(db)
            except Exception as e:
                log.debug(f"[Risk] 相容方法預熱失敗: {e}")
        return self.check_24h_drawdown(symbol, max_drawdown_pct, pause_hours)
