"""
crypto_main_grid.py — AQT_v1 動態網格機器人主迴圈（強化版）

強化項目：
  ① 部分成交：透過 PartialFillTracker 正確追蹤 PARTIALLY_FILLED
  ② 精度處理：PrecisionManager floor 截斷 + 全項驗證
  ③ 動態滑價：SlippageModel（基礎 + 衝擊 + 波動率）
  ④ WebSocket 心跳：BinanceUserStream（主要）+ REST 輪詢（備援）
  ⑤ Threading Lock：GridManager 內建，防競態重複下單
  ⑥ 手續費扣除：FeeCalculator 計算安全賣出量
  ⑦ Kill Switch：緊急停止（撤單 + 平倉 + 暫停）
  ⑧ Rate Limiter：Token Bucket 防 API 超限
  ⑨ 曝險上限：單幣種不超過帳戶 N%
  ⑩ 24h 回撤暫停：超限自動停止開倉

架構：
  - WebSocket 模式（預設）：BinanceUserStream 接收即時成交事件
  - REST 輪詢模式（備援）：WebSocket 不可用時退化至 30 秒輪詢
  - 兩種模式都透過同一個 GridManager 處理，無需更改邏輯
"""
from __future__ import annotations
import os, sys, time, logging, signal, threading
from typing import Dict, Optional
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
load_dotenv()

from quant_system.grid      import GridEngine, GridManager, TrendFilter
from quant_system.grid.grid_engine import GridLevel, GridSnapshot
from quant_system.market    import DataCache
from quant_system.execution import (
    CostModel, SlippageModel, PrecisionManager, FeeCalculator
)
from quant_system.exchange  import BinanceUserStream, Reconciler
from quant_system.exchange.websocket_client import StreamMarket
from quant_system.risk.risk_manager    import RiskManager, RiskConfig
from quant_system.risk.kill_switch     import get_kill_switch
from quant_system.risk.circuit_breaker import get_circuit_breaker
from quant_system.utils.logger      import setup_logger
from quant_system.utils.heartbeat   import HeartbeatMonitor
from quant_system.utils.alert       import GridAlerts
from crypto_exchange        import ExchangeClient
from database               import TradeDB

DEBUG    = os.getenv("DEBUG_MODE",           "false").lower() == "true"
INTERVAL = int(os.getenv("GRID_INTERVAL_SECONDS", "30"))
USE_WS   = os.getenv("USE_WEBSOCKET",        "true").lower() == "true"
MAX_RETRY = 5

# ── 風險設定（從環境變數讀取）────────────────────────────────
RISK_CFG = RiskConfig(
    max_exposure_pct     = float(os.getenv("MAX_EXPOSURE_PCT",      "0.30")),
    max_24h_drawdown_pct = float(os.getenv("MAX_24H_DRAWDOWN_PCT",  "0.10")),
    pause_duration_hours = float(os.getenv("PAUSE_DURATION_HOURS",  "4.0")),
)

logger = setup_logger("GridBot", "logs/grid_bot.log", debug_mode=DEBUG)
db     = TradeDB()
alerts = GridAlerts()   # Telegram 推播（未設 TOKEN 時靜默）

# ── 全域風險控管器 ────────────────────────────────────────────
risk_mgr       = RiskManager(RISK_CFG)
kill_switch    = get_kill_switch(db=db)
circuit_breaker = get_circuit_breaker(
    error_threshold = int(os.getenv("CB_ERROR_THRESHOLD", "10")),
    window_sec      = float(os.getenv("CB_WINDOW_SEC",     "60")),
    cooldown_sec    = float(os.getenv("CB_COOLDOWN_SEC",   "600")),
)

# ════════════════════════════════════════════════════════════
# ③ 優雅關閉（Graceful Shutdown）
# ════════════════════════════════════════════════════════════
_is_running:  bool             = True      # SIGTERM/SIGINT 設為 False 讓主迴圈退出
_shutdown_ev: threading.Event  = threading.Event()


def _handle_shutdown(signum: int, frame) -> None:
    """
    捕捉 SIGTERM（systemd stop / docker stop）與 SIGINT（Ctrl+C）。
    僅設旗標，不做任何阻塞操作；主迴圈跑完當前 tick 後自行退出。
    """
    global _is_running
    sig_name = signal.Signals(signum).name
    logger.warning(f"[Shutdown] 收到 {sig_name}，等待本輪 tick 完成後退出...")
    _is_running = False
    _shutdown_ev.set()


# 在模組載入時即註冊（確保 run_bot.py 啟動後立刻生效）
signal.signal(signal.SIGTERM, _handle_shutdown)
signal.signal(signal.SIGINT,  _handle_shutdown)

# ── 每日清理排程追蹤 ─────────────────────────────────────────
_last_cleanup_day: int = -1   # 記錄最後執行清理的日期（避免重複觸發）


def _maybe_run_cleanup() -> None:
    """每天只執行一次 DB 清理（以日期判斷）。"""
    global _last_cleanup_day
    today = time.localtime().tm_yday   # 一年中的第幾天
    if today != _last_cleanup_day:
        _last_cleanup_day = today
        try:
            results = db.cleanup_old_records(days=30)
            total   = sum(results.values())
            if total > 0:
                logger.info(f"[Cleanup] 每日自動清理完成，共刪除 {total} 筆舊紀錄")
        except Exception as e:
            logger.warning(f"[Cleanup] 自動清理失敗: {e}")

MTF = {
    "1h": {"period": "30d",  "interval": "1h"},
    "1d": {"period": "365d", "interval": "1d"},
}


# ════════════════════════════════════════════════════════════
# 半自動模式工具：從固定上下限建立 GridSnapshot
# ════════════════════════════════════════════════════════════
def _build_semi_snap(
    upper: float,
    lower: float,
    grid_count: int,
    current_price: float,
    fee_rate: float = 0.001,
) -> Optional[GridSnapshot]:
    """
    根據使用者手動設定的上下限與格數，建立固定的 GridSnapshot。

    - 不依賴 ATR / EMA，格距固定為 (upper - lower) / grid_count
    - side 依當前價格決定：price < current_price → BUY；price >= current_price → SELL
    - fee_ok = False 表示格距過小（呼叫方自行決定是否拒絕）
    """
    if upper <= lower or grid_count < 2 or current_price <= 0:
        logger.warning(
            f"[SemiGrid] 參數無效: upper={upper} lower={lower} "
            f"gc={grid_count} price={current_price}"
        )
        return None

    spacing = (upper - lower) / grid_count
    center  = (upper + lower) / 2.0

    # 手續費保護：格距需 > fee_rate × 2（來回費用）
    min_spacing = current_price * fee_rate * 2
    fee_ok = spacing >= min_spacing
    if not fee_ok:
        logger.warning(
            f"[SemiGrid] 格距 {spacing:.6f} < 手續費下限 {min_spacing:.6f}，"
            f"網格將無法盈利（fee_rate={fee_rate:.3%}）"
        )

    levels: list = []
    for i in range(grid_count + 1):
        price = lower + i * spacing
        side  = "BUY" if price < current_price else "SELL"
        levels.append(GridLevel(index=i, price=round(price, 8), side=side))

    return GridSnapshot(
        center         = round(center, 8),
        upper          = round(upper, 8),
        lower          = round(lower, 8),
        spacing        = round(spacing, 8),
        atr            = 0.0,          # 半自動模式無 ATR
        levels         = levels,
        grid_count     = len(levels),
        spacing_source = "semi_manual",
        fee_ok         = fee_ok,
    )


class GridBotInstance:
    """單一幣種的動態網格機器人實例（強化版）。"""

    def __init__(self, cfg: Dict, client, ws_stream: Optional[BinanceUserStream]) -> None:
        self.symbol    = cfg["symbol"]
        self.yf_symbol = cfg.get("yf_symbol", self.symbol.replace("USDT", "-USD"))
        self.grid_id   = cfg["id"]
        self.cfg       = cfg
        self._ws       = ws_stream

        # ── 交易模式（雙模式核心）──────────────────────────────
        # 'auto'：全自動，GridEngine 根據 ATR/EMA 動態計算上下限
        # 'semi'：半自動，使用 UI 手動設定的 upper_price / lower_price
        self.trade_mode: str = cfg.get("trade_mode", "auto").strip().lower()

        # ── 精度管理器（共用）───────────────────────────────
        self._prec = PrecisionManager()

        # ── 手續費計算器 ────────────────────────────────────
        self._fee_calc = FeeCalculator(
            taker_fee = 0.001,
            maker_fee = 0.001,
            use_bnb   = os.getenv("USE_BNB_FEE", "false").lower() == "true",
        )

        # ── 動態滑價模型 ────────────────────────────────────
        self._slip_model = SlippageModel(
            base_pct      = 0.0005,
            impact_factor = 0.10,
            vol_factor    = 2.0,
            add_noise     = True,
        )
        self._cost_model = CostModel(
            use_dynamic_slippage = True,
            slippage_model       = self._slip_model,
        )

        # ── 網格引擎 ────────────────────────────────────────
        self.engine  = GridEngine(
            k          = float(cfg.get("k",          0.5)),
            m          = float(cfg.get("m",          3.0)),
            atr_period = int(cfg.get("atr_period",   14)),
            ema_period = int(cfg.get("ema_period",   20)),
        )
        self.filter  = TrendFilter()
        self.manager = GridManager(
            client       = client,
            symbol       = self.symbol,
            qty_per_grid = float(cfg.get("qty_per_grid", 0.001)),
            precision    = self._prec,
        )
        self.manager.setup()
        self.cache   = DataCache(ttl_seconds=60)
        self.snap    = None
        self._deployed = False

    # ── 每輪執行 ─────────────────────────────────────────────
    def tick(self) -> None:
        # ⓪-A 全域緊急停止檢查
        if kill_switch.is_paused:
            logger.warning(f"[{self.symbol}] Kill Switch 觸發中，跳過所有操作")
            return

        # ⓪-B API 斷路器檢查（⑤ 熔斷期間拒絕任何 API 呼叫）
        cb_ok, cb_reason = circuit_breaker.check()
        if not cb_ok:
            logger.warning(f"[{self.symbol}] {cb_reason}")
            return

        # ① 取資料
        dfs  = self.cache.get_multi_timeframe(self.yf_symbol, MTF)
        df1h = dfs.get("1h")
        df1d = dfs.get("1d")

        if df1h is None or df1h.empty:
            logger.warning(f"[{self.symbol}] 1h 資料缺失，跳過"); return

        current_price = float(df1h["Close"].iloc[-1])

        # ② 趨勢過濾
        fil = self.filter.check(
            df1d if (df1d is not None and len(df1d) > 200) else df1h
        )
        if not fil.allow_grid:
            logger.warning(f"[{self.symbol}] 網格暫停: {fil.reason}")
            if self._deployed:
                self.manager.cancel_all()
                self._deployed = False
            return

        # ③ 處理成交事件
        if self._deployed and self.snap:
            fills = self._collect_fills()
            for fill_event in fills:
                # 記錄 DB
                db.update_grid_level_filled(
                    self.grid_id, fill_event.order_id,
                    fill_event.avg_price, fill_event.executed_qty,
                )

                # ⑥ 手續費扣除：計算安全賣出量
                if fill_event.side == "BUY":
                    fee_res = self._fee_calc.calc_buy(
                        fill_event.executed_qty, fill_event.avg_price
                    )
                    safe_qty = fee_res.safe_sell_qty
                    logger.info(
                        f"[{self.symbol}] BUY 成交 qty={fill_event.executed_qty:.6f} "
                        f"→ 到帳 {fee_res.net_qty:.6f} 安全賣量 {safe_qty:.6f}"
                    )
                    # 將安全賣出量更新回 fill_event（供 on_fill 使用）
                    fill_event.executed_qty = safe_qty

                # 反向掛單（含 Lock 防重複）
                new_oid = self.manager.on_fill(
                    fill_event, self.snap,
                    allow_buy=fil.allow_buy, allow_sell=fil.allow_sell,
                )

                # 記錄成對交易 PnL（SELL 成交時）+ 推入 risk_mgr PnL 快取
                net_pnl: Optional[float] = None
                if fill_event.side == "SELL":
                    self._record_grid_trade(fill_event)
                    # 計算 net_pnl 供 Telegram 推播
                    try:
                        if self.snap:
                            spacing = self.snap.spacing
                            pnl_raw = self._fee_calc.calc_round_trip_pnl(
                                buy_qty    = fill_event.executed_qty,
                                buy_price  = fill_event.avg_price - spacing,
                                sell_price = fill_event.avg_price,
                            )
                            net_pnl = pnl_raw.get("net_pnl")
                    except Exception:
                        pass

                # Telegram：成交通知（含反向掛單價格）
                reverse_price: Optional[float] = None
                if new_oid and self.snap:
                    # 嘗試從 snap levels 找到反向掛單的格位價格
                    opp_side = "SELL" if fill_event.side == "BUY" else "BUY"
                    for lv in (self.snap.levels or []):
                        if lv.order_id == new_oid:
                            reverse_price = lv.price
                            break
                alerts.notify_fill(
                    symbol        = self.symbol,
                    side          = fill_event.side,
                    avg_price     = fill_event.avg_price,
                    qty           = fill_event.executed_qty,
                    net_pnl       = net_pnl,
                    reverse_price = reverse_price,
                )

                logger.info(
                    f"[{self.symbol}] {fill_event.side} 成交 "
                    f"avg_px={fill_event.avg_price:.4f} "
                    f"qty={fill_event.executed_qty:.6f} "
                    f"complete={fill_event.is_complete} → 反向={new_oid}"
                )

        # ══════════════════════════════════════════════════════
        # ④ 模式分流：SEMI（半自動固定區間）vs AUTO（全自動ATR）
        # ══════════════════════════════════════════════════════
        if self.trade_mode == "semi":
            self._tick_semi(current_price, fil)
        else:
            self._tick_auto(current_price, df1h, fil)

    # ════════════════════════════════════════════════════════
    # 半自動模式 Tick：固定上下限，不自動移動網格
    # ════════════════════════════════════════════════════════
    def _tick_semi(self, current_price: float, fil) -> None:
        upper = float(self.cfg.get("upper_price", 0))
        lower = float(self.cfg.get("lower_price", 0))
        gc    = int(self.cfg.get("grid_count", 10))
        sl    = float(self.cfg.get("stop_loss_price", 0))

        # ── 止損觸發檢查 ────────────────────────────────────
        if sl > 0 and current_price <= sl:
            logger.warning(
                f"[{self.symbol}] SEMI 止損觸發！"
                f"price={current_price:.4f} <= SL={sl:.4f}，"
                f"撤銷所有掛單並暫停"
            )
            self.manager.cancel_all()
            self._deployed = False
            db.set_grid_status(self.symbol, "STOPPED")
            alerts.notify_stop_loss(
                symbol        = self.symbol,
                current_price = current_price,
                stop_price    = sl,
            )
            return

        # ── 價格是否在固定區間內 ─────────────────────────────
        if not (lower < current_price < upper):
            logger.info(
                f"[{self.symbol}] SEMI 等待："
                f"price={current_price:.4f} 不在固定區間 [{lower:.4f}, {upper:.4f}]"
            )
            # 不移動網格，等待價格回到區間內
            return

        # ── 首次部署（或重啟後恢復）─────────────────────────
        if self._deployed:
            return  # 固定區間模式：已部署則無需 regrid

        # ④.a 24h 回撤暫停檢查
        dd_ok, dd_status = risk_mgr.check_24h_drawdown(self.symbol)
        if not dd_ok:
            logger.warning(f"[{self.symbol}] SEMI 24h 回撤暫停: {dd_status.reason}")
            return

        # ⑤ 建立固定網格快照
        new_snap = _build_semi_snap(upper, lower, gc, current_price,
                                    fee_rate=self._fee_calc.taker_fee)
        if not new_snap:
            return
        if not new_snap.fee_ok:
            logger.warning(
                f"[{self.symbol}] SEMI 格距 {new_snap.spacing:.6f} 低於手續費下限，"
                f"建議增大格距或減少格數"
            )
            # 仍繼續部署（由使用者自行承擔，與 UI 行為一致）

        # 曝險上限檢查
        n_buy = sum(1 for lv in new_snap.levels if lv.side == "BUY")
        proposed_usdt = n_buy * self.cfg.get("qty_per_grid", 0.001) * current_price
        total_equity  = self.cfg.get("capital", 1000.0)
        exp_ok, exp_reason, _ = risk_mgr.check_exposure(
            self.symbol, proposed_usdt, total_equity
        )
        if not exp_ok:
            logger.warning(f"[{self.symbol}] SEMI {exp_reason}")
            return
        risk_mgr.update_exposure(self.symbol, proposed_usdt)

        logger.info(
            f"[{self.symbol}] SEMI 鋪設固定網格 "
            f"upper={upper:.4f} lower={lower:.4f} "
            f"spacing={new_snap.spacing:.6f} levels={new_snap.grid_count} "
            f"[buy:{fil.allow_buy} sell:{fil.allow_sell}]"
        )
        placed = self.manager.deploy(
            new_snap, current_price,
            allow_buy=fil.allow_buy, allow_sell=fil.allow_sell,
        )
        db.save_grid_levels(self.grid_id, self.symbol, placed)
        db.set_grid_last_regrid(self.symbol)
        self.snap      = new_snap
        self._deployed = True

        # Telegram：SEMI 網格部署通知
        alerts.notify_grid_deployed(
            symbol     = self.symbol,
            mode       = "semi",
            upper      = upper,
            lower      = lower,
            spacing    = new_snap.spacing,
            grid_count = gc,
            qty        = float(self.cfg.get("qty_per_grid", 0.001)),
        )

    # ════════════════════════════════════════════════════════
    # 全自動模式 Tick：ATR/EMA 動態計算（原始邏輯，完整保留）
    # ════════════════════════════════════════════════════════
    def _tick_auto(self, current_price: float, df1h, fil) -> None:
        # ④ 判斷是否重新鋪設
        need_regrid = (not self._deployed) or self.engine.should_regrid(df1h, threshold=0.05)
        if not need_regrid:
            return

        # ④.a 24h 回撤暫停檢查（純記憶體操作，無 DB I/O）
        dd_ok, dd_status = risk_mgr.check_24h_drawdown(self.symbol)
        if not dd_ok:
            logger.warning(f"[{self.symbol}] 24h 回撤暫停: {dd_status.reason}")
            if self._deployed:
                self.manager.cancel_all()
                self._deployed = False
            return

        # ⑤ 計算新網格並部署
        new_snap = self.engine.compute(df1h)
        if not new_snap:
            return

        # 驗證格距是否大於損益平衡所需最小格距
        min_spacing = self._fee_calc.min_profitable_spacing(current_price)
        if new_snap.spacing < min_spacing:
            logger.warning(
                f"[{self.symbol}] 格距 {new_snap.spacing:.4f} < "
                f"最小獲利格距 {min_spacing:.4f}，跳過（手續費吃掉利潤）"
            )
            return

        # 曝險上限檢查：估算本次部署所需 USDT
        n_buy_levels  = sum(1 for lv in (new_snap.levels if hasattr(new_snap, "levels") else [])
                            if getattr(lv, "side", "") == "BUY")
        proposed_usdt = n_buy_levels * self.cfg.get("qty_per_grid", 0.001) * current_price
        total_equity  = self.cfg.get("capital", 1000.0)

        exp_ok, exp_reason, _ = risk_mgr.check_exposure(
            self.symbol, proposed_usdt, total_equity
        )
        if not exp_ok:
            logger.warning(f"[{self.symbol}] {exp_reason}")
            return
        risk_mgr.update_exposure(self.symbol, proposed_usdt)

        logger.info(
            f"[{self.symbol}] AUTO 重新鋪設 "
            f"center={new_snap.center:.4f} spacing={new_snap.spacing:.4f} "
            f"levels={new_snap.grid_count} "
            f"[buy:{fil.allow_buy} sell:{fil.allow_sell}]"
        )

        placed = self.manager.deploy(
            new_snap, current_price,
            allow_buy=fil.allow_buy, allow_sell=fil.allow_sell,
        )
        db.save_grid_levels(self.grid_id, self.symbol, placed)
        db.set_grid_last_regrid(self.symbol)
        self.snap      = new_snap
        self._deployed = True

        # Telegram：AUTO 網格部署通知
        alerts.notify_grid_deployed(
            symbol     = self.symbol,
            mode       = "auto",
            upper      = new_snap.upper,
            lower      = new_snap.lower,
            spacing    = new_snap.spacing,
            grid_count = new_snap.grid_count,
            qty        = float(self.cfg.get("qty_per_grid", 0.001)),
        )

    # ── 收集成交（WebSocket 優先 + REST 備援）────────────────
    def _collect_fills(self):
        fills = []

        # WebSocket 模式
        if self._ws and self._ws.is_connected:
            events = self._ws.get_all_events()
            for ev in events:
                if ev.symbol != self.symbol:
                    continue
                result = self.manager.on_ws_event(ev)
                if result:
                    fills.append(result)
        else:
            # REST 輪詢備援模式
            fills = self.manager.poll_fills()

        return fills

    # ── 記錄成對 PnL ─────────────────────────────────────────
    def _record_grid_trade(self, sell_event) -> None:
        """SELL 成交後，嘗試從 DB 找配對的 BUY 價格記錄 PnL。"""
        try:
            levels = db.get_grid_levels(self.grid_id)
            # 找最近一個 FILLED 的 BUY 格位
            buy_levels = [
                lv for lv in levels
                if lv["side"] == "BUY" and lv["status"] == "FILLED"
            ]
            if not buy_levels:
                return
            buy_lv   = buy_levels[-1]
            buy_px   = float(buy_lv["filled_price"])
            sell_px  = sell_event.avg_price
            qty      = sell_event.executed_qty

            # 計算完整 round-trip PnL（含手續費）
            pnl_info = self._fee_calc.calc_round_trip_pnl(
                buy_qty   = qty,
                buy_price = buy_px,
                sell_price = sell_px,
            )
            db.record_grid_trade(
                symbol   = self.symbol,
                buy_px   = buy_px,
                sell_px  = sell_px,
                qty      = qty,
                fee      = pnl_info["total_fees"],
            )
            # ① 同步推入記憶體 PnL 快取（check_24h_drawdown 免查 DB）
            risk_mgr.push_pnl(self.symbol, pnl_info["net_pnl"])
            logger.info(
                f"[{self.symbol}] 網格完成 "
                f"buy={buy_px:.4f} sell={sell_px:.4f} "
                f"net_pnl={pnl_info['net_pnl']:+.4f} USDT "
                f"({pnl_info['return_pct']:+.3f}%)"
            )
        except Exception as e:
            logger.warning(f"[{self.symbol}] 記錄 PnL 失敗: {e}")

    def stop(self) -> None:
        self.manager.cancel_all()
        self._deployed = False
        logger.info(f"[{self.symbol}] 網格已停止")


# ════════════════════════════════════════════════════════════
# 主迴圈
# ════════════════════════════════════════════════════════════
def _get_exchange(retries: int = MAX_RETRY) -> ExchangeClient:
    for i in range(retries):
        try:
            return ExchangeClient()
        except Exception as e:
            w = min(2 ** i, 120)
            logger.warning(f"[連線] 第{i+1}次失敗: {e} → {w}s 後重試")
            time.sleep(w)
    raise RuntimeError("無法連線至 Binance")


def _graceful_exit(
    ex:        Optional[ExchangeClient],
    ws:        Optional[object],
    instances: Dict[str, object],
    reason:    str = "優雅關閉",
    heartbeat: Optional["HeartbeatMonitor"] = None,
) -> None:
    """
    ③ 優雅關閉：
      1. 停止所有網格實例（撤銷掛單）
      2. 停止 WebSocket
      3. 等待 DB Writer 佇列清空（最多 30 秒）
    注意：不觸發 KillSwitch 的「市價平倉」，僅撤單後靜止。
    若需完全平倉，應在呼叫前手動觸發 kill_switch.trigger()。
    """
    logger.info(f"[Shutdown] 開始優雅關閉，原因：{reason}")
    # ② 停止心跳排程
    if heartbeat:
        heartbeat.stop()
    for inst in instances.values():
        try:
            inst.stop()
        except Exception as e:
            logger.warning(f"[Shutdown] 停止網格實例失敗: {e}")
    if ws:
        try:
            ws.stop()
        except Exception:
            pass
    # ③ 確保所有寫入落盤再退出
    logger.info("[Shutdown] 等待 DB 寫入佇列清空...")
    flushed = db.flush(timeout=30)
    if flushed:
        logger.info("[Shutdown] DB 寫入已全部落盤 ✅")
    else:
        logger.warning("[Shutdown] DB 寫入等待超時，部分資料可能遺失 ⚠")
    db.stop()
    logger.info("[Shutdown] 已安全退出")


def run_grid_bot() -> None:
    global _is_running
    logger.info("╔══════════════════════════════════════════╗")
    logger.info("║   AQT_v1 動態網格機器人啟動（強化版）    ║")
    logger.info("╚══════════════════════════════════════════╝")

    ex:        Optional[ExchangeClient]          = None
    ws:        Optional[BinanceUserStream]        = None
    instances: Dict[str, GridBotInstance]         = {}
    errs = 0

    # ② 啟動心跳監控（12h 定時向 Telegram 發送系統狀態）
    hb = HeartbeatMonitor(
        db              = db,
        risk_mgr        = risk_mgr,
        circuit_breaker = circuit_breaker,
        instances       = instances,
        interval_hours  = float(os.getenv("HEARTBEAT_INTERVAL_HOURS", "12")),
    )
    hb.start()

    testnet = os.getenv("BINANCE_TESTNET", "true").lower() == "true"

    # ③ 使用 _is_running 旗標控制主迴圈（SIGTERM/SIGINT 設為 False）
    while _is_running:
        try:
            # ── 建立交易所連線 ───────────────────────────────
            if ex is None:
                ex   = _get_exchange()
                errs = 0
                logger.info("[OK] 已連線至 Binance")

                # ── 啟動對帳（狀態恢復）────────────────────────
                try:
                    rec    = Reconciler(client=ex.client, db=db)
                    report = rec.run()
                    logger.info(f"[Reconcile] {report.summary}")
                    if report.errors:
                        for err in report.errors:
                            logger.warning(err)
                except Exception as e:
                    logger.warning(f"[Reconcile] 對帳失敗（不影響主流程）: {e}")

                # ③ 狀態 Hydration：從 DB + Binance 還原記憶體狀態
                try:
                    risk_mgr.hydrate(db=db, client=ex.client)
                except Exception as e:
                    logger.warning(f"[Hydrate] 狀態還原失敗（不影響主流程）: {e}")

                # ── 啟動 WebSocket ───────────────────────────
                if USE_WS:
                    if ws:
                        ws.stop()
                    ws = BinanceUserStream(
                        api_key    = os.getenv("BINANCE_API_KEY",    "").strip(),
                        api_secret = os.getenv("BINANCE_API_SECRET", "").strip(),
                        market     = StreamMarket.SPOT,
                        testnet    = testnet,
                    )
                    ws.start()
                    logger.info("[WS] User Data Stream 已啟動")

            # ── 讀取 RUNNING 網格設定 ────────────────────────
            cfgs = [c for c in db.get_all_grid_configs() if c.get("status") == "RUNNING"]
            if not cfgs:
                logger.info("[Grid] 無執行中的網格設定，等待...")
            else:
                for cfg in cfgs:
                    sym = cfg["symbol"]
                    if sym not in instances:
                        instances[sym] = GridBotInstance(cfg, ex.client, ws)
                        logger.info(f"[Init] {sym} 網格實例已建立")
                    instances[sym].tick()

            # ── 清除已停止的實例 ─────────────────────────────
            running_syms = {c["symbol"] for c in cfgs}
            for sym in list(instances.keys()):
                if sym not in running_syms:
                    instances[sym].stop()
                    del instances[sym]

            errs = 0
            _maybe_run_cleanup()   # 每天自動清理 DB 舊紀錄

            # ③ 使用 _shutdown_ev 替代 time.sleep，以便 SIGTERM 立即喚醒
            _shutdown_ev.wait(timeout=INTERVAL)
            _shutdown_ev.clear()

        except Exception as e:
            errs += 1
            w = min(2 ** errs, 300)
            logger.error(f"[Loop] 錯誤#{errs}: {e} → {w}s 後重連", exc_info=True)
            # ⑤ 向斷路器報告 API 層級錯誤
            tripped = circuit_breaker.record_error(detail=str(e)[:200])
            if tripped:
                # 熔斷器剛觸發 → Telegram 通知
                alerts.notify_circuit_breaker(
                    error_count  = errs,
                    cooldown_sec = int(os.getenv("CB_COOLDOWN_SEC", "600")),
                    detail       = str(e)[:200],
                )
            ex = None
            if ws:
                ws.stop()
                ws = None
            # ③ 同樣使用 Event 等待，讓 SIGTERM 可以提前喚醒
            _shutdown_ev.wait(timeout=min(w, 60))
            _shutdown_ev.clear()

    # ③ 主迴圈退出後執行優雅關閉（含停止心跳）
    _graceful_exit(ex, ws, instances, reason="收到關閉訊號", heartbeat=hb)


if __name__ == "__main__":
    run_grid_bot()
