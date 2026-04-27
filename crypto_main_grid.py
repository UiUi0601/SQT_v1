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
import os, sys, time, logging, signal, threading, json, uuid
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
from quant_system.exchange.market_factory    import MarketFactory
from quant_system.exchange.market_data_stream import MarketDataStream
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

# ── SEMI_AUTO 執行模式（可由 UI /api/set_execution_mode 動態切換）──
# FULL_AUTO：機器人全自動執行（預設）
# SEMI_AUTO ：機器人產出訊號後等待人工確認再執行
TRADE_EXECUTION_MODE  = os.getenv("TRADE_EXECUTION_MODE",  "FULL_AUTO").upper()
SIGNAL_EXPIRE_SECONDS = int(os.getenv("SIGNAL_EXPIRE_SECONDS", "30"))

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
        self._ex_client = client   # python-binance Client; used by FuturesRiskMonitor.refresh

        # ── 市場類型與槓桿設定 ──────────────────────────────
        self.market_type       = cfg.get("market_type",        "SPOT").upper()
        self.leverage          = int(cfg.get("leverage",        1))
        self.margin_type_cfg   = cfg.get("margin_type",        "CROSSED").upper()
        self.position_side_mode = cfg.get("position_side_mode", "BOTH").upper()

        # ── 交易模式（雙模式核心）──────────────────────────────
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
            client         = client,
            symbol         = self.symbol,
            qty_per_grid   = float(cfg.get("qty_per_grid", 0.001)),
            precision      = self._prec,
            market_type    = self.market_type,
            position_side  = self.position_side_mode if self.position_side_mode != "BOTH" else "BOTH",
            is_isolated    = self.margin_type_cfg == "ISOLATED",
        )
        self.manager.setup()

        # ── 市場風險監控器（FUTURES 強平 / MARGIN 保證金水位）──
        self._market_risk = MarketFactory.create_risk_monitor(cfg, risk_mgr)

        # ── FUTURES 初始化：設定槓桿與保證金模式（一次性）──────
        if self.market_type == "FUTURES":
            self._init_futures_settings(client)

        self.cache      = DataCache(ttl_seconds=60)
        self.snap       = None
        self._deployed  = False
        self._obs_snap  = None

    def _init_futures_settings(self, client) -> None:
        """在 GridBotInstance 建立時，對 FUTURES 幣種設定槓桿與保證金模式。"""
        try:
            client.futures_change_leverage(
                symbol=self.symbol, leverage=self.leverage
            )
            logger.info(
                f"[{self.symbol}] FUTURES 槓桿設定完成：{self.leverage}×"
            )
        except Exception as e:
            logger.warning(f"[{self.symbol}] 設定槓桿失敗（可能已設定）: {e}")
        try:
            client.futures_change_margin_type(
                symbol=self.symbol, marginType=self.margin_type_cfg
            )
            logger.info(
                f"[{self.symbol}] FUTURES 保證金模式設定完成：{self.margin_type_cfg}"
            )
        except Exception as e:
            # -4046 = No need to change margin type (already set)
            if "-4046" not in str(e):
                logger.warning(f"[{self.symbol}] 設定保證金模式失敗: {e}")

    # ════════════════════════════════════════════════════════
    # SEMI_AUTO 訊號工具
    # ════════════════════════════════════════════════════════

    def _build_semi_auto_signal(
        self,
        current_price: float,
        df1h,
        snap: "GridSnapshot",
        fil,
    ) -> dict:
        """
        建立帶有技術指標的待確認訊號 dict。
        indicator_data 包含：price, ATR, 網格上下限/格距/中心,
        趨勢過濾結果（allow_buy/allow_sell），以及 EMA200（如資料足夠）。
        """
        ema200: float = 0.0
        try:
            if df1h is not None and len(df1h) >= 200:
                ema200 = float(df1h["Close"].rolling(200).mean().iloc[-1])
        except Exception:
            pass

        levels = getattr(snap, "levels", []) or []
        n_buy  = sum(1 for lv in levels if getattr(lv, "side", "") == "BUY")
        n_sell = sum(1 for lv in levels if getattr(lv, "side", "") == "SELL")
        dominant_side = "BUY" if n_buy >= n_sell else "SELL"

        indicator_data = json.dumps({
            "price":        round(current_price, 6),
            "atr":          round(getattr(snap, "atr", 0.0), 6),
            "upper":        round(getattr(snap, "upper", 0.0), 6),
            "lower":        round(getattr(snap, "lower", 0.0), 6),
            "spacing":      round(getattr(snap, "spacing", 0.0), 6),
            "center":       round(getattr(snap, "center", 0.0), 6),
            "spacing_src":  getattr(snap, "spacing_source", ""),
            "allow_buy":    fil.allow_buy,
            "allow_sell":   fil.allow_sell,
            "ema200":       round(ema200, 6),
            "grid_count":   getattr(snap, "grid_count", 0),
            "fee_ok":       getattr(snap, "fee_ok", True),
        })

        return {
            "signal_id":      str(uuid.uuid4()),
            "symbol":         self.symbol,
            "side":           dominant_side,
            "price":          current_price,
            "quantity":       float(self.cfg.get("qty_per_grid", 0.001)),
            "market_type":    self.market_type,
            "leverage":       self.leverage,
            "indicator_data": indicator_data,
            "signal_reason":  (
                f"SEMI_AUTO deploy: center={getattr(snap,'center',0):.4f} "
                f"atr={getattr(snap,'atr',0):.4f}"
            ),
            "slippage_pct":   float(os.getenv("SEMI_AUTO_SLIPPAGE_PCT", "0.005")),
            "expires_at":     time.time() + SIGNAL_EXPIRE_SECONDS,
        }

    def _await_signal_confirmation(self, signal: dict, timeout: float) -> bool:
        """
        ① 將訊號寫入 DB（pending_signals 表）
        ② 輪詢 DB 等待 UI 確認（CONFIRMED/REJECTED/EXPIRED）
        ③ CONFIRMED 後進行滑點驗證（比對 DataCache 最新收盤價）
        返回 True = 可執行下單；False = 放棄（已在內部記錄日誌）。

        每 0.5 秒檢查一次；在 _is_running=False 或 _shutdown_ev 設定時
        立即退出，確保 SIGTERM 能即時響應。
        """
        db.upsert_pending_signal(
            signal_id      = signal["signal_id"],
            symbol         = signal["symbol"],
            side           = signal["side"],
            price          = signal["price"],
            quantity       = signal["quantity"],
            market_type    = signal["market_type"],
            leverage       = signal["leverage"],
            indicator_data = signal["indicator_data"],
            signal_reason  = signal["signal_reason"],
            slippage_pct   = signal["slippage_pct"],
            expires_at     = signal["expires_at"],
        )
        sid_short = signal["signal_id"][:8]

        # 訊號推播：寫入 DB 後 /ws/signals WebSocket 每秒輪詢並推送至 UI。
        # 同時透過 Telegram alerts 發出通知（未設 TOKEN 時靜默）。
        try:
            ind = json.loads(signal.get("indicator_data", "{}"))
            alerts.notify_semi_signal(
                symbol     = signal["symbol"],
                side       = signal["side"],
                price      = signal["price"],
                signal_id  = signal["signal_id"],
                atr        = ind.get("atr", 0.0),
                upper      = ind.get("upper", 0.0),
                lower      = ind.get("lower", 0.0),
                expires_in = int(timeout),
            )
        except Exception:
            pass  # 推播失敗不影響確認等待流程

        logger.info(
            f"[{self.symbol}] SEMI_AUTO 訊號已發出 "
            f"id={sid_short}… side={signal['side']} "
            f"price={signal['price']:.4f}，等待 UI 確認（逾時 {timeout:.0f}s）"
        )

        deadline = time.time() + timeout
        while time.time() < deadline:
            # 優先響應關閉訊號
            if not _is_running or _shutdown_ev.is_set():
                logger.info(
                    f"[{self.symbol}] SEMI_AUTO 收到關閉訊號，中止等待 id={sid_short}…"
                )
                return False

            row = db.get_pending_signal(signal["signal_id"])
            if row:
                status = row.get("status", "PENDING")

                if status == "CONFIRMED":
                    # ── 滑點驗證：取 DataCache 最新收盤價與訊號價格比較 ──
                    try:
                        fresh_dfs = self.cache.get_multi_timeframe(
                            self.yf_symbol, {"1h": MTF["1h"]}
                        )
                        fresh_df = fresh_dfs.get("1h")
                        if fresh_df is not None and not fresh_df.empty:
                            fresh_px = float(fresh_df["Close"].iloc[-1])
                            drift    = abs(fresh_px - signal["price"]) / signal["price"]
                            limit    = signal.get("slippage_pct", 0.005)
                            if drift > limit:
                                logger.warning(
                                    f"[{self.symbol}] SEMI_AUTO 滑點驗證失敗 "
                                    f"id={sid_short}… "
                                    f"signal_px={signal['price']:.4f} "
                                    f"now={fresh_px:.4f} "
                                    f"drift={drift:.3%} > limit={limit:.3%}，"
                                    f"放棄下單"
                                )
                                return False
                    except Exception:
                        pass  # 無法取得最新價格，寬鬆放行
                    logger.info(
                        f"[{self.symbol}] SEMI_AUTO 訊號 id={sid_short}… "
                        f"UI 確認通過 ✅"
                    )
                    return True

                if status in ("REJECTED", "EXPIRED"):
                    logger.warning(
                        f"[{self.symbol}] SEMI_AUTO 訊號 id={sid_short}… "
                        f"已{status}，放棄此次下單"
                    )
                    return False

            time.sleep(0.5)

        # 逾時 → 標記 DB 中所有過期訊號
        db.expire_stale_signals()
        logger.warning(
            f"[{self.symbol}] SEMI_AUTO 訊號 id={sid_short}… "
            f"等待確認逾時 ({timeout:.0f}s)，放棄下單"
        )
        return False

    # ── 每輪執行 ─────────────────────────────────────────────
    def tick(self) -> None:
        # ⓪-A 全域緊急停止檢查
        if kill_switch.is_paused:
            logger.warning(f"[{self.symbol}] Kill Switch 觸發中，跳過所有操作")
            return

        # ⓪-B API 斷路器檢查（熔斷期間拒絕任何 API 呼叫）
        cb_ok, cb_reason = circuit_breaker.check()
        if not cb_ok:
            logger.warning(f"[{self.symbol}] {cb_reason}")
            return

        # ══════════════════════════════════════════════════════
        # 階段一：觀測 (Observation) — Auto/Semi 無條件執行
        # ══════════════════════════════════════════════════════

        # 1-A: 拉取 K 線資料
        dfs  = self.cache.get_multi_timeframe(self.yf_symbol, MTF)
        df1h = dfs.get("1h")
        df1d = dfs.get("1d")
        if df1h is None or df1h.empty:
            logger.warning(f"[{self.symbol}] 1h 資料缺失，跳過")
            return
        current_price = float(df1h["Close"].iloc[-1])

        # 1-A2: FUTURES 強平資料刷新（Observation Phase，永遠執行，不檢查系統鎖）
        # FuturesRiskMonitor 內建限速（預設 60s），此處無條件呼叫即可
        if self.market_type == "FUTURES":
            try:
                self._market_risk.refresh_from_exchange(self._ex_client, self.symbol)
            except Exception as _rf_err:
                logger.debug(f"[{self.symbol}] FUTURES 強平資料刷新失敗（不影響主流程）: {_rf_err}")

        # 1-B: 趨勢過濾（仍對兩模式有效；超出趨勢則撤單暫停）
        fil = self.filter.check(
            df1d if (df1d is not None and len(df1d) > 200) else df1h
        )
        if not fil.allow_grid:
            logger.warning(f"[{self.symbol}] 網格暫停: {fil.reason}")
            if self._deployed:
                self.manager.cancel_all()
                self._deployed = False
            return

        # 1-C: 極端風控監控 — 無論 Auto/Semi 皆執行；結果傳遞給執行階段
        dd_ok, dd_status = risk_mgr.check_24h_drawdown(self.symbol)
        if not dd_ok:
            logger.warning(f"[{self.symbol}] 24h 回撤風控觸發: {dd_status.reason}")

        # 1-D: 計算並更新技術指標快照（更新 self.snap 狀態；不觸發真實佈署）
        if self.trade_mode == "semi":
            upper = float(self.cfg.get("upper_price", 0))
            lower = float(self.cfg.get("lower_price", 0))
            gc    = int(self.cfg.get("grid_count", 10))
            if upper > lower > 0:
                built = _build_semi_snap(
                    upper, lower, gc, current_price,
                    fee_rate=self._fee_calc.taker_fee,
                )
                if built:
                    self.snap = built  # 更新快照供 UI 查詢；不執行 deploy
        else:
            computed = self.engine.compute(df1h)
            if computed:
                self._obs_snap = computed  # 候選快照；執行階段再決定是否 deploy

        # 1-E: 監聽並同步現有訂單成交狀態（純資料同步；on_fill 推遲至執行階段）
        fills_pending: list = []
        if self._deployed and self.snap:
            raw_fills = self._collect_fills()
            for fill_event in raw_fills:
                # DB 狀態更新
                db.update_grid_level_filled(
                    self.grid_id, fill_event.order_id,
                    fill_event.avg_price, fill_event.executed_qty,
                )
                # 手續費計算：更新 BUY 安全賣出量
                if fill_event.side == "BUY":
                    fee_res = self._fee_calc.calc_buy(
                        fill_event.executed_qty, fill_event.avg_price
                    )
                    safe_qty = fee_res.safe_sell_qty
                    logger.info(
                        f"[{self.symbol}] BUY 成交 qty={fill_event.executed_qty:.6f} "
                        f"→ 到帳 {fee_res.net_qty:.6f} 安全賣量 {safe_qty:.6f}"
                    )
                    fill_event.executed_qty = safe_qty
                # SELL 成交：記錄 PnL 並推入風控快取
                if fill_event.side == "SELL":
                    self._record_grid_trade(fill_event)
                logger.info(
                    f"[{self.symbol}] {fill_event.side} 成交同步 "
                    f"avg_px={fill_event.avg_price:.4f} "
                    f"qty={fill_event.executed_qty:.6f} "
                    f"complete={fill_event.is_complete}"
                )
                fills_pending.append(fill_event)

        # ══════════════════════════════════════════════════════
        # 階段二：執行 (Execution) — 模式權限分離點
        # ══════════════════════════════════════════════════════

        # ── 全域安全鎖門（System Lock）───────────────────────
        # 由 UI 的「解鎖交易雙手」按鈕控制（db.set_system_lock(True)）。
        # 觀測階段（Phase 1）永遠不接觸此鎖；鎖關閉時只是靜默回傳，
        # 所有 Binance 真實下單操作都不會發生。
        if not db.get_system_lock():
            logger.debug(
                f"[{self.symbol}] 系統安全鎖關閉（is_unlocked=0），略過執行階段"
            )
            return

        # ── 市場風險檢查（FUTURES 強平距離 / MARGIN 保證金水位）──
        market_risk_result = self._market_risk.check(
            current_price = current_price,
            proposed_usdt = 0.0,   # 純檢查，非新增倉位
            total_equity  = float(self.cfg.get("capital", 1000.0)),
        )
        if not market_risk_result.ok:
            logger.warning(f"[{self.symbol}] 市場風險封鎖: {market_risk_result.reason}")
            return

        if self.trade_mode == "auto":
            # AUTO 模式：允許 on_fill 反向掛單 + deploy 重新鋪設
            for fill_event in fills_pending:
                net_pnl: Optional[float] = None
                if fill_event.side == "SELL" and self.snap:
                    try:
                        pnl_raw = self._fee_calc.calc_round_trip_pnl(
                            buy_qty    = fill_event.executed_qty,
                            buy_price  = fill_event.avg_price - self.snap.spacing,
                            sell_price = fill_event.avg_price,
                        )
                        net_pnl = pnl_raw.get("net_pnl")
                    except Exception:
                        pass
                new_oid = self.manager.on_fill(
                    fill_event, self.snap,
                    allow_buy=fil.allow_buy, allow_sell=fil.allow_sell,
                )
                reverse_price: Optional[float] = None
                if new_oid and self.snap:
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
            self._tick_auto(current_price, df1h, fil, dd_ok, dd_status)

        else:
            # SEMI 模式：執行階段分三層
            #   ① 成交通知（保留）
            #   ② 訊號生成 + 人工確認（系統鎖已解開才走到這裡）
            #   ③ 止損防禦（保留）
            logger.debug(
                f"[{self.symbol}] SEMI 執行階段 — "
                f"系統鎖已解鎖，deploy 需等待 UI 確認"
            )

            # ① 成交通知（無反向掛單；semi 不掛反向單）
            for fill_event in fills_pending:
                alerts.notify_fill(
                    symbol        = self.symbol,
                    side          = fill_event.side,
                    avg_price     = fill_event.avg_price,
                    qty           = fill_event.executed_qty,
                    net_pnl       = None,
                    reverse_price = None,
                )

            # ② 訊號生成 + 等待人工確認（真正的 SEMI_AUTO 流程）
            #    系統安全鎖已在階段二入口保證解鎖；此處一定是允許執行的狀態。
            #    條件：未部署 + snap 已就緒 + 24h 回撤正常 + 曝險未超限
            if not self._deployed and self.snap:
                if dd_ok:
                    n_buy_levels = sum(
                        1 for lv in (getattr(self.snap, "levels", []) or [])
                        if getattr(lv, "side", "") == "BUY"
                    )
                    proposed_usdt = (
                        n_buy_levels
                        * float(self.cfg.get("qty_per_grid", 0.001))
                        * current_price
                    )
                    total_equity = float(self.cfg.get("capital", 1000.0))
                    # ── 曝險上限（保留原有風控，不可刪除）──────────────
                    exp_ok, exp_reason, _ = risk_mgr.check_exposure(
                        self.symbol, proposed_usdt, total_equity
                    )
                    if exp_ok:
                        signal = self._build_semi_auto_signal(
                            current_price, df1h, self.snap, fil
                        )
                        confirmed = self._await_signal_confirmation(
                            signal, timeout=float(SIGNAL_EXPIRE_SECONDS)
                        )
                        if confirmed:
                            risk_mgr.update_exposure(self.symbol, proposed_usdt)
                            placed = self.manager.deploy(
                                self.snap, current_price,
                                allow_buy=fil.allow_buy, allow_sell=fil.allow_sell,
                            )
                            db.save_grid_levels(self.grid_id, self.symbol, placed)
                            db.set_grid_last_regrid(self.symbol)
                            self._deployed = True
                            logger.info(
                                f"[{self.symbol}] SEMI_AUTO 網格部署成功 "
                                f"center={self.snap.center:.4f} "
                                f"levels={self.snap.grid_count}"
                            )
                            alerts.notify_grid_deployed(
                                symbol     = self.symbol,
                                mode       = "semi",
                                upper      = self.snap.upper,
                                lower      = self.snap.lower,
                                spacing    = self.snap.spacing,
                                grid_count = self.snap.grid_count,
                                qty        = float(self.cfg.get("qty_per_grid", 0.001)),
                            )
                    else:
                        logger.warning(
                            f"[{self.symbol}] SEMI_AUTO 曝險攔截: {exp_reason}"
                        )
                else:
                    logger.warning(
                        f"[{self.symbol}] SEMI_AUTO 24h 回撤暫停 "
                        f"({dd_status.reason})，略過訊號生成"
                    )

            # ③ 止損觸發：撤銷現有掛單屬防禦性操作，非新建單，仍允許執行
            sl = float(self.cfg.get("stop_loss_price", 0))
            if sl > 0 and current_price <= sl:
                logger.warning(
                    f"[{self.symbol}] SEMI 止損觸發！"
                    f"price={current_price:.4f} <= SL={sl:.4f}，撤銷所有掛單並暫停"
                )
                self.manager.cancel_all()
                self._deployed = False
                db.set_grid_status(self.symbol, "STOPPED")
                alerts.notify_stop_loss(
                    symbol        = self.symbol,
                    current_price = current_price,
                    stop_price    = sl,
                )

    # ════════════════════════════════════════════════════════
    # 全自動模式執行 Tick：ATR/EMA 動態計算（核心演算法完整保留）
    # dd_ok / dd_status 由 tick() 階段一統一計算後傳入，避免重複查詢
    # ════════════════════════════════════════════════════════
    def _tick_auto(self, current_price: float, df1h, fil, dd_ok: bool, dd_status) -> None:
        # 判斷是否需要重新鋪設
        need_regrid = (not self._deployed) or self.engine.should_regrid(df1h, threshold=0.05)
        if not need_regrid:
            return

        # 24h 回撤暫停（由觀測階段傳入，無需重複呼叫）
        if not dd_ok:
            logger.warning(f"[{self.symbol}] 24h 回撤暫停: {dd_status.reason}")
            if self._deployed:
                self.manager.cancel_all()
                self._deployed = False
            return

        # 使用觀測階段預計算的快照；若未命中則立即計算
        new_snap = self._obs_snap or self.engine.compute(df1h)
        self._obs_snap = None   # 用後清空，下一輪重新計算
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

        # ── SEMI_AUTO 確認門：全自動計算完成後等待 UI 確認再部署 ──
        # 曝險在確認通過後才更新，避免拒絕/逾時時虛耗額度
        if TRADE_EXECUTION_MODE == "SEMI_AUTO":
            signal = self._build_semi_auto_signal(current_price, df1h, new_snap, fil)
            if not self._await_signal_confirmation(
                signal, timeout=float(SIGNAL_EXPIRE_SECONDS)
            ):
                logger.info(
                    f"[{self.symbol}] AUTO SEMI_AUTO 確認未通過，跳過本次部署"
                )
                return
        # ─────────────────────────────────────────────────────

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

    # ── 啟動 MarketDataStream（共用行情 WS，期貨幣種訂閱於 instance 建立時加入）
    market_stream = MarketDataStream(
        testnet = os.getenv("BINANCE_TESTNET", "true").lower() == "true"
    )
    market_stream.start()

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

                # ── 啟動 WebSocket（預設 SPOT；各幣種如需 FUTURES 串流
                #    則在 GridBotInstance 建立時傳入對應的 ws_stream）───
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
                    logger.info("[WS] User Data Stream（SPOT）已啟動")

            # ── 讀取 RUNNING 網格設定 ────────────────────────
            cfgs = [c for c in db.get_all_grid_configs() if c.get("status") == "RUNNING"]
            if not cfgs:
                logger.info("[Grid] 無執行中的網格設定，等待...")
            else:
                for cfg in cfgs:
                    sym = cfg["symbol"]
                    if sym not in instances:
                        instances[sym] = GridBotInstance(cfg, ex.client, ws)
                        # 注入行情串流至 DataCache，並訂閱該幣種
                        instances[sym].cache.set_stream(market_stream)
                        market_stream.subscribe(sym)
                        logger.info(f"[Init] {sym} 網格實例已建立，行情串流已訂閱")
                    instances[sym].tick()

            # ── 清除已停止的實例 ─────────────────────────────
            running_syms = {c["symbol"] for c in cfgs}
            for sym in list(instances.keys()):
                if sym not in running_syms:
                    instances[sym].stop()
                    market_stream.unsubscribe(sym)
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

    # 停止行情串流
    try:
        market_stream.stop()
    except Exception:
        pass

    # ③ 主迴圈退出後執行優雅關閉（含停止心跳）
    _graceful_exit(ex, ws, instances, reason="收到關閉訊號", heartbeat=hb)


if __name__ == "__main__":
    run_grid_bot()
