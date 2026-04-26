"""
quant_system/utils/heartbeat.py — Telegram 心跳包監控（② Phase G）

功能：
  每 12 小時（可設定）透過 Telegram 發送一次系統狀態心跳包，包含：
    🟢 存活狀態與執行時間
    💰 當日已實現獲利（USDT）
    📊 目前各幣種曝險比例
    ⚡ 斷路器狀態
    🔢 活躍格位數量

設計：
  - 使用 threading.Timer 鏈式呼叫（每次觸發後重新調度下一次），無需 cron
  - 主程式退出時自動取消（daemon=True 概念，透過 stop() 主動取消）
  - 若 Telegram 未設定，心跳訊息僅記錄至日誌

整合方式（crypto_main_grid.py）：
    from quant_system.utils.heartbeat import HeartbeatMonitor

    hb = HeartbeatMonitor(
        db=db,
        risk_mgr=risk_mgr,
        circuit_breaker=circuit_breaker,
        instances=instances,   # Dict[symbol, GridBotInstance]
        interval_hours=12.0,
    )
    hb.start()

    # 優雅關閉時
    hb.stop()
"""
from __future__ import annotations

import logging
import os
import threading
import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Dict, Optional

log = logging.getLogger(__name__)

if TYPE_CHECKING:
    from database import TradeDB
    from quant_system.risk.risk_manager import RiskManager
    from quant_system.risk.circuit_breaker import CircuitBreaker


class HeartbeatMonitor:
    """
    ② 系統健康心跳包排程器。

    Args:
        db              — TradeDB 實例（讀取當日獲利）
        risk_mgr        — RiskManager 實例（讀取曝險資訊）
        circuit_breaker — CircuitBreaker 實例（讀取斷路器狀態）
        instances       — {symbol: GridBotInstance}（讀取活躍格位數量）
        interval_hours  — 心跳間隔（小時，預設 12）
        telegram_token  — Telegram Bot Token（預設從環境變數讀取）
        telegram_chat   — Telegram Chat ID（預設從環境變數讀取）
    """

    def __init__(
        self,
        db:              Optional["TradeDB"]           = None,
        risk_mgr:        Optional["RiskManager"]       = None,
        circuit_breaker: Optional["CircuitBreaker"]    = None,
        instances:       Optional[Dict]                = None,
        interval_hours:  float                         = 12.0,
        telegram_token:  str                           = "",
        telegram_chat:   str                           = "",
    ) -> None:
        self._db              = db
        self._risk_mgr        = risk_mgr
        self._circuit_breaker = circuit_breaker
        self._instances       = instances or {}
        self._interval_sec    = interval_hours * 3600

        self._token = telegram_token or os.getenv("TELEGRAM_BOT_TOKEN", "")
        self._chat  = telegram_chat  or os.getenv("TELEGRAM_CHAT_ID",   "")
        self._use_telegram = bool(self._token and self._chat)

        self._started_at: float = 0.0
        self._timer: Optional[threading.Timer] = None
        self._lock  = threading.Lock()
        self._stop_flag = False

        if not self._use_telegram:
            log.info("[Heartbeat] Telegram 未設定，心跳訊息僅記錄至日誌")

    # ── 公開介面 ─────────────────────────────────────────────
    def start(self) -> None:
        """啟動心跳排程（立即發送第一次，之後每 interval_hours 發送）。"""
        self._started_at = time.time()
        self._stop_flag  = False
        log.info(
            f"[Heartbeat] 啟動，間隔={self._interval_sec/3600:.1f}h "
            f"Telegram={'啟用' if self._use_telegram else '停用'}"
        )
        # 立即發一次（確認機器人已啟動）
        self._send_heartbeat()
        self._schedule_next()

    def stop(self) -> None:
        """停止心跳排程（優雅關閉時呼叫）。"""
        with self._lock:
            self._stop_flag = True
            if self._timer:
                self._timer.cancel()
                self._timer = None
        log.info("[Heartbeat] 已停止")

    def send_now(self, extra_msg: str = "") -> None:
        """立即發送一次心跳（用於手動觸發或警示通知）。"""
        self._send_heartbeat(extra=extra_msg)

    # ── 內部排程 ─────────────────────────────────────────────
    def _schedule_next(self) -> None:
        """鏈式 Timer：觸發後立刻調度下一次。"""
        with self._lock:
            if self._stop_flag:
                return
            self._timer = threading.Timer(self._interval_sec, self._on_tick)
            self._timer.daemon = True
            self._timer.start()

    def _on_tick(self) -> None:
        self._send_heartbeat()
        self._schedule_next()

    # ── 心跳訊息建構 ─────────────────────────────────────────
    def _send_heartbeat(self, extra: str = "") -> None:
        try:
            msg = self._build_message(extra)
            log.info(f"[Heartbeat] {msg}")
            if self._use_telegram:
                self._post_telegram(msg)
        except Exception as e:
            log.warning(f"[Heartbeat] 發送失敗: {e}")

    def _build_message(self, extra: str = "") -> str:
        """組裝心跳訊息字串。"""
        now_utc  = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        uptime_h = (time.time() - self._started_at) / 3600 if self._started_at else 0.0

        lines: list[str] = [
            "💓 AQT_v1 心跳包",
            f"🕒 時間：{now_utc}",
            f"⏱ 運行時長：{uptime_h:.1f}h",
        ]

        # 當日獲利
        daily_pnl = self._get_daily_pnl()
        pnl_icon  = "🟢" if daily_pnl >= 0 else "🔴"
        lines.append(f"{pnl_icon} 當日獲利：{daily_pnl:+.2f} USDT")

        # 各幣種曝險
        exposure_lines = self._get_exposure_summary()
        if exposure_lines:
            lines.append("📊 曝險概況：")
            lines.extend(exposure_lines)
        else:
            lines.append("📊 曝險：無活躍倉位")

        # 斷路器狀態
        cb_status = self._get_cb_status()
        lines.append(f"⚡ 斷路器：{cb_status}")

        # 活躍格位
        total_levels = sum(
            inst.grid_manager.active_count()
            for inst in self._instances.values()
            if hasattr(inst, "grid_manager")
        )
        lines.append(f"🔢 活躍格位：{total_levels} 筆")

        if extra:
            lines.append(f"📌 附加訊息：{extra}")

        return "\n".join(lines)

    def _get_daily_pnl(self) -> float:
        """從 DB 查詢今日已實現 PnL（UTC 日期）。"""
        if not self._db:
            return 0.0
        try:
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            # 直接用 DB 的 read connection 查詢
            import sqlite3
            db_path = getattr(self._db, "_db_path", None) or os.getenv("DB_PATH", "trading_bot.db")
            conn = sqlite3.connect(db_path, check_same_thread=False)
            try:
                row = conn.execute(
                    "SELECT COALESCE(SUM(net_pnl), 0.0) "
                    "FROM grid_trades "
                    "WHERE date(closed_at) = ?",
                    (today,)
                ).fetchone()
                return float(row[0]) if row else 0.0
            finally:
                conn.close()
        except Exception as e:
            log.debug(f"[Heartbeat] 查詢當日 PnL 失敗: {e}")
            return 0.0

    def _get_exposure_summary(self) -> list[str]:
        """從 RiskManager 取得各幣種曝險比例。"""
        if not self._risk_mgr:
            return []
        try:
            lines = []
            pos_map = getattr(self._risk_mgr, "_pos", {})
            exp_map = getattr(self._risk_mgr, "_exposure", {})
            for sym in list(pos_map.keys()) or list(exp_map.keys()):
                exp = exp_map.get(sym, 0.0)
                pos = pos_map.get(sym)
                qty = getattr(pos, "quantity", 0.0) if pos else 0.0
                lines.append(f"  {sym}: {exp:.1f} USDT (qty={qty:.6f})")
            return lines[:5]   # 最多顯示 5 個幣種
        except Exception:
            return []

    def _get_cb_status(self) -> str:
        """讀取斷路器狀態。"""
        if not self._circuit_breaker:
            return "未設定"
        try:
            from quant_system.risk.circuit_breaker import (
                STATE_CLOSED, STATE_OPEN, STATE_HALF_OPEN
            )
            state = getattr(self._circuit_breaker, "_state", STATE_CLOSED)
            if state == STATE_CLOSED:
                return "✅ 正常（CLOSED）"
            elif state == STATE_OPEN:
                return "🔴 熔斷中（OPEN）"
            else:
                return "🟡 測試中（HALF_OPEN）"
        except Exception:
            return "未知"

    # ── Telegram 發送 ─────────────────────────────────────────
    def _post_telegram(self, text: str) -> None:
        """以 Telegram Bot API 發送訊息。"""
        try:
            import urllib.request
            import json
            url     = f"https://api.telegram.org/bot{self._token}/sendMessage"
            payload = json.dumps({
                "chat_id":    self._chat,
                "text":       text,
                "parse_mode": "HTML",
            }).encode("utf-8")
            req = urllib.request.Request(
                url,
                data    = payload,
                headers = {"Content-Type": "application/json"},
                method  = "POST",
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                if resp.status != 200:
                    log.warning(f"[Heartbeat] Telegram API 回傳 {resp.status}")
        except Exception as e:
            log.warning(f"[Heartbeat] Telegram 發送失敗: {e}")
