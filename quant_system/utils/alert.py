"""
quant_system/utils/alert.py — 通知系統（Telegram 選用）v2

v2 新增：
  - GridAlerts：網格機器人專用推播類別
    * notify_grid_deployed()   — 網格開始部署
    * notify_fill()            — 訂單成交 + 反向掛單
    * notify_stop_loss()       — 止損 / 止損觸發
    * notify_circuit_breaker() — 熔斷觸發
    * notify_bot_started()     — 機器人啟動
    * notify_bot_stopped()     — 機器人停止
  - 全部使用執行緒池非阻塞發送（不卡主迴圈）
  - 若 .env 未設 TELEGRAM_BOT_TOKEN，僅記錄 log，不報錯

SQT_v1 原有 AlertSystem 完整保留（向後相容）
"""
from __future__ import annotations
import os
import logging
import concurrent.futures
from datetime import datetime, timezone
from typing import Optional, Dict, Any

log = logging.getLogger(__name__)

# 共用執行緒池（全程式共享，避免 Telegram 發送阻塞主迴圈）
_tg_executor = concurrent.futures.ThreadPoolExecutor(
    max_workers=2, thread_name_prefix="tg_alert"
)


def _tg_send(token: str, chat_id: str, text: str) -> None:
    """
    同步發送 Telegram 訊息（由執行緒池呼叫，不阻塞主迴圈）。
    長度超過 4096 字元時自動截斷。
    """
    try:
        import requests
        payload = {
            "chat_id":    chat_id,
            "text":       text[:4096],
            "parse_mode": "HTML",
        }
        r = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json=payload, timeout=8,
        )
        if not r.ok:
            log.warning(f"[TG] 發送失敗 {r.status_code}: {r.text[:200]}")
    except Exception as e:
        log.warning(f"[TG] 發送異常: {e}")


class GridAlerts:
    """
    網格機器人專用 Telegram 推播。
    無 Token 時全部靜默（只記 log），不拋例外。
    """

    def __init__(self) -> None:
        self._token   = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
        self._chat    = os.getenv("TELEGRAM_CHAT_ID",   "").strip()
        self._enabled = bool(self._token and self._chat)
        if not self._enabled:
            log.info("[GridAlerts] Telegram 未設定，推播靜默")

    # ── internal ─────────────────────────────────────────────
    def _push(self, text: str) -> None:
        """非阻塞提交發送任務到執行緒池。"""
        log.info(f"[TG]\n{text}")
        if self._enabled:
            _tg_executor.submit(_tg_send, self._token, self._chat, text)

    @staticmethod
    def _ts() -> str:
        return datetime.now(timezone.utc).strftime("%m/%d %H:%M UTC")

    # ── 公開推播方法 ─────────────────────────────────────────

    def notify_bot_started(self, symbols: list, mode: str = "auto") -> None:
        mode_label = "🤖 全自動" if mode == "auto" else "🎯 半自動"
        self._push(
            f"✅ <b>AQT 網格機器人啟動</b>\n"
            f"模式：{mode_label}\n"
            f"幣種：{', '.join(symbols)}\n"
            f"時間：{self._ts()}"
        )

    def notify_bot_stopped(self, reason: str = "正常停止") -> None:
        self._push(
            f"🛑 <b>AQT 機器人停止</b>\n"
            f"原因：{reason}\n"
            f"時間：{self._ts()}"
        )

    def notify_grid_deployed(
        self,
        symbol:     str,
        mode:       str,   # "auto" | "semi"
        upper:      float,
        lower:      float,
        spacing:    float,
        grid_count: int,
        qty:        float,
    ) -> None:
        mode_label = "🤖 全自動" if mode == "auto" else "🎯 半自動"
        self._push(
            f"📐 <b>網格部署完成</b>  {symbol}\n"
            f"模式：{mode_label}\n"
            f"區間：${lower:,.4f} ～ ${upper:,.4f}\n"
            f"格數：{grid_count} 格  格距：${spacing:,.4f}\n"
            f"每格數量：{qty:.6f}\n"
            f"時間：{self._ts()}"
        )

    def notify_fill(
        self,
        symbol:    str,
        side:      str,    # "BUY" | "SELL"
        avg_price: float,
        qty:       float,
        net_pnl:   Optional[float] = None,
        reverse_price: Optional[float] = None,
    ) -> None:
        icon   = "🟢 BUY" if side == "BUY" else "🔴 SELL"
        pnl_s  = f"\n盈虧：{net_pnl:+.4f} USDT" if net_pnl is not None else ""
        rev_s  = f"\n反向掛單：${reverse_price:,.4f}" if reverse_price else ""
        self._push(
            f"{icon} <b>網格成交</b>  {symbol}\n"
            f"成交均價：${avg_price:,.4f}\n"
            f"數量：{qty:.6f}"
            f"{pnl_s}{rev_s}\n"
            f"時間：{self._ts()}"
        )

    def notify_stop_loss(
        self,
        symbol:        str,
        current_price: float,
        stop_price:    float,
    ) -> None:
        self._push(
            f"⛔ <b>止損觸發！</b>  {symbol}\n"
            f"當前價格：${current_price:,.4f}\n"
            f"止損價格：${stop_price:,.4f}\n"
            f"→ 已撤銷所有掛單，網格停止\n"
            f"時間：{self._ts()}"
        )

    def notify_circuit_breaker(
        self,
        error_count: int,
        cooldown_sec: int,
        detail: str = "",
    ) -> None:
        self._push(
            f"🔴 <b>API 熔斷觸發</b>\n"
            f"錯誤次數：{error_count}\n"
            f"冷卻時間：{cooldown_sec} 秒\n"
            f"原因：{detail[:200] if detail else '—'}\n"
            f"時間：{self._ts()}"
        )

    def notify_drawdown_pause(
        self,
        symbol:   str,
        drawdown: float,
        hours:    float,
    ) -> None:
        self._push(
            f"⚠️ <b>24h 回撤暫停</b>  {symbol}\n"
            f"回撤幅度：{drawdown:.2%}\n"
            f"暫停時長：{hours:.1f} 小時\n"
            f"時間：{self._ts()}"
        )

    def notify_semi_signal(
        self,
        symbol:     str,
        side:       str,
        price:      float,
        signal_id:  str,
        atr:        float = 0.0,
        upper:      float = 0.0,
        lower:      float = 0.0,
        expires_in: int   = 30,
    ) -> None:
        side_icon = "📈" if side == "BUY" else "📉"
        self._push(
            f"🎯 <b>SEMI_AUTO 訊號待確認</b>  {symbol}\n"
            f"{side_icon} 方向：{side}  價格：{price:.4f}\n"
            f"網格範圍：{lower:.4f} ~ {upper:.4f}  ATR：{atr:.4f}\n"
            f"請在 {expires_in}s 內至 UI 面板確認\n"
            f"ID：<code>{signal_id[:8]}…</code>  {self._ts()}"
        )


class AlertSystem:
    """發送交易訊號通知；若沒設定 Telegram token 則僅印 log。"""

    def __init__(self, use_telegram: bool = False) -> None:
        self.use_telegram = use_telegram
        self._token  = os.getenv("TELEGRAM_BOT_TOKEN", "")
        self._chat   = os.getenv("TELEGRAM_CHAT_ID", "")
        if use_telegram and (not self._token or not self._chat):
            log.warning("[Alert] Telegram token/chat_id 未設定，停用 Telegram")
            self.use_telegram = False

    # ── public API ───────────────────────────────────────────

    def bot_started(self, bot_name: str, symbols: list) -> None:
        self._send(f"🤖 [{bot_name}] 機器人已啟動  symbols={symbols}")

    def bot_stopped(self, bot_name: str) -> None:
        self._send(f"🛑 [{bot_name}] 機器人已停止")

    def signal_alert(
        self,
        symbol:        str,
        side:          str,               # "BUY" | "SELL"
        price:         float,
        strategy:      str   = "—",
        regime:        str   = "—",
        confidence:    float = 0.0,
        reason:        str   = "—",
        suggested_qty: Optional[float] = None,
        indicators:    Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        發送詳細多行訊號通知（Telegram + logger）。
        包含：幣種、方向、當前價格、策略名稱、市場狀態、
              信心指數、觸發原因、建議數量（可選）、指標摘要（可選）。
        """
        icon   = "🟢" if side == "BUY"  else "🔴"
        action = "買入" if side == "BUY" else "賣出"
        now    = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

        lines = [
            f"{icon}  ━━━ SQT 訊號通知 ━━━",
            f"📌 幣種     : {symbol}",
            f"💡 方向     : {action}（{side}）",
            f"💰 當前價格 : ${price:,.4f}",
            f"📊 策略名稱 : {strategy}",
            f"📈 市場狀態 : {regime}",
            f"🎯 信心指數 : {confidence:.0%}",
            f"📝 觸發原因 : {reason}",
        ]

        if suggested_qty is not None:
            lines.append(f"📦 建議數量 : {suggested_qty:.6f}")

        if indicators:
            # 只取前幾個主要指標避免訊息過長
            top = {k: indicators[k] for k in list(indicators)[:4]}
            ind_str = "  ".join(f"{k}={v:.2f}" if isinstance(v, float) else f"{k}={v}"
                                for k, v in top.items())
            lines.append(f"📉 指標摘要 : {ind_str}")

        lines += [
            f"🕐 時間     : {now}",
            f"━━━━━━━━━━━━━━━━━━━━━━",
            f"⚠️  請手動確認後於 Binance 下單",
        ]

        self._send("\n".join(lines))

    # 向後相容（舊介面保留，內部轉呼叫 signal_alert）
    def trade_signal(self, symbol: str, side: str, price: float, confidence: float = 0.0) -> None:
        self.signal_alert(symbol=symbol, side=side, price=price, confidence=confidence)

    def error(self, symbol: str, msg: str) -> None:
        self._send(f"⚠️ [{symbol}] 錯誤: {msg}")

    def custom(self, msg: str) -> None:
        self._send(msg)

    # ── internal ─────────────────────────────────────────────
    def _send(self, text: str) -> None:
        log.info(f"[Alert]\n{text}")
        if not self.use_telegram:
            return
        try:
            import requests  # type: ignore
            requests.post(
                f"https://api.telegram.org/bot{self._token}/sendMessage",
                json={"chat_id": self._chat, "text": text, "parse_mode": ""},
                timeout=5,
            )
        except Exception as e:
            log.warning(f"[Alert] Telegram 發送失敗: {e}")
