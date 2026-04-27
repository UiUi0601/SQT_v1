"""
crypto_exchange.py — Binance 交易所客戶端（含 Rate Limiter）

安全規範：
  - API Key/Secret 必須放在 .env 檔案，程式碼中無任何明文金鑰
  - 建議在 Binance 後台將 API Key 綁定 IP 並關閉提現權限
  - Testnet / 正式環境由 BINANCE_TESTNET 環境變數切換

Rate Limiter：
  - 每個 API 呼叫前先向 TokenBucket 消耗對應 weight
  - 輕量查詢 (weight=1)、帳戶查詢 (weight=10)、所有掛單 (weight=40)
  - 若 token 不足會自動等待，不會對 Binance 發出超限請求

③ API 金鑰安全檢查（Phase G）：
  - __init__() 呼叫 _check_api_permissions()
  - 若偵測到「提現」權限開啟，立即拋出 SecurityError，阻止機器人啟動
  - Testnet 環境跳過此檢查（Testnet 無法查詢權限 API）
"""
import logging
import os
from typing import Any, Dict, Optional

from binance.client import Client
from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger(__name__)


class SecurityError(RuntimeError):
    """
    ③ API 金鑰安全違規錯誤。
    當偵測到危險權限（如提現）時拋出，強制中止機器人啟動。
    """

# ── Rate Limiter（延遲 import 以避免循環依賴）────────────────
try:
    from quant_system.utils.rate_limiter import rate_limiter as _rl
except ImportError:
    _rl = None  # 若模組尚未安裝，退化為無限制模式


def _consume(weight: int = 1) -> None:
    """消耗 API weight，若 rate_limiter 不可用則靜默跳過。"""
    if _rl is not None:
        _rl.consume(weight, block=True)


class ExchangeClient:
    """
    Binance 交易所封裝。

    自動從 .env 讀取金鑰，支援 Testnet/正式環境切換，
    所有 API 呼叫皆先通過 Rate Limiter。
    """

    def __init__(self) -> None:
        api_key    = os.getenv("BINANCE_API_KEY", "").strip()
        api_secret = os.getenv("BINANCE_API_SECRET", "").strip()
        testnet    = os.getenv("BINANCE_TESTNET", "true").lower() == "true"

        # ── 安全檢查：拒絕明文金鑰殘留 ─────────────────────
        if not api_key or not api_secret:
            raise ValueError(
                "缺少 Binance API 金鑰！\n"
                "請在 .env 檔案中設定：\n"
                "  BINANCE_API_KEY=your_key\n"
                "  BINANCE_API_SECRET=your_secret\n"
                "（勿將金鑰直接寫入程式碼）"
            )

        self.client  = Client(
            api_key, api_secret,
            testnet=testnet,
            requests_params={"timeout": 10},   # ③ 防止 HTTP 請求無限掛起
        )
        self.testnet = testnet
        env_label    = "Testnet ⚗️" if testnet else "正式環境 🔴"
        log.info(f"[Exchange] 已連線 Binance {env_label}")

        # ③ API 金鑰安全檢查：若開啟提現權限則拒絕啟動
        if not testnet:
            self._check_api_permissions()

    # ── 帳戶 (weight=10) ─────────────────────────────────────
    def get_usdt_balance(self) -> float:
        _consume(10)
        acc = self.client.get_account()
        for b in acc.get("balances", []):
            if b["asset"] == "USDT":
                return float(b["free"])
        return 0.0

    def get_asset_balance(self, asset: str) -> float:
        _consume(10)
        acc = self.client.get_account()
        for b in acc.get("balances", []):
            if b["asset"] == asset.upper():
                return float(b["free"])
        return 0.0

    def get_account_snapshot(self) -> Dict:
        """回傳帳戶完整餘額快照（含所有幣種）。"""
        _consume(10)
        return self.client.get_account()

    # ── 行情 (weight=1) ──────────────────────────────────────
    def get_price(self, symbol: str) -> float:
        _consume(1)
        try:
            ticker = self.client.get_symbol_ticker(symbol=symbol)
            return float(ticker["price"])
        except Exception as e:
            log.warning(f"[Exchange] 取價失敗 {symbol}: {e}")
            return 0.0

    def get_prices_bulk(self, symbols: list) -> Dict[str, float]:
        """批次取得多幣種最新價格（單次請求 weight=2）。"""
        _consume(2)
        try:
            all_tickers = self.client.get_all_tickers()
            ticker_map  = {t["symbol"]: float(t["price"]) for t in all_tickers}
            return {s: ticker_map.get(s, 0.0) for s in symbols}
        except Exception as e:
            log.warning(f"[Exchange] 批次取價失敗: {e}")
            return {s: 0.0 for s in symbols}

    # ── 下單 (weight=1) ──────────────────────────────────────
    def market_buy(self, symbol: str, quantity: float) -> bool:
        """市價買入，quantity 為幣種數量（e.g. 0.001 BTC）。"""
        _consume(1)
        try:
            self.client.order_market_buy(symbol=symbol, quantity=round(quantity, 6))
            log.info(f"[Exchange] 市價買入 {symbol} qty={quantity:.6f}")
            return True
        except Exception as e:
            log.error(f"[Exchange] BUY 失敗 {symbol}: {e}")
            return False

    def market_sell(self, symbol: str, quantity: float) -> bool:
        """市價賣出，quantity 為幣種數量。"""
        _consume(1)
        try:
            self.client.order_market_sell(symbol=symbol, quantity=round(quantity, 6))
            log.info(f"[Exchange] 市價賣出 {symbol} qty={quantity:.6f}")
            return True
        except Exception as e:
            log.error(f"[Exchange] SELL 失敗 {symbol}: {e}")
            return False

    def limit_buy(
        self, symbol: str, quantity: float, price: float, time_in_force: str = "GTC"
    ) -> Optional[Dict]:
        """限價買單，回傳 Binance 訂單 dict 或 None。"""
        _consume(1)
        try:
            order = self.client.order_limit_buy(
                symbol=symbol,
                quantity=round(quantity, 6),
                price=str(round(price, 8)),
                timeInForce=time_in_force,
            )
            log.info(
                f"[Exchange] 限價買入 {symbol} qty={quantity:.6f} @ {price:.4f} "
                f"orderId={order.get('orderId')}"
            )
            return order
        except Exception as e:
            log.error(f"[Exchange] LIMIT BUY 失敗 {symbol}: {e}")
            return None

    def limit_sell(
        self, symbol: str, quantity: float, price: float, time_in_force: str = "GTC"
    ) -> Optional[Dict]:
        """限價賣單，回傳 Binance 訂單 dict 或 None。"""
        _consume(1)
        try:
            order = self.client.order_limit_sell(
                symbol=symbol,
                quantity=round(quantity, 6),
                price=str(round(price, 8)),
                timeInForce=time_in_force,
            )
            log.info(
                f"[Exchange] 限價賣出 {symbol} qty={quantity:.6f} @ {price:.4f} "
                f"orderId={order.get('orderId')}"
            )
            return order
        except Exception as e:
            log.error(f"[Exchange] LIMIT SELL 失敗 {symbol}: {e}")
            return None

    # ── 掛單管理 (weight=1~40) ────────────────────────────────
    def cancel_order(self, symbol: str, order_id: str) -> bool:
        """撤銷指定訂單。"""
        _consume(1)
        try:
            self.client.cancel_order(symbol=symbol, orderId=order_id)
            return True
        except Exception as e:
            log.error(f"[Exchange] 撤單失敗 {symbol} {order_id}: {e}")
            return False

    def cancel_all_orders(self, symbol: str) -> bool:
        """撤銷某幣種所有掛單 (weight=1)。"""
        _consume(1)
        try:
            self.client.cancel_open_orders(symbol=symbol)
            return True
        except Exception as e:
            log.error(f"[Exchange] 撤全部掛單失敗 {symbol}: {e}")
            return False

    def get_open_orders(self, symbol: str = "") -> list:
        """取得掛單（含 symbol 時 weight=3，不含時 weight=40）。"""
        _consume(3 if symbol else 40)
        try:
            if symbol:
                return self.client.get_open_orders(symbol=symbol)
            return self.client.get_open_orders()
        except Exception as e:
            log.error(f"[Exchange] 查詢掛單失敗: {e}")
            return []

    # ── ③ API 金鑰安全檢查 ────────────────────────────────────
    def _check_api_permissions(self) -> None:
        """
        ③ 啟動時驗證 API 金鑰權限安全性。

        呼叫 GET /sapi/v1/account/apiRestrictions，檢查以下危險權限：
          - enableWithdrawals     — 絕對不允許：提現權限一旦洩漏即可轉走資金
          - enableInternalTransfer — 強烈建議關閉：Binance 站內轉帳

        若偵測到危險權限，立即拋出 SecurityError，阻止機器人啟動。

        注意：
          - 此 API 需要 SIGNED 請求（python-binance 自動處理）
          - Testnet 環境不支援此 API，已在 __init__() 中跳過
          - 若 API 查詢本身失敗（如網路問題），發出警告但不阻止啟動
            （避免因臨時網路問題導致機器人無法重啟）
        """
        try:
            _consume(1)
            restrictions = self.client.get_api_key_permission()
            enable_withdraw  = restrictions.get("enableWithdrawals",        False)
            enable_transfer  = restrictions.get("enableInternalTransfer",   False)
            ip_restrict      = restrictions.get("ipRestrict",               False)

            log.info(
                f"[Exchange] ③ API 權限檢查 — "
                f"提現={enable_withdraw} 站內轉帳={enable_transfer} IP限制={ip_restrict}"
            )

            # ⛔ 提現權限開啟 → 強制中止
            if enable_withdraw:
                raise SecurityError(
                    "🚨 危險！API 金鑰開啟了「提現（Withdrawals）」權限！\n"
                    "  一旦金鑰洩漏，資金將直接被轉走。\n"
                    "  請立即至 Binance API 管理頁面關閉提現權限後重啟機器人。\n"
                    "  ➡ https://www.binance.com/zh-TW/my/settings/api-management"
                )

            # ⚠️ 站內轉帳開啟 → 警告但不阻止
            if enable_transfer:
                log.warning(
                    "[Exchange] ⚠️  API 金鑰開啟了「站內轉帳」權限，"
                    "建議在 Binance 後台關閉以降低風險。"
                )

            # 💡 未綁定 IP → 建議
            if not ip_restrict:
                log.warning(
                    "[Exchange] 💡 API 金鑰未綁定 IP，"
                    "建議在 Binance 後台設定「限制存取 IP」。"
                )

        except SecurityError:
            raise   # 直接向上拋出，阻止啟動
        except Exception as e:
            # API 查詢失敗（如網路問題、Testnet 不支援等）→ 警告但不阻止
            log.warning(
                f"[Exchange] ③ API 權限查詢失敗（不影響啟動）: {e}"
            )

    # ── Rate Limiter 統計 ─────────────────────────────────────
    def rate_limiter_stats(self) -> Dict:
        """回傳當前 Rate Limiter 統計（用於 UI 健康儀表板）。"""
        if _rl is None:
            return {"available": 1100, "capacity": 1100, "requests": 0, "blocked": 0}
        s = _rl.stats()
        return {
            "available":     s.available,
            "capacity":      s.capacity,
            "requests":      s.requests,
            "blocked":       s.blocked,
            "total_weight":  s.total_weight,
        }
