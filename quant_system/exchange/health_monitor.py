"""
quant_system/exchange/health_monitor.py — 系統健康監控

功能：
  1. API Ping 延遲量測（ms）
  2. API 請求權重（X-MBX-USED-WEIGHT-1M，每分鐘上限 1200）
  3. 最後資料更新時間戳記
  4. 資金費率查詢（現貨無費率，顯示 N/A；期貨顯示下次費率與時間）
  5. 帳戶餘額快照（Total Equity / Available / Locked）
"""
from __future__ import annotations
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

log = logging.getLogger(__name__)


@dataclass
class PingResult:
    latency_ms:    float    # 來回延遲（毫秒）
    server_time:   int      # Binance 伺服器時間（ms）
    time_diff_ms:  float    # 本機時間與伺服器時間差
    timestamp:     float = field(default_factory=time.time)


@dataclass
class WeightStatus:
    used_weight:   int      # 已使用權重
    max_weight:    int = 1200
    percent:       float = 0.0
    status:        str = "OK"    # OK / WARNING / CRITICAL
    timestamp:     float = field(default_factory=time.time)

    def __post_init__(self):
        self.percent = self.used_weight / self.max_weight * 100
        if self.percent >= 90:
            self.status = "CRITICAL"
        elif self.percent >= 70:
            self.status = "WARNING"
        else:
            self.status = "OK"


@dataclass
class BalanceSnapshot:
    total_equity_usdt:  float   # 總權益（USDT 計價）
    available_usdt:     float   # 可用餘額
    locked_usdt:        float   # 凍結保證金（掛單凍結）
    assets:             Dict[str, Dict] = field(default_factory=dict)
    timestamp:          float = field(default_factory=time.time)


@dataclass
class FundingInfo:
    symbol:             str
    rate:               float   # 費率（現貨為 0.0）
    next_funding_time:  Optional[int] = None   # Unix ms
    is_spot:            bool = True
    note:               str = "現貨交易無資金費率"


@dataclass
class HealthSnapshot:
    ping:       Optional[PingResult]    = None
    weight:     Optional[WeightStatus]  = None
    balance:    Optional[BalanceSnapshot] = None
    funding:    List[FundingInfo]       = field(default_factory=list)
    last_update: float                  = field(default_factory=time.time)
    errors:     List[str]               = field(default_factory=list)


class HealthMonitor:
    """
    系統健康監控器。
    設計為輕量、可選用（client=None 時回傳模擬資料）。
    """

    def __init__(self, client=None) -> None:
        self._client = client
        self._last_weight = WeightStatus(used_weight=0)
        self._cache_ttl   = 10.0    # 快取 10 秒，避免過度呼叫 API
        self._cache_ts    = 0.0
        self._cached:     Optional[HealthSnapshot] = None

    # ── 主入口 ───────────────────────────────────────────────
    def get_snapshot(self, force: bool = False) -> HealthSnapshot:
        """取得完整健康快照（含快取機制）。"""
        if not force and self._cached and (time.time() - self._cache_ts < self._cache_ttl):
            return self._cached

        snap   = HealthSnapshot()
        errors = []

        if self._client is None:
            snap.errors = ["未連線至 Binance（模擬模式）"]
            self._cached = snap
            return snap

        # Ping
        try:
            snap.ping = self._measure_ping()
        except Exception as e:
            errors.append(f"Ping 失敗: {e}")

        # 帳戶餘額
        try:
            snap.balance = self._get_balance()
        except Exception as e:
            errors.append(f"餘額查詢失敗: {e}")

        # API 權重（從最後一次 REST 呼叫的 headers 推斷）
        snap.weight = self._last_weight

        snap.errors     = errors
        snap.last_update = time.time()

        self._cached  = snap
        self._cache_ts = time.time()
        return snap

    # ── Ping ─────────────────────────────────────────────────
    def _measure_ping(self) -> PingResult:
        t0  = time.time()
        srv = self._client.get_server_time()
        t1  = time.time()
        latency = (t1 - t0) * 1000
        srv_ms  = srv.get("serverTime", 0)
        local_ms = t0 * 1000
        diff    = srv_ms - local_ms
        return PingResult(latency_ms=latency, server_time=srv_ms, time_diff_ms=diff)

    # ── 餘額 ─────────────────────────────────────────────────
    def _get_balance(self) -> BalanceSnapshot:
        acc = self._client.get_account()
        balances = acc.get("balances", [])

        assets = {}
        total_usdt    = 0.0
        available_usdt = 0.0
        locked_usdt   = 0.0

        # 取得所有有餘額的資產
        for b in balances:
            free   = float(b.get("free",   0))
            locked = float(b.get("locked", 0))
            asset  = b.get("asset", "")
            if free + locked < 1e-9:
                continue
            assets[asset] = {"free": free, "locked": locked}

        # USDT 直接計算
        usdt = assets.get("USDT", {})
        available_usdt = usdt.get("free",   0.0)
        locked_usdt    = usdt.get("locked", 0.0)

        # 其他幣種嘗試換算 USDT（最多查 5 個主流幣）
        main_coins = ["BTC", "ETH", "BNB", "SOL"]
        for coin in main_coins:
            if coin not in assets:
                continue
            try:
                ticker = self._client.get_symbol_ticker(symbol=f"{coin}USDT")
                px     = float(ticker.get("price", 0))
                total  = (assets[coin]["free"] + assets[coin]["locked"]) * px
                total_usdt    += total
                locked_usdt   += assets[coin]["locked"] * px
            except Exception:
                pass

        total_usdt += available_usdt + locked_usdt

        return BalanceSnapshot(
            total_equity_usdt  = round(total_usdt, 4),
            available_usdt     = round(available_usdt, 4),
            locked_usdt        = round(locked_usdt, 4),
            assets             = assets,
        )

    # ── 更新 API 權重（外部呼叫）────────────────────────────
    def update_weight(self, used: int) -> None:
        self._last_weight = WeightStatus(used_weight=used)

    # ── 取得活躍掛單 ─────────────────────────────────────────
    def get_open_orders(self, symbol: str = "") -> List[Dict]:
        if self._client is None:
            return []
        try:
            if symbol:
                orders = self._client.get_open_orders(symbol=symbol)
            else:
                orders = self._client.get_open_orders()
            return [
                {
                    "symbol":   o.get("symbol"),
                    "orderId":  str(o.get("orderId")),
                    "side":     o.get("side"),
                    "type":     o.get("type"),
                    "price":    float(o.get("price", 0)),
                    "origQty":  float(o.get("origQty", 0)),
                    "executedQty": float(o.get("executedQty", 0)),
                    "status":   o.get("status"),
                    "time":     datetime.fromtimestamp(
                                    o.get("time", 0) / 1000, tz=timezone.utc
                                ).strftime("%H:%M:%S"),
                }
                for o in orders
            ]
        except Exception as e:
            log.warning(f"[Health] 查詢掛單失敗: {e}")
            return []

    def cancel_all_orders(self, symbol: str) -> Tuple[bool, str]:
        if self._client is None:
            return False, "未連線"
        try:
            self._client.cancel_open_orders(symbol=symbol)
            return True, f"{symbol} 所有掛單已撤銷"
        except Exception as e:
            return False, str(e)
