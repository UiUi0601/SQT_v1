"""
quant_system/utils/rate_limiter.py — Token Bucket 速率限制器

設計基礎：
  Binance Spot API 每分鐘最高 1200 weight（X-MBX-USED-WEIGHT-1M）
  本模組限制在 1100 以內（預留 100 的安全邊際），
  以 20 token/sec 的速率持續補充，與 Binance 1200/60s 一致。

使用方式：
  from quant_system.utils.rate_limiter import rate_limiter

  # 一般輕量請求（weight=1）
  rate_limiter.consume(1)

  # 較重的請求（e.g. GET /api/v3/allOrders weight=10）
  rate_limiter.consume(10)

  # 不阻塞模式（不夠時拋出 RateLimitExceeded）
  rate_limiter.consume(5, block=False)
"""
from __future__ import annotations
import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Optional

log = logging.getLogger(__name__)


class RateLimitExceeded(Exception):
    """Token 不足且 block=False 時拋出。"""


@dataclass
class RateLimiterStats:
    available:   float = 0.0
    capacity:    int   = 1100
    requests:    int   = 0
    blocked:     int   = 0
    total_weight: int  = 0


class RateLimiter:
    """
    Token Bucket 速率限制器。

    參數：
      capacity     — 桶容量（預設 1100，留 100 緩衝給 Binance 1200 上限）
      refill_rate  — 每秒補充 token 數（預設 20.0 = 1200/60）
    """

    def __init__(
        self,
        capacity:    int   = 1100,
        refill_rate: float = 20.0,
    ) -> None:
        self._capacity    = capacity
        self._tokens      = float(capacity)
        self._refill_rate = refill_rate
        self._last_refill = time.monotonic()
        self._lock        = threading.Lock()

        # 統計
        self._requests    = 0
        self._blocked     = 0
        self._total_weight = 0

    # ── 內部補充 ─────────────────────────────────────────────
    def _refill(self) -> None:
        """根據距上次補充的時間差，補充對應 token（需在 lock 內呼叫）。"""
        now     = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(
            float(self._capacity),
            self._tokens + elapsed * self._refill_rate
        )
        self._last_refill = now

    # ── 主要消耗介面 ─────────────────────────────────────────
    def consume(self, weight: int = 1, block: bool = True) -> bool:
        """
        消耗指定 weight 的 token。

        block=True  → token 不足時等待直到可用（最多等 60s）。
        block=False → token 不足時拋出 RateLimitExceeded。

        回傳 True 表示成功消耗。
        """
        while True:
            with self._lock:
                self._refill()
                if self._tokens >= weight:
                    self._tokens -= weight
                    self._requests += 1
                    self._total_weight += weight
                    return True

                if not block:
                    self._blocked += 1
                    raise RateLimitExceeded(
                        f"速率限制：需要 {weight} tokens，"
                        f"目前僅剩 {self._tokens:.0f}，"
                        f"請稍後再試"
                    )

                # block=True：計算等待時間後離開 lock 再 sleep
                wait_sec = (weight - self._tokens) / self._refill_rate + 0.05
                self._blocked += 1

            # 在 lock 外 sleep，避免長時間持鎖
            wait_sec = min(wait_sec, 60.0)
            log.warning(
                f"[RateLimit] token 不足（需 {weight}，剩 {self._tokens:.0f}），"
                f"等待 {wait_sec:.2f}s"
            )
            time.sleep(wait_sec)

    # ── 屬性 ─────────────────────────────────────────────────
    @property
    def available(self) -> float:
        with self._lock:
            self._refill()
            return self._tokens

    @property
    def used_pct(self) -> float:
        """已使用比例（0–100），用於 UI 顯示。"""
        avail = self.available
        return (1.0 - avail / self._capacity) * 100.0

    def stats(self) -> RateLimiterStats:
        with self._lock:
            self._refill()
            return RateLimiterStats(
                available    = round(self._tokens, 1),
                capacity     = self._capacity,
                requests     = self._requests,
                blocked      = self._blocked,
                total_weight = self._total_weight,
            )

    def reset(self) -> None:
        """緊急重置：將 token 補滿（例如 IP 切換後重置計數器）。"""
        with self._lock:
            self._tokens      = float(self._capacity)
            self._last_refill = time.monotonic()
            log.info("[RateLimit] 計數器已重置")

    def __repr__(self) -> str:
        s = self.stats()
        return (
            f"RateLimiter(available={s.available}/{s.capacity}, "
            f"reqs={s.requests}, blocked={s.blocked})"
        )


# ════════════════════════════════════════════════════════════
# 全域單例（模組內唯一實例）
# ════════════════════════════════════════════════════════════
rate_limiter: RateLimiter = RateLimiter()
