"""
quant_system/risk/circuit_breaker.py — API 斷路器（Circuit Breaker）

設計邏輯：
  ① 滑動時間窗口追蹤（collections.deque）：只計算「最近 window_sec 秒內」的錯誤數
  ② 錯誤數超過 error_threshold → 斷路器「開路」(OPEN)，
     系統進入強制冷卻期 cooldown_sec 秒（預設 10 分鐘）
  ③ 冷卻期結束後自動進入「半開路」(HALF_OPEN)：
     下一次 API 呼叫成功則恢復，失敗則重新觸發
  ④ 所有方法均執行緒安全（threading.Lock）

使用方式：
  from quant_system.risk.circuit_breaker import get_circuit_breaker

  cb = get_circuit_breaker()

  # 在每次 API 呼叫前檢查
  ok, reason = cb.check()
  if not ok:
      log.warning(reason)
      return

  try:
      client.some_api_call()
      cb.record_success()   # 半開路時累積成功
  except BinanceAPIException as e:
      cb.record_error(detail=str(e))

狀態機：
  CLOSED → (errors >= threshold) → OPEN
  OPEN   → (cooldown 到期)       → HALF_OPEN
  HALF_OPEN → (success)          → CLOSED
  HALF_OPEN → (error)            → OPEN（重置冷卻計時器）
"""
from __future__ import annotations

import logging
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Optional, Tuple

log = logging.getLogger(__name__)

# ── 斷路器狀態常數 ────────────────────────────────────────────
STATE_CLOSED    = "CLOSED"     # 正常：允許所有請求
STATE_OPEN      = "OPEN"       # 熔斷：拒絕所有請求
STATE_HALF_OPEN = "HALF_OPEN"  # 探測：允許少量請求測試是否恢復


@dataclass
class ErrorEntry:
    timestamp: float
    detail:    str = ""


@dataclass
class CircuitStats:
    state:              str
    error_count_window: int    # 當前滑動窗口內的錯誤數
    total_errors:       int    # 累計錯誤數（重啟前）
    total_trips:        int    # 累計熔斷次數
    open_until:         Optional[float]   # 熔斷解除時間（None = 未熔斷）
    remaining_sec:      float  # 冷卻剩餘秒數（0 = 已解除）
    last_error_detail:  str


class CircuitBreaker:
    """
    API 斷路器。
    三態狀態機：CLOSED → OPEN → HALF_OPEN → CLOSED
    """

    def __init__(
        self,
        error_threshold: int   = 10,     # 觸發熔斷的錯誤次數上限
        window_sec:      float = 60.0,   # 滑動時間窗口（秒）
        cooldown_sec:    float = 600.0,  # 熔斷冷卻期（秒），預設 10 分鐘
        half_open_ok:    int   = 3,      # HALF_OPEN 需要多少次成功才恢復
    ) -> None:
        self._threshold   = error_threshold
        self._window      = window_sec
        self._cooldown    = cooldown_sec
        self._half_open_ok = half_open_ok

        self._lock         = threading.Lock()
        self._state:  str  = STATE_CLOSED
        self._errors: Deque[ErrorEntry] = deque()   # 滑動窗口內的錯誤記錄
        self._open_until:  float = 0.0
        self._total_errors: int  = 0
        self._total_trips:  int  = 0
        self._half_success: int  = 0    # HALF_OPEN 模式下的累積成功數
        self._last_detail:  str  = ""

    # ════════════════════════════════════════════════════════
    # 外部主要介面
    # ════════════════════════════════════════════════════════
    def check(self) -> Tuple[bool, str]:
        """
        API 呼叫前先呼叫 check()。
        回傳 (允許, 原因)。
          (True,  "")           → 可以繼續 API 呼叫
          (False, "<原因>")     → 斷路器開路，應中止呼叫
        """
        with self._lock:
            self._maybe_transition()
            if self._state == STATE_OPEN:
                remaining = max(0.0, self._open_until - time.time())
                reason = (
                    f"[CB] ⚡ 斷路器熔斷中，冷卻剩餘 {remaining:.0f}s "
                    f"（{remaining/60:.1f} 分鐘）"
                )
                return False, reason
            return True, ""

    def record_error(self, detail: str = "") -> bool:
        """
        記錄一次 API 錯誤。
        回傳 True = 本次錯誤觸發了新的熔斷；False = 尚未達閾值。
        """
        with self._lock:
            now = time.time()
            self._errors.append(ErrorEntry(timestamp=now, detail=detail))
            self._total_errors += 1
            self._last_detail = detail
            self._prune_window(now)

            error_count = len(self._errors)

            # HALF_OPEN 模式下任何錯誤立刻重新熔斷
            if self._state == STATE_HALF_OPEN:
                self._trip(now, reason="HALF_OPEN 探測失敗")
                return True

            # CLOSED 模式下檢查閾值
            if self._state == STATE_CLOSED and error_count >= self._threshold:
                self._trip(
                    now,
                    reason=f"{self._window:.0f}s 內累積 {error_count} 次錯誤（上限 {self._threshold}）",
                )
                return True

            log.debug(
                f"[CB] API 錯誤記錄 ({error_count}/{self._threshold}) detail={detail[:80]}"
            )
            return False

    def record_success(self) -> None:
        """
        記錄一次 API 呼叫成功（HALF_OPEN 模式下累積，達標後關閉斷路器）。
        CLOSED 模式下可選擇性呼叫（無副作用）。
        """
        with self._lock:
            if self._state == STATE_HALF_OPEN:
                self._half_success += 1
                if self._half_success >= self._half_open_ok:
                    self._state = STATE_CLOSED
                    self._half_success = 0
                    self._errors.clear()
                    log.info(
                        f"[CB] ✅ 斷路器已關閉（HALF_OPEN → CLOSED），"
                        f"連續 {self._half_open_ok} 次 API 成功"
                    )

    def reset(self) -> None:
        """手動強制關閉斷路器（管理員操作）。"""
        with self._lock:
            old_state = self._state
            self._state       = STATE_CLOSED
            self._open_until  = 0.0
            self._half_success = 0
            self._errors.clear()
        log.warning(f"[CB] 🔧 斷路器手動重置 {old_state} → CLOSED")

    # ════════════════════════════════════════════════════════
    # 查詢
    # ════════════════════════════════════════════════════════
    @property
    def is_open(self) -> bool:
        """快速查詢：True = 斷路器熔斷中（拒絕請求）。"""
        with self._lock:
            self._maybe_transition()
            return self._state == STATE_OPEN

    @property
    def state(self) -> str:
        with self._lock:
            self._maybe_transition()
            return self._state

    def stats(self) -> CircuitStats:
        with self._lock:
            self._maybe_transition()
            now = time.time()
            self._prune_window(now)
            remaining = max(0.0, self._open_until - now) if self._state == STATE_OPEN else 0.0
            return CircuitStats(
                state              = self._state,
                error_count_window = len(self._errors),
                total_errors       = self._total_errors,
                total_trips        = self._total_trips,
                open_until         = self._open_until if self._open_until > now else None,
                remaining_sec      = remaining,
                last_error_detail  = self._last_detail,
            )

    # ════════════════════════════════════════════════════════
    # 內部工具（需在 _lock 內呼叫）
    # ════════════════════════════════════════════════════════
    def _trip(self, now: float, reason: str) -> None:
        """觸發熔斷，進入 OPEN 狀態。"""
        self._state       = STATE_OPEN
        self._open_until  = now + self._cooldown
        self._half_success = 0
        self._total_trips += 1
        until_str = time.strftime("%H:%M:%S", time.localtime(self._open_until))
        log.critical(
            f"[CB] ⚡⚡⚡ 斷路器熔斷！原因: {reason}\n"
            f"      → 系統強制暫停 {self._cooldown/60:.0f} 分鐘，"
            f"解除時間: {until_str}\n"
            f"      → 最後錯誤: {self._last_detail[:120]}"
        )

    def _maybe_transition(self) -> None:
        """檢查是否應從 OPEN → HALF_OPEN（冷卻期到期）。"""
        if self._state == STATE_OPEN and time.time() >= self._open_until:
            self._state = STATE_HALF_OPEN
            self._half_success = 0
            log.warning(
                f"[CB] 冷卻期結束，進入 HALF_OPEN 模式，"
                f"需要 {self._half_open_ok} 次成功才能完全恢復"
            )

    def _prune_window(self, now: float) -> None:
        """移除滑動窗口外的舊錯誤記錄。"""
        cutoff = now - self._window
        while self._errors and self._errors[0].timestamp < cutoff:
            self._errors.popleft()


# ════════════════════════════════════════════════════════════
# 全域單例
# ════════════════════════════════════════════════════════════
_cb_instance:      Optional[CircuitBreaker] = None
_cb_instance_lock: threading.Lock           = threading.Lock()


def get_circuit_breaker(
    error_threshold: int   = 10,
    window_sec:      float = 60.0,
    cooldown_sec:    float = 600.0,
    half_open_ok:    int   = 3,
) -> CircuitBreaker:
    """
    取得全域 CircuitBreaker 單例。
    首次呼叫使用傳入的參數初始化；後續呼叫忽略參數，回傳同一實例。
    """
    global _cb_instance
    with _cb_instance_lock:
        if _cb_instance is None:
            _cb_instance = CircuitBreaker(
                error_threshold = error_threshold,
                window_sec      = window_sec,
                cooldown_sec    = cooldown_sec,
                half_open_ok    = half_open_ok,
            )
            log.debug(
                f"[CB] 斷路器初始化：threshold={error_threshold} "
                f"window={window_sec}s cooldown={cooldown_sec}s"
            )
    return _cb_instance
