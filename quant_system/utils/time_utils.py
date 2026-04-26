"""
quant_system/utils/time_utils.py — 全域時間工具（④ 時區標準化）

設計原則：
  ─ 系統內部所有時間以「毫秒 Unix 時間戳（int）」表示
  ─ 所有 DB 存入、API 傳送均使用 UTC ISO-8601 字串
  ─ 僅在 UI 呈現時才轉換成本地時間字串（fmt_local）
  ─ 禁止在業務邏輯中直接呼叫 datetime.now()（無時區）或 time.time()（秒）

提供函數：
  now_ms()            → int        當前 UTC 毫秒時間戳
  now_iso()           → str        當前 UTC ISO-8601 字串（DB 寫入用）
  now_sec()           → float      當前 UTC 秒（與 time.time() 等效，供相容用）
  ms_to_dt(ms)        → datetime   毫秒 → UTC aware datetime
  ms_to_iso(ms)       → str        毫秒 → UTC ISO-8601 字串
  ms_to_sec(ms)       → float      毫秒 → 秒（浮點）
  sec_to_ms(sec)      → int        秒 → 毫秒
  iso_to_ms(iso)      → int        ISO-8601 字串 → 毫秒
  fmt_local(ms, fmt)  → str        ⚠ 僅 UI 用：毫秒 → 本地時間字串
  fmt_utc(ms, fmt)    → str        ⚠ 僅 UI 用：毫秒 → UTC 時間字串
  age_ms(ms)          → int        距現在多少毫秒（ms 比現在早則為正）

使用範例：
  from quant_system.utils.time_utils import now_ms, now_iso, fmt_local

  # DB 寫入
  closed_at = now_iso()

  # 計算超時
  age = age_ms(order_created_ms)
  if age > 300_000:   # 超過 5 分鐘
      ...

  # UI 顯示（只在 Streamlit / 日誌格式化 時使用）
  display = fmt_local(order_created_ms)
"""
from __future__ import annotations

import time
from datetime import datetime, timezone, timedelta
from typing import Optional

# ── 日期格式常數 ─────────────────────────────────────────────
FMT_DEFAULT  = "%Y-%m-%d %H:%M:%S"      # 不含毫秒
FMT_PRECISE  = "%Y-%m-%d %H:%M:%S.%f"   # 含微秒
FMT_DATE     = "%Y-%m-%d"
FMT_TIME     = "%H:%M:%S"
FMT_LOG      = "%m/%d %H:%M:%S"         # 短格式日誌


# ════════════════════════════════════════════════════════════
# 取得當前時間
# ════════════════════════════════════════════════════════════

def now_ms() -> int:
    """
    當前 UTC 毫秒時間戳。
    所有業務邏輯、訂單 ID、超時計算均應使用此函數。

    >>> now_ms() > 1_700_000_000_000
    True
    """
    return int(time.time() * 1000)


def now_iso() -> str:
    """
    當前 UTC 時間的 ISO-8601 字串（含時區後綴）。
    用於寫入 SQLite 的所有時間欄位。

    >>> now_iso().endswith('+00:00')
    True
    """
    return datetime.now(timezone.utc).isoformat()


def now_sec() -> float:
    """
    當前 UTC 秒（浮點）。等同 time.time()，提供語意化別名。
    """
    return time.time()


# ════════════════════════════════════════════════════════════
# 毫秒 ↔ 其他格式
# ════════════════════════════════════════════════════════════

def ms_to_dt(ms: int) -> datetime:
    """
    毫秒 Unix 時間戳 → UTC aware datetime。

    >>> ms_to_dt(0).year
    1970
    """
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)


def ms_to_iso(ms: int) -> str:
    """
    毫秒 → UTC ISO-8601 字串（DB 可直接儲存）。

    >>> ms_to_iso(0)
    '1970-01-01T00:00:00+00:00'
    """
    return ms_to_dt(ms).isoformat()


def ms_to_sec(ms: int) -> float:
    """毫秒 → 秒（浮點）。"""
    return ms / 1000.0


def sec_to_ms(sec: float) -> int:
    """秒 → 毫秒（整數）。"""
    return int(sec * 1000)


def iso_to_ms(iso: str) -> int:
    """
    ISO-8601 字串 → 毫秒 Unix 時間戳。
    支援帶 timezone（+00:00 / Z）與不帶 timezone 的格式（預設 UTC）。

    >>> iso_to_ms('1970-01-01T00:00:00+00:00')
    0
    """
    iso = iso.strip()
    # 處理 Z 後綴
    if iso.endswith("Z"):
        iso = iso[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(iso)
    except ValueError:
        # 相容舊版 Python：手動解析常見格式
        for fmt in (FMT_DEFAULT, FMT_PRECISE, FMT_DATE):
            try:
                dt = datetime.strptime(iso, fmt).replace(tzinfo=timezone.utc)
                break
            except ValueError:
                continue
        else:
            raise ValueError(f"無法解析時間字串: {iso!r}")

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def age_ms(ts_ms: int) -> int:
    """
    計算 ts_ms 距今的毫秒數（ts_ms 比現在早則為正數）。

    常用於訂單超時判斷：
      if age_ms(order.created_ms) > 300_000:  # 5 分鐘
          cancel(order)
    """
    return now_ms() - ts_ms


# ════════════════════════════════════════════════════════════
# ⚠ UI 專用格式化（業務邏輯禁用）
# ════════════════════════════════════════════════════════════

def fmt_local(
    ms:  int,
    fmt: str = FMT_DEFAULT,
    tz_offset_hours: Optional[float] = None,
) -> str:
    """
    ⚠ 僅用於 UI 顯示（Streamlit / 日誌）。

    毫秒 → 本地時間字串。
    tz_offset_hours=None 時使用系統本地時區（time.localtime）。
    tz_offset_hours=8    → Asia/Taipei（UTC+8）。

    注意：業務邏輯（超時計算、DB 寫入、訂單 ID）請勿使用此函數。
    """
    if tz_offset_hours is not None:
        tz  = timezone(timedelta(hours=tz_offset_hours))
        dt  = ms_to_dt(ms).astimezone(tz)
        return dt.strftime(fmt)
    # 使用系統本地時區
    local_dt = datetime.fromtimestamp(ms / 1000.0)
    return local_dt.strftime(fmt)


def fmt_utc(ms: int, fmt: str = FMT_DEFAULT) -> str:
    """
    ⚠ 僅用於 UI 顯示。

    毫秒 → UTC 時間字串。
    """
    return ms_to_dt(ms).strftime(fmt)


def fmt_age(ms: int) -> str:
    """
    ⚠ 僅用於 UI 顯示。

    將距今毫秒數格式化為人性化字串，例如：
      "3s 前" / "2m 15s 前" / "1h 30m 前"
    """
    age = age_ms(ms)
    if age < 0:
        return "未來"
    secs = age // 1000
    if secs < 60:
        return f"{secs}s 前"
    mins, secs = divmod(secs, 60)
    if mins < 60:
        return f"{mins}m {secs}s 前"
    hours, mins = divmod(mins, 60)
    if hours < 24:
        return f"{hours}h {mins}m 前"
    days, hours = divmod(hours, 24)
    return f"{days}d {hours}h 前"


# ════════════════════════════════════════════════════════════
# 常數：常用時間長度（毫秒）
# ════════════════════════════════════════════════════════════
ONE_SECOND_MS  = 1_000
ONE_MINUTE_MS  = 60_000
ONE_HOUR_MS    = 3_600_000
ONE_DAY_MS     = 86_400_000
ONE_WEEK_MS    = 604_800_000
