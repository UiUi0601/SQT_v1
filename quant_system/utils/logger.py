"""
quant_system/utils/logger.py — 日誌工具（④ Phase G 強化版）

Phase G 新增：
  ④ Log Rotation — 改用 TimedRotatingFileHandler，
                   每天 00:00 UTC 自動切割備份日誌，
                   最多保留最近 14 天記錄，
                   舊日誌自動以 gzip 壓縮節省磁碟空間

日誌切割行為：
  logs/aqt.log               ← 當天日誌（活躍寫入）
  logs/aqt.log.2026-04-20.gz ← 前一天壓縮備份
  logs/aqt.log.2026-04-19.gz
  ...（最多保留 14 個 .gz 檔案）

注意事項：
  - 壓縮由自訂 rotator 函式完成（使用 Python 標準庫 gzip，無需額外依賴）
  - 日誌檔名格式：{base}.{YYYY-MM-DD}.gz
  - 若同一進程重複呼叫 setup_logger()（如 Streamlit 熱重載），
    判斷 logger.handlers 是否已存在，避免重複加 handler
"""
from __future__ import annotations

import gzip
import logging
import os
import shutil
from logging.handlers import TimedRotatingFileHandler


# ── gzip 壓縮 rotator（標準庫，無額外依賴）──────────────────
def _gzip_rotator(source: str, dest: str) -> None:
    """
    ④ 日誌切割後自動壓縮。

    `TimedRotatingFileHandler` 在切割時呼叫 `rotation_filename` 重命名，
    再呼叫 `rotator` 執行實際搬移。
    本函式：source（舊日誌）→ dest.gz（gzip 壓縮後刪除原檔）
    """
    with open(source, "rb") as f_in:
        with gzip.open(dest, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
    os.remove(source)


def _gzip_namer(default_name: str) -> str:
    """
    ④ 自訂備份檔名：追加 .gz 副檔名。

    example:
      logs/aqt.log.2026-04-20  →  logs/aqt.log.2026-04-20.gz
    """
    return default_name + ".gz"


def setup_logger(
    name:       str,
    log_file:   str  = "logs/aqt.log",
    level:      int  = logging.INFO,
    debug_mode: bool = False,
    # ④ Log Rotation 參數
    backup_count: int  = 14,    # 保留天數（預設 14 天）
    rotate_when:  str  = "midnight",  # 切割時間點
    rotate_utc:   bool = True,   # True=以 UTC 午夜切割，False=以本地時間
) -> logging.Logger:
    """
    建立或取得具名 Logger。

    Args:
        name         — Logger 名稱（通常為模組名或機器人名稱）
        log_file     — 日誌主檔路徑（自動建立目錄）
        level        — 日誌等級（debug_mode=True 時強制為 DEBUG）
        debug_mode   — 是否啟用 DEBUG 模式
        backup_count — ④ 保留備份天數（預設 14 天，超出自動刪除）
        rotate_when  — ④ 切割頻率（"midnight"=每天，"W0"=每週一，等同 logging 規格）
        rotate_utc   — ④ 是否以 UTC 時間切割（建議 True，避免日光節約時間問題）

    Returns:
        已設定好 handler 的 Logger 實例
    """
    log_dir = os.path.dirname(log_file)
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)

    logger = logging.getLogger(name)

    # 若 handler 已存在（如 Streamlit 熱重載），直接回傳，避免重複加 handler
    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG if debug_mode else level)

    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # ── ④ 檔案 Handler（每日切割 + gzip 壓縮）──────────────
    fh = TimedRotatingFileHandler(
        filename    = log_file,
        when        = rotate_when,
        backupCount = backup_count,
        encoding    = "utf-8",
        utc         = rotate_utc,
    )
    fh.setFormatter(fmt)
    fh.namer   = _gzip_namer    # 備份檔名加 .gz
    fh.rotator = _gzip_rotator  # 切割後 gzip 壓縮

    # ── 控制台 Handler ──────────────────────────────────────
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(sh)

    return logger
