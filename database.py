"""
database.py — AQT_v1 SQLite 持久層（強化版 v3）

① DB Writer Thread（本版新增）：
   ─ 所有 INSERT / UPDATE / DELETE 透過 queue.Queue 排入單一寫入執行緒
   ─ 寫入執行緒持有長生命週期連線（避免每次開連線的開銷）
   ─ 讀取（SELECT）仍走獨立 WAL 連線，允許多讀並發
   ─ 外部呼叫 submit(fn).result() 可同步等待；僅傳 submit(fn) 則為非阻塞
   ─ TradeDB.flush() / TradeDB.stop() 供優雅關閉時確保所有寫入完成

WAL 保留：
   ─ WAL 模式在讀取路徑仍開啟，讀取永遠不阻塞寫入
   ─ busy_timeout / synchronous=NORMAL 維持原設定

表格：
  bot_config / trades / positions / paper_account / paper_positions
  paper_trades / paper_equity_log / grid_config / grid_levels / grid_trades
"""
from __future__ import annotations

import concurrent.futures
import logging
import os
import queue
import sqlite3
import threading
import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

import pandas as pd

log = logging.getLogger(__name__)

DB_PATH      = os.getenv("DB_PATH", "trading_bot.db")
_TIMEOUT     = 30      # 讀取連線等待鎖定最長秒數
_WRITE_TIMEOUT = 15    # submit().result() 同步等待秒數（防止業務邏輯永久阻塞）
_MAX_RETRY   = 3
_RETRY_DELAY = 0.5


# ════════════════════════════════════════════════════════════
# 讀取連線（每次呼叫建新連線，WAL 模式允許多讀並發）
# ════════════════════════════════════════════════════════════
def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(DB_PATH, timeout=_TIMEOUT, check_same_thread=False)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA journal_mode=WAL")
    c.execute(f"PRAGMA busy_timeout={int(_TIMEOUT * 1000)}")
    c.execute("PRAGMA synchronous=NORMAL")
    return c


def _now() -> str:
    """UTC ISO-8601 時間字串，供 DB 欄位寫入使用。"""
    return datetime.now(timezone.utc).isoformat()


# ════════════════════════════════════════════════════════════
# ① DB Writer Thread
# ════════════════════════════════════════════════════════════
_SENTINEL = object()   # 終止訊號
_FLUSH    = object()   # 強制 commit + 通知 flush() 呼叫端

class _DBWriter:
    """
    單一寫入執行緒，持有唯一的 SQLite 寫入連線。

    所有 INSERT / UPDATE / DELETE 操作以 Callable[[sqlite3.Connection], Any]
    形式提交至 queue.Queue，由此執行緒循序執行。

    設計要點：
      ─ 寫入連線使用 isolation_level=None（autocommit 關閉，
        由 writer 自行 commit/rollback）
      ─ queue 為無界（unbounded）SimpleQueue，非阻塞 put
      ─ 每個任務回傳 concurrent.futures.Future，呼叫端可選擇
        .result(timeout) 等待或直接忽略（fire-and-forget）
      ─ 批次提交：每 _BATCH 筆或每 _BATCH_INTERVAL 秒 commit 一次，
        減少 fsync 次數，提升吞吐量
    """

    _BATCH          = 50      # 每 N 筆 commit 一次
    _BATCH_INTERVAL = 0.2     # 最多等 200ms 再 commit（避免寫入延遲過高）

    def __init__(self, db_path: str) -> None:
        self._db_path  = db_path
        self._queue: queue.SimpleQueue = queue.SimpleQueue()
        self._running  = True
        self._thread   = threading.Thread(
            target = self._loop,
            name   = "db-writer",
            daemon = True,
        )
        self._thread.start()
        log.debug("[DBWriter] 寫入執行緒已啟動")

    # ── 提交寫入任務 ─────────────────────────────────────────
    def submit(
        self,
        fn: Callable[[sqlite3.Connection], Any],
    ) -> concurrent.futures.Future:
        """
        提交一個寫入任務。

        fn(conn) — 接收寫入連線，執行 SQL，回傳任意值。
        回傳 Future；呼叫 .result(timeout=N) 可同步等待。
        """
        fut: concurrent.futures.Future = concurrent.futures.Future()
        self._queue.put((fn, fut))
        return fut

    # ── 等待佇列清空（含強制 commit）────────────────────────
    def flush(self, timeout: float = 30.0) -> bool:
        """
        阻塞直到佇列中所有已提交的寫入完成（含 commit 落盤）。
        使用 _FLUSH 特殊標記，在 _loop 中 commit 完成後才通知 Event。
        回傳 True = 成功清空；False = 超時。
        """
        evt = threading.Event()
        self._queue.put((_FLUSH, evt))     # _FLUSH 標記 + Event
        return evt.wait(timeout)

    # ── 優雅停止 ─────────────────────────────────────────────
    def stop(self, timeout: float = 30.0) -> None:
        """
        先 flush 再停止執行緒。
        應在程序退出前呼叫，確保所有寫入落盤。
        """
        self.flush(timeout=timeout / 2)
        self._running = False
        self._queue.put(_SENTINEL)
        self._thread.join(timeout=timeout / 2)
        log.info("[DBWriter] 寫入執行緒已停止")

    # ── 主迴圈 ───────────────────────────────────────────────
    def _loop(self) -> None:
        conn = self._make_conn()
        pending   = 0
        last_commit = time.monotonic()

        while True:
            # 取出下一個任務（非阻塞：get_nowait；超時則 commit 積累批次）
            try:
                item = self._queue.get(timeout=self._BATCH_INTERVAL)
            except queue.Empty:
                if pending > 0:
                    self._safe_commit(conn)
                    pending = 0
                    last_commit = time.monotonic()
                continue

            # ── 終止訊號 ─────────────────────────────────────
            if item is _SENTINEL:
                if pending > 0:
                    self._safe_commit(conn)
                break

            # ── Flush 標記：強制 commit 後通知呼叫端 ─────────
            if isinstance(item, tuple) and len(item) == 2 and item[0] is _FLUSH:
                _, flush_evt = item
                if pending > 0:
                    self._safe_commit(conn)
                    pending = 0
                    last_commit = time.monotonic()
                flush_evt.set()   # commit 已完成，才通知 flush() 呼叫端
                continue

            fn, fut = item

            try:
                result = fn(conn)
                pending += 1
                # 批次提交邏輯
                elapsed = time.monotonic() - last_commit
                if pending >= self._BATCH or elapsed >= self._BATCH_INTERVAL:
                    self._safe_commit(conn)
                    pending   = 0
                    last_commit = time.monotonic()
                fut.set_result(result)
            except Exception as e:
                # 用 SQL 指令 ROLLBACK+BEGIN，確保 isolation_level=None 模式下
                # 事務狀態一致（conn.rollback() 不會自動開啟新 BEGIN）
                try:
                    conn.execute("ROLLBACK")
                except Exception:
                    pass
                try:
                    conn.execute("BEGIN")
                except Exception:
                    pass
                pending = 0
                log.error(f"[DBWriter] 寫入失敗，已回滾: {e}", exc_info=True)
                fut.set_exception(e)

        conn.close()

    def _make_conn(self) -> sqlite3.Connection:
        """建立寫入專用連線（手動 commit，不使用 context manager）。"""
        c = sqlite3.connect(
            self._db_path,
            timeout            = _TIMEOUT,
            check_same_thread  = True,    # 寫入連線只給 writer thread 用
            isolation_level    = None,    # autocommit off；由我們手動 commit
        )
        c.row_factory = sqlite3.Row
        c.execute("PRAGMA journal_mode=WAL")
        c.execute(f"PRAGMA busy_timeout={int(_TIMEOUT * 1000)}")
        c.execute("PRAGMA synchronous=NORMAL")
        c.execute("BEGIN")               # 開啟第一個事務
        return c

    @staticmethod
    def _safe_commit(conn: sqlite3.Connection) -> None:
        # COMMIT 與 BEGIN 分開 try/except，確保即使 COMMIT 失敗也能重啟 BEGIN
        try:
            conn.execute("COMMIT")
        except sqlite3.OperationalError as e:
            err_lower = str(e).lower()
            if "no transaction is active" in err_lower or "cannot commit" in err_lower:
                # 正常邊界情況：空事務（無寫入卻觸發 commit），靜默忽略
                pass
            else:
                # 非預期錯誤才記錄警告
                log.warning(f"[DBWriter] commit 失敗: {e}")
                try:
                    conn.execute("ROLLBACK")
                except Exception:
                    pass
        # 無論 COMMIT 成功與否，始終重新開啟一個事務
        try:
            conn.execute("BEGIN")
        except Exception as e:
            log.error(f"[DBWriter] BEGIN 失敗，寫入可能進入 autocommit 狀態: {e}")


# ════════════════════════════════════════════════════════════
# 全域 Writer 單例（lazy init）
# ════════════════════════════════════════════════════════════
_writer: Optional[_DBWriter] = None
_writer_lock = threading.Lock()

def _get_writer() -> _DBWriter:
    global _writer
    if _writer is None:
        with _writer_lock:
            if _writer is None:
                _writer = _DBWriter(DB_PATH)
    return _writer


def _w(fn: Callable[[sqlite3.Connection], Any]) -> concurrent.futures.Future:
    """快捷方式：提交寫入任務到全域 writer。"""
    return _get_writer().submit(fn)


# ════════════════════════════════════════════════════════════
# TradeDB
# ════════════════════════════════════════════════════════════
class TradeDB:
    """
    AQT_v1 SQLite 持久層。

    讀取方法（SELECT）：直接使用 _conn()，WAL 允許多執行緒並發讀取。
    寫入方法（INSERT/UPDATE/DELETE）：透過 _DBWriter 佇列，
      由單一執行緒循序執行，從根本上消除「database is locked」問題。
    """

    def __init__(self, db_path: str = DB_PATH) -> None:
        global DB_PATH
        DB_PATH = db_path
        self._init_tables()

    # ── 優雅關閉介面 ─────────────────────────────────────────
    def flush(self, timeout: float = 30.0) -> bool:
        """等待所有已提交的寫入完成（優雅關閉前呼叫）。"""
        return _get_writer().flush(timeout)

    def stop(self, timeout: float = 30.0) -> None:
        """停止 Writer Thread（程序退出時呼叫）。"""
        _get_writer().stop(timeout)

    # ════════════════════════════════════════════════════════
    # 初始化（表格建立為一次性寫入，同步執行）
    # ════════════════════════════════════════════════════════
    def _init_tables(self) -> None:
        def _create(conn):
            conn.executescript("""
            CREATE TABLE IF NOT EXISTS bot_config (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                bot_type     TEXT NOT NULL,
                symbol       TEXT NOT NULL,
                display_name TEXT NOT NULL,
                quantity     REAL DEFAULT 0.001,
                sl_pct       REAL DEFAULT 0.02,
                tp_pct       REAL DEFAULT 0.04,
                trailing_stop_pct REAL DEFAULT 0.015,
                enabled      INTEGER DEFAULT 1,
                created_at   TEXT DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS trades (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol       TEXT NOT NULL,
                side         TEXT NOT NULL,
                price        REAL NOT NULL,
                quantity     REAL NOT NULL,
                strategy     TEXT DEFAULT '',
                timestamp    TEXT DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS positions (
                symbol       TEXT PRIMARY KEY,
                entry_price  REAL NOT NULL,
                quantity     REAL NOT NULL,
                strategy     TEXT DEFAULT '',
                opened_at    TEXT DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS paper_account (
                id           INTEGER PRIMARY KEY CHECK (id = 1),
                cash         REAL NOT NULL DEFAULT 100000.0,
                initial_cash REAL NOT NULL DEFAULT 100000.0,
                updated_at   TEXT
            );
            INSERT OR IGNORE INTO paper_account (id, cash, initial_cash, updated_at)
            VALUES (1, 100000.0, 100000.0, CURRENT_TIMESTAMP);

            CREATE TABLE IF NOT EXISTS paper_positions (
                symbol       TEXT PRIMARY KEY,
                entry_price  REAL NOT NULL,
                quantity     REAL NOT NULL,
                sl_price     REAL DEFAULT 0,
                tp_price     REAL DEFAULT 0,
                strategy     TEXT DEFAULT '',
                opened_at    TEXT
            );

            CREATE TABLE IF NOT EXISTS paper_trades (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol       TEXT NOT NULL,
                side         TEXT NOT NULL,
                price        REAL NOT NULL,
                adj_price    REAL NOT NULL,
                quantity     REAL NOT NULL,
                fee          REAL DEFAULT 0,
                pnl          REAL DEFAULT 0,
                reason       TEXT DEFAULT '',
                strategy     TEXT DEFAULT '',
                timestamp    TEXT
            );

            CREATE TABLE IF NOT EXISTS paper_equity_log (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                total_value  REAL NOT NULL,
                timestamp    TEXT
            );

            CREATE TABLE IF NOT EXISTS grid_config (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol          TEXT NOT NULL UNIQUE,
                k               REAL DEFAULT 0.5,
                m               REAL DEFAULT 3.0,
                atr_period      INTEGER DEFAULT 14,
                ema_period      INTEGER DEFAULT 20,
                qty_per_grid    REAL DEFAULT 0.001,
                capital         REAL DEFAULT 1000.0,
                status          TEXT DEFAULT 'STOPPED',
                last_regrid     TEXT,
                created_at      TEXT,
                updated_at      TEXT,
                trade_mode      TEXT DEFAULT 'auto',
                upper_price     REAL DEFAULT 0,
                lower_price     REAL DEFAULT 0,
                grid_count      INTEGER DEFAULT 10,
                stop_loss_price REAL DEFAULT 0,
                direction       TEXT DEFAULT '做多 (Long)'
            );

            CREATE TABLE IF NOT EXISTS grid_levels (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                grid_id      INTEGER NOT NULL,
                symbol       TEXT NOT NULL,
                level_index  INTEGER NOT NULL,
                price        REAL NOT NULL,
                side         TEXT NOT NULL,
                order_id     TEXT DEFAULT '',
                status       TEXT DEFAULT 'PENDING',
                filled_price REAL DEFAULT 0,
                filled_qty   REAL DEFAULT 0,
                created_at   TEXT,
                filled_at    TEXT
            );

            CREATE TABLE IF NOT EXISTS grid_trades (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol       TEXT NOT NULL,
                buy_price    REAL NOT NULL,
                sell_price   REAL NOT NULL,
                qty          REAL NOT NULL,
                gross_pnl    REAL NOT NULL,
                fee          REAL DEFAULT 0,
                net_pnl      REAL NOT NULL,
                closed_at    TEXT
            );
            """)

            # ── 遷移：為舊版 grid_config 自動補齊新欄位 ──────────
            for _col_sql in [
                "ALTER TABLE grid_config ADD COLUMN trade_mode TEXT DEFAULT 'auto'",
                "ALTER TABLE grid_config ADD COLUMN upper_price REAL DEFAULT 0",
                "ALTER TABLE grid_config ADD COLUMN lower_price REAL DEFAULT 0",
                "ALTER TABLE grid_config ADD COLUMN grid_count INTEGER DEFAULT 10",
                "ALTER TABLE grid_config ADD COLUMN stop_loss_price REAL DEFAULT 0",
                "ALTER TABLE grid_config ADD COLUMN direction TEXT DEFAULT '做多 (Long)'",
            ]:
                try:
                    conn.execute(_col_sql)
                except Exception:
                    pass  # 欄位已存在，忽略

            existing = conn.execute("SELECT COUNT(*) FROM bot_config").fetchone()[0]
            if existing == 0:
                seeds = [
                    ("crypto", "BTC-USD", "BTC", 0.001, 0.02, 0.04),
                    ("crypto", "ETH-USD", "ETH", 0.01,  0.02, 0.04),
                    ("crypto", "SOL-USD", "SOL", 0.1,   0.02, 0.04),
                ]
                conn.executemany(
                    "INSERT INTO bot_config "
                    "(bot_type,symbol,display_name,quantity,sl_pct,tp_pct) "
                    "VALUES (?,?,?,?,?,?)",
                    seeds
                )
        # 初始化必須同步完成（後續所有操作依賴 schema 存在）
        _w(_create).result(timeout=30)

    # ════════════════════════════════════════════════════════
    # bot_config（唯讀）
    # ════════════════════════════════════════════════════════
    def get_bot_config(self, bot_type: str = "crypto") -> List[Dict]:
        with _conn() as c:
            return [dict(r) for r in c.execute(
                "SELECT * FROM bot_config WHERE bot_type=? ORDER BY id", (bot_type,)
            ).fetchall()]

    # ════════════════════════════════════════════════════════
    # 真實盤
    # ════════════════════════════════════════════════════════
    def add_trade(self, symbol, side, price, qty, strategy="") -> None:
        ts = _now()
        def _write(conn):
            conn.execute(
                "INSERT INTO trades "
                "(symbol,side,price,quantity,strategy,timestamp) VALUES (?,?,?,?,?,?)",
                (symbol, side, price, qty, strategy, ts)
            )
            if side == "BUY":
                conn.execute(
                    "INSERT OR REPLACE INTO positions "
                    "(symbol,entry_price,quantity,strategy,opened_at) VALUES (?,?,?,?,?)",
                    (symbol, price, qty, strategy, ts)
                )
            else:
                conn.execute("DELETE FROM positions WHERE symbol=?", (symbol,))
        _w(_write)   # fire-and-forget

    def get_position(self, symbol: str) -> Optional[Tuple]:
        with _conn() as c:
            row = c.execute(
                "SELECT entry_price,quantity,strategy FROM positions WHERE symbol=?",
                (symbol,)
            ).fetchone()
            return tuple(row) if row else None

    def get_trades(self, limit: int = 100) -> pd.DataFrame:
        with _conn() as c:
            return pd.read_sql(
                "SELECT * FROM trades ORDER BY id DESC LIMIT ?",
                c, params=(limit,)
            )

    # ════════════════════════════════════════════════════════
    # 模擬盤
    # ════════════════════════════════════════════════════════
    def get_paper_account(self) -> Dict:
        with _conn() as c:
            return dict(c.execute("SELECT * FROM paper_account WHERE id=1").fetchone())

    def paper_buy(
        self,
        symbol:   str,
        qty:      float,
        price:    float,
        sl:       float = 0.0,
        tp:       float = 0.0,
        fee_rate: float = 0.001,
        reason:   str   = "manual",
    ) -> Dict:
        adj  = price * (1 + 0.001) * (1 + fee_rate)
        fee  = price * qty * fee_rate
        cost = adj * qty
        ts   = _now()

        # 先讀帳戶（需同步）
        with _conn() as c:
            acct = dict(c.execute("SELECT cash FROM paper_account WHERE id=1").fetchone())
        if acct["cash"] < cost:
            return {"ok": False, "msg": f"現金不足 需{cost:.2f} 餘{acct['cash']:.2f}"}

        def _write(conn):
            conn.execute(
                "UPDATE paper_account SET cash=cash-?, updated_at=? WHERE id=1",
                (cost, ts)
            )
            conn.execute(
                "INSERT OR REPLACE INTO paper_positions "
                "(symbol,entry_price,quantity,sl_price,tp_price,opened_at) "
                "VALUES (?,?,?,?,?,?)",
                (symbol, adj, qty, sl, tp, ts)
            )
            conn.execute(
                "INSERT INTO paper_trades "
                "(symbol,side,price,adj_price,quantity,fee,pnl,reason,timestamp) "
                "VALUES (?,?,?,?,?,?,?,?,?)",
                (symbol, "BUY", price, adj, qty, fee, 0, reason, ts)
            )
        _w(_write).result(timeout=_WRITE_TIMEOUT)
        return {"ok": True, "adj_price": adj, "fee": fee, "cost": cost}

    def paper_sell(
        self,
        symbol:   str,
        qty:      float,
        price:    float,
        fee_rate: float = 0.001,
        reason:   str   = "manual",
    ) -> Dict:
        # 先讀持倉（需同步）
        with _conn() as c:
            pos = c.execute(
                "SELECT entry_price,quantity FROM paper_positions WHERE symbol=?",
                (symbol,)
            ).fetchone()
        if not pos:
            return {"ok": False, "msg": "無持倉"}

        adj_sell = price * (1 - 0.001) * (1 - fee_rate)
        fee      = price * qty * fee_rate
        proceeds = adj_sell * qty
        pnl      = proceeds - pos["entry_price"] * qty
        ts       = _now()

        def _write(conn):
            conn.execute(
                "UPDATE paper_account SET cash=cash+?, updated_at=? WHERE id=1",
                (proceeds, ts)
            )
            conn.execute("DELETE FROM paper_positions WHERE symbol=?", (symbol,))
            conn.execute(
                "INSERT INTO paper_trades "
                "(symbol,side,price,adj_price,quantity,fee,pnl,reason,timestamp) "
                "VALUES (?,?,?,?,?,?,?,?,?)",
                (symbol, "SELL", price, adj_sell, qty, fee, pnl, reason, ts)
            )
        _w(_write).result(timeout=_WRITE_TIMEOUT)
        return {"ok": True, "adj_price": adj_sell, "fee": fee, "pnl": pnl}

    def get_paper_positions(self) -> List[Dict]:
        with _conn() as c:
            return [dict(r) for r in c.execute("SELECT * FROM paper_positions").fetchall()]

    def get_paper_trades(self, limit: int = 100) -> pd.DataFrame:
        with _conn() as c:
            return pd.read_sql(
                "SELECT * FROM paper_trades ORDER BY id DESC LIMIT ?",
                c, params=(limit,)
            )

    def log_paper_equity(self, total_value: float) -> None:
        ts = _now()
        _w(lambda conn: conn.execute(
            "INSERT INTO paper_equity_log (total_value,timestamp) VALUES (?,?)",
            (total_value, ts)
        ))   # fire-and-forget

    def get_paper_equity_log(self, limit: int = 500) -> pd.DataFrame:
        with _conn() as c:
            return pd.read_sql(
                "SELECT * FROM paper_equity_log ORDER BY id DESC LIMIT ?",
                c, params=(limit,)
            )

    def reset_paper_account(self, initial_cash: float = 100_000.0) -> None:
        ts = _now()
        def _write(conn):
            conn.execute(
                "UPDATE paper_account SET cash=?, initial_cash=?, updated_at=? WHERE id=1",
                (initial_cash, initial_cash, ts)
            )
            conn.execute("DELETE FROM paper_positions")
            conn.execute("DELETE FROM paper_trades")
            conn.execute("DELETE FROM paper_equity_log")
        _w(_write).result(timeout=_WRITE_TIMEOUT)

    # ════════════════════════════════════════════════════════
    # 網格設定
    # ════════════════════════════════════════════════════════
    def upsert_grid_config(self, symbol: str, **kwargs) -> int:
        """
        新增或更新網格設定。
        支援欄位：k, m, atr_period, ema_period, qty_per_grid, capital,
                  status, trade_mode, upper_price, lower_price,
                  grid_count, stop_loss_price, direction
        """
        defaults: Dict[str, Any] = {
            "k":               0.5,
            "m":               3.0,
            "atr_period":      14,
            "ema_period":      20,
            "qty_per_grid":    0.001,
            "capital":         1000.0,
            "status":          "STOPPED",
            # ── 雙模式新增欄位 ──────────────────────────────
            "trade_mode":      "auto",    # "auto" | "semi"
            "upper_price":     0.0,
            "lower_price":     0.0,
            "grid_count":      10,
            "stop_loss_price": 0.0,
            "direction":       "做多 (Long)",
        }
        defaults.update(kwargs)
        ts = _now()

        # 先讀（需同步）
        with _conn() as c:
            row = c.execute(
                "SELECT id FROM grid_config WHERE symbol=?", (symbol,)
            ).fetchone()
            existing_id = row["id"] if row else None

        def _write(conn) -> int:
            if existing_id:
                sets = ", ".join(f"{k}=?" for k in defaults)
                conn.execute(
                    f"UPDATE grid_config SET {sets}, updated_at=? WHERE symbol=?",
                    (*defaults.values(), ts, symbol)
                )
                return existing_id

            # 動態 INSERT（欄位由 defaults 決定，避免硬編碼）
            cols = list(defaults.keys())
            vals = list(defaults.values())
            col_str  = ", ".join(cols)
            ph_str   = ", ".join("?" for _ in cols)
            cur = conn.execute(
                f"INSERT INTO grid_config (symbol, {col_str}, created_at, updated_at)"
                f" VALUES (?, {ph_str}, ?, ?)",
                (symbol, *vals, ts, ts)
            )
            return cur.lastrowid

        return _w(_write).result(timeout=_WRITE_TIMEOUT)

    def get_grid_config(self, symbol: str) -> Optional[Dict]:
        with _conn() as c:
            row = c.execute(
                "SELECT * FROM grid_config WHERE symbol=?", (symbol,)
            ).fetchone()
            return dict(row) if row else None

    def get_all_grid_configs(self) -> List[Dict]:
        with _conn() as c:
            return [dict(r) for r in c.execute(
                "SELECT * FROM grid_config WHERE status != 'DELETED' ORDER BY id"
            ).fetchall()]

    def set_grid_status(self, symbol: str, status: str) -> None:
        ts = _now()
        _w(lambda conn: conn.execute(
            "UPDATE grid_config SET status=?, updated_at=? WHERE symbol=?",
            (status, ts, symbol)
        ))   # fire-and-forget

    def set_grid_last_regrid(self, symbol: str) -> None:
        ts = _now()
        _w(lambda conn: conn.execute(
            "UPDATE grid_config SET last_regrid=?, updated_at=? WHERE symbol=?",
            (ts, ts, symbol)
        ))

    # ════════════════════════════════════════════════════════
    # 網格格位
    # ════════════════════════════════════════════════════════
    def save_grid_levels(self, grid_id: int, symbol: str, levels: list) -> None:
        ts = _now()
        rows = [
            (grid_id, symbol, lv.index, lv.price, lv.side,
             lv.order_id, lv.status, ts)
            for lv in levels
        ]
        def _write(conn):
            conn.execute("DELETE FROM grid_levels WHERE grid_id=?", (grid_id,))
            conn.executemany(
                "INSERT INTO grid_levels "
                "(grid_id,symbol,level_index,price,side,order_id,status,created_at) "
                "VALUES (?,?,?,?,?,?,?,?)",
                rows
            )
        _w(_write).result(timeout=_WRITE_TIMEOUT)

    def get_grid_levels(self, grid_id: int) -> List[Dict]:
        with _conn() as c:
            return [dict(r) for r in c.execute(
                "SELECT * FROM grid_levels WHERE grid_id=? ORDER BY price",
                (grid_id,)
            ).fetchall()]

    def update_grid_level_filled(
        self,
        grid_id:      int,
        order_id:     str,
        filled_price: float,
        filled_qty:   float,
    ) -> None:
        ts = _now()
        _w(lambda conn: conn.execute(
            "UPDATE grid_levels "
            "SET status='FILLED', filled_price=?, filled_qty=?, filled_at=? "
            "WHERE grid_id=? AND order_id=?",
            (filled_price, filled_qty, ts, grid_id, order_id)
        ))

    def update_grid_level_status(
        self,
        grid_id:  int,
        order_id: str,
        status:   str,
    ) -> None:
        _w(lambda conn: conn.execute(
            "UPDATE grid_levels SET status=? WHERE grid_id=? AND order_id=?",
            (status, grid_id, order_id)
        ))

    # ════════════════════════════════════════════════════════
    # 網格 PnL
    # ════════════════════════════════════════════════════════
    def record_grid_trade(
        self,
        symbol:  str,
        buy_px:  float,
        sell_px: float,
        qty:     float,
        fee:     float = 0.0,
    ) -> Dict:
        gross = (sell_px - buy_px) * qty
        net   = gross - fee
        ts    = _now()
        _w(lambda conn: conn.execute(
            "INSERT INTO grid_trades "
            "(symbol,buy_price,sell_price,qty,gross_pnl,fee,net_pnl,closed_at) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (symbol, buy_px, sell_px, qty, gross, fee, net, ts)
        ))   # fire-and-forget — PnL 由 risk_mgr.push_pnl 即時更新記憶體快取
        return {"gross_pnl": gross, "net_pnl": net}

    def get_grid_trades(self, symbol: str = "", limit: int = 200) -> pd.DataFrame:
        with _conn() as c:
            if symbol:
                return pd.read_sql(
                    "SELECT * FROM grid_trades "
                    "WHERE symbol=? ORDER BY id DESC LIMIT ?",
                    c, params=(symbol, limit)
                )
            return pd.read_sql(
                "SELECT * FROM grid_trades ORDER BY id DESC LIMIT ?",
                c, params=(limit,)
            )

    def get_recent_grid_trades(self, limit: int = 20, symbol: str = "") -> List[Dict]:
        """
        回傳最近 N 筆網格成交紀錄（JSON 可序列化的 list[dict]）。
        供 web_ui.py 的 /api/recent_fills 端點使用。
        """
        with _conn() as c:
            if symbol:
                rows = c.execute(
                    "SELECT * FROM grid_trades WHERE symbol=? ORDER BY id DESC LIMIT ?",
                    (symbol.upper(), limit)
                ).fetchall()
            else:
                rows = c.execute(
                    "SELECT * FROM grid_trades ORDER BY id DESC LIMIT ?",
                    (limit,)
                ).fetchall()
            return [dict(r) for r in rows]

    def get_open_grid_levels(self, symbol: str) -> List[Dict]:
        """
        回傳指定幣種目前狀態為 PENDING 或 OPEN 的網格格位。
        供 web_ui.py 的 /api/open_orders/{symbol} 端點使用。
        """
        with _conn() as c:
            # 先找最近一筆 RUNNING 的 grid_config
            cfg_row = c.execute(
                "SELECT id FROM grid_config WHERE symbol=? AND status='RUNNING' ORDER BY id DESC LIMIT 1",
                (symbol.upper(),)
            ).fetchone()
            if not cfg_row:
                return []
            grid_id = cfg_row["id"]
            rows = c.execute(
                "SELECT * FROM grid_levels WHERE grid_id=? AND status IN ('PENDING','OPEN') ORDER BY price",
                (grid_id,)
            ).fetchall()
            return [dict(r) for r in rows]

    def get_grid_summary(self, symbol: str) -> Dict:
        with _conn() as c:
            row = c.execute(
                "SELECT COUNT(*) as trades, SUM(net_pnl) as total_pnl, "
                "SUM(fee) as total_fee FROM grid_trades WHERE symbol=?",
                (symbol,)
            ).fetchone()
            return dict(row) if row else {"trades": 0, "total_pnl": 0.0, "total_fee": 0.0}

    # ════════════════════════════════════════════════════════
    # 自動清理
    # ════════════════════════════════════════════════════════
    def cleanup_old_records(self, days: int = 30) -> Dict[str, int]:
        from datetime import timedelta
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()

        results: Dict[str, int] = {}

        def _write(conn) -> Dict[str, int]:
            r: Dict[str, int] = {}
            r["trades"]         = conn.execute(
                "DELETE FROM trades WHERE timestamp < ?", (cutoff,)
            ).rowcount
            r["paper_trades"]   = conn.execute(
                "DELETE FROM paper_trades WHERE timestamp < ?", (cutoff,)
            ).rowcount
            r["paper_equity_log"] = conn.execute(
                "DELETE FROM paper_equity_log WHERE timestamp < ?", (cutoff,)
            ).rowcount
            r["grid_trades"]    = conn.execute(
                "DELETE FROM grid_trades "
                "WHERE closed_at IS NOT NULL AND closed_at < ?", (cutoff,)
            ).rowcount
            r["grid_levels"]    = conn.execute(
                "DELETE FROM grid_levels "
                "WHERE status IN ('FILLED','CANCELLED') AND filled_at < ?",
                (cutoff,)
            ).rowcount
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            return r

        results = _w(_write).result(timeout=60)
        total   = sum(results.values())
        log.info(
            f"[DB Cleanup] 清理完成（>{days}天）: "
            + ", ".join(f"{k}={v}" for k, v in results.items())
            + f"，共 {total} 筆"
        )
        return results

    def get_db_stats(self) -> Dict:
        tables = [
            "trades", "paper_trades", "paper_equity_log",
            "grid_config", "grid_levels", "grid_trades",
        ]
        stats: Dict[str, Any] = {}
        with _conn() as c:
            for tbl in tables:
                try:
                    row = c.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()
                    stats[tbl] = row[0] if row else 0
                except Exception:
                    stats[tbl] = -1
        try:
            stats["db_size_mb"] = round(os.path.getsize(DB_PATH) / 1024 / 1024, 2)
        except Exception:
            stats["db_size_mb"] = 0.0
        # 佇列待寫筆數（診斷用）
        stats["writer_queue_size"] = _get_writer()._queue.qsize() if _writer else 0
        return stats
