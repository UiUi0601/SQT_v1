#!/usr/bin/env python3
"""
Quick utility: update the active grid_config row to FUTURES / leverage=5 / CROSSED.

Usage:
  python update_grid_config.py               # updates the RUNNING row (or latest)
  python update_grid_config.py BTCUSDT       # target a specific symbol
  python update_grid_config.py BTCUSDT 10    # custom leverage
"""
import sqlite3, sys, pathlib

DB = pathlib.Path("trading_bot.db")
if not DB.exists():
    sys.exit(f"DB not found at: {DB.resolve()}\nRun from the project root directory.")

target_symbol = sys.argv[1].upper() if len(sys.argv) > 1 else None
target_lev    = int(sys.argv[2]) if len(sys.argv) > 2 else 5

con = sqlite3.connect(DB, timeout=10)
con.row_factory = sqlite3.Row
con.execute("PRAGMA journal_mode=WAL")

if target_symbol:
    row = con.execute(
        "SELECT symbol, status, market_type, leverage, margin_type FROM grid_config WHERE symbol=? LIMIT 1",
        (target_symbol,)
    ).fetchone()
    if row is None:
        sys.exit(f"Symbol {target_symbol!r} not found in grid_config.")
else:
    # Prefer RUNNING; fall back to most-recently-updated row
    row = con.execute("""
        SELECT symbol, status, market_type, leverage, margin_type
        FROM grid_config
        WHERE status != 'DELETED'
        ORDER BY CASE status WHEN 'RUNNING' THEN 0 ELSE 1 END,
                 updated_at DESC
        LIMIT 1
    """).fetchone()
    if row is None:
        sys.exit("No rows found in grid_config.")

sym = row["symbol"]
print(f"Target  : {sym}")
print(f"Before  : market_type={row['market_type']}, leverage={row['leverage']}x, margin_type={row['margin_type']}, status={row['status']}")

con.execute("""
    UPDATE grid_config
    SET market_type  = 'FUTURES',
        leverage     = ?,
        margin_type  = 'CROSSED',
        updated_at   = datetime('now')
    WHERE symbol = ?
""", (target_lev, sym))
con.commit()

after = con.execute(
    "SELECT market_type, leverage, margin_type FROM grid_config WHERE symbol=?", (sym,)
).fetchone()
print(f"After   : market_type={after['market_type']}, leverage={after['leverage']}x, margin_type={after['margin_type']}")
print(f"\nDone. Restart run_bot.py to pick up the new settings.")
con.close()
