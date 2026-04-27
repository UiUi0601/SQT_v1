# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Running the System

Two processes must run separately (they share `trading_bot.db` via SQLite WAL mode):

```bash
# 1. Grid trading bot
python run_bot.py

# 2. Web UI (FastAPI on port 8000)
python web_ui.py
```

`run_bot.py` dispatches to `crypto_main_grid.py` (default) or `crypto_main_v4.py` based on `BOT_VERSION` env var. After restarting `web_ui.py`, always **hard-refresh the browser** (Ctrl+Shift+R) to clear the cached `index.html`.

### Environment setup

```bash
cp .env.example .env   # then fill in real values
pip install -r requirements.txt
```

Minimum required `.env` fields: `BINANCE_API_KEY`, `BINANCE_API_SECRET`, `BINANCE_TESTNET=true` (start with testnet).

### UI authentication

```bash
# Generate a PBKDF2 password hash for web UI
python -m quant_system.utils.ui_auth hash
# Then set UI_USERNAME and UI_PASSWORD_HASH in .env
```

If `UI_USERNAME`/`UI_PASSWORD_HASH` are unset, the UI is open (dev mode).

### Docker

```bash
docker compose -f deploy/docker-compose.yml up -d
docker compose -f deploy/docker-compose.yml logs -f aqt_bot
docker compose -f deploy/docker-compose.yml restart aqt_bot   # restart bot only
```

### Encrypt `.env` for production

```bash
python scripts/encrypt_env.py   # produces .env.enc
AQT_MASTER_PASSWORD=your_pwd python run_bot.py
```

---

## Architecture

### Two-process design

```
run_bot.py  ──→  crypto_main_grid.py  (GridBotInstance loop)
                       │
                  trading_bot.db  (SQLite WAL, shared)
                       │
web_ui.py  (FastAPI) ──┘  ←── index.html (single-page UI)
```

`web_ui.py` only reads/writes the DB and calls Binance REST directly. The bot process is the only one that holds a live Binance WebSocket and places grid orders.

### Trading modes (critical design constraint)

`GridBotInstance` operates in one of two modes set via `cfg["trade_mode"]`:

- **`auto`** — Full autonomy: `GridEngine` computes ATR/EMA bounds every tick, `GridManager.deploy()` places orders, `GridManager.on_fill()` places reverse orders on fill.
- **`semi`** — Observation only: the bot fetches data, runs risk checks, and syncs fill status to DB, but **`deploy()` and `on_fill()` are completely blocked**. Human places orders via `POST /api/manual_trade` (which has its own risk gate).

The `tick()` method in `GridBotInstance` enforces this split as two explicit phases: **Observation** (both modes) then **Execution** (auto-only gate).

### Bot main loop (`crypto_main_grid.py`)

`run_grid_bot()` → infinite loop → reads `RUNNING` configs from DB → creates/reuses `GridBotInstance` per symbol → calls `instance.tick()` every `GRID_INTERVAL_SECONDS` (default 30s).

`GridBotInstance.tick()` phases:
1. **Observation**: fetch k-lines (`DataCache` → yfinance), trend filter (`TrendFilter`), `RiskManager.check_24h_drawdown()`, compute snap (`_build_semi_snap` or `GridEngine.compute`), `_collect_fills()` → DB sync + fee calc + PnL recording.
2. **Execution**: auto-only → `GridManager.on_fill()` (reverse orders) then `_tick_auto()` (regrid via `deploy()`). Semi → blocked, only stop-loss `cancel_all()` allowed.

### Database (`database.py`)

Single SQLite file (`trading_bot.db`, WAL mode). All writes go through a `_DBWriter` thread queue — never write directly with raw `sqlite3.execute` from a non-writer thread. Key tables: `grid_config`, `grid_levels`, `grid_trades`. Access via `TradeDB` class methods.

### Web API (`web_ui.py`)

FastAPI app serving `index.html` at `/` plus REST endpoints. Auth via `Depends(_check_auth)` which calls `ui_auth.verify_password()` against `UI_PASSWORD_HASH`. Key endpoints:

| Endpoint | Purpose |
|---|---|
| `GET /api/grid_status` | Bot alive status + last config |
| `POST /api/save_grid` 🔒 | Write config to DB (bot picks it up next tick) |
| `POST /api/manual_trade` 🔒 | Human order (semi mode); gated by `check_exposure` + `check_24h_drawdown` |
| `GET /api/health_check` | Self-diagnosis: .env keys, DB, bot PID, Binance ping |
| `WS /ws/kline/{symbol}` | Proxy Binance kline stream to browser |
| `WS /ws/logs` | Tail `logs/grid_bot.log` to browser |

### Risk stack (`quant_system/risk/`)

- **`RiskManager`** — in-memory PBKDF2-rolling 24h PnL deque (`push_pnl`) + per-symbol exposure tracking. `check_24h_drawdown()` is O(n) memory-only, no DB. Call `hydrate(db, client)` on startup to restore state.
- **`KillSwitch`** — global pause flag; when triggered, all `tick()` calls return immediately. Supports `limit_5pct`, `twap`, `market` close modes.
- **`CircuitBreaker`** — trips after N errors in a sliding window; blocks API calls during cooldown.

### Grid engine (`quant_system/grid/`)

- **`GridEngine.compute(df)`** → `GridSnapshot` (ATR/EMA auto bounds). `should_regrid()` checks if center has drifted > threshold.
- **`GridManager`** — holds `PartialFillTracker`, places orders via `deploy()` (initial grid) and `on_fill()` (reverse orders). Uses `PrecisionManager` to floor quantities to exchange step size. Order IDs are UUID-based (`newClientOrderId`) for idempotency.
- **`TrendFilter`** — 4-layer check: ATR/price extreme → crash detection → EMA200 → volume surge + price shock. Returns `FilterResult` with `allow_buy`, `allow_sell`, `allow_grid`.

### Data flow for a grid fill

```
Binance WebSocket → BinanceUserStream → _collect_fills()
  → DB update (grid_levels)
  → FeeCalculator.calc_buy() adjusts safe_sell_qty
  → _record_grid_trade() → risk_mgr.push_pnl()
  → [AUTO only] GridManager.on_fill() → places reverse limit order
```

### PID / process management

`runtime/trading_bot.pid` — written by `run_bot.py`, used by web UI to check bot liveness. `runtime/web_ui.pid` — same for the web process. `web_ui.py` auto-kills stale processes occupying port 8000 on startup.

### SEMI_AUTO execution mode

`TRADE_EXECUTION_MODE` (env var, default `FULL_AUTO`) controls whether the bot needs human confirmation before deploying a grid:

- **`FULL_AUTO`** — Original behavior; bot deploys automatically once all risk checks pass.
- **`SEMI_AUTO`** — Bot generates a signal with `indicator_data` (ATR, EMA, grid bounds) and writes it to `pending_signals` DB table. `GridBotInstance._await_signal_confirmation()` blocks-polls for up to `SIGNAL_EXPIRE_SECONDS` seconds. If the UI confirms (`POST /api/confirm_trade`), the bot validates slippage and deploys. If the signal is rejected, expires, or slippage too high, deployment is abandoned and logged.

This gate fires in **both** `trade_mode=semi` (fixed bounds) and `trade_mode=auto` (ATR/EMA computed bounds). All existing risk checks (24h drawdown, exposure cap, circuit breaker, kill switch, trend filter) run **before** the SEMI_AUTO gate — they are never skipped.

The gate can be toggled at runtime via `POST /api/set_execution_mode` (UI button in the 機器人 panel). The change writes to `.env` and takes effect on the next bot restart.

| Env var | Default | Effect |
|---|---|---|
| `TRADE_EXECUTION_MODE` | `FULL_AUTO` | `SEMI_AUTO` = human-confirm before deploy |
| `SIGNAL_EXPIRE_SECONDS` | 30 | Seconds before an unconfirmed signal auto-expires |
| `SEMI_AUTO_SLIPPAGE_PCT` | 0.005 | Max price drift allowed between signal generation and confirmed execution |

### Key env vars for tuning

| Variable | Default | Effect |
|---|---|---|
| `GRID_INTERVAL_SECONDS` | 30 | Main loop cadence |
| `MAX_EXPOSURE_PCT` | 0.30 | Per-symbol exposure cap (fraction of `capital` in cfg) |
| `MAX_24H_DRAWDOWN_PCT` | 0.10 | Rolling 24h loss threshold |
| `CB_ERROR_THRESHOLD` / `CB_COOLDOWN_SEC` | 10 / 600 | Circuit breaker sensitivity |
| `USE_WEBSOCKET` | true | false → REST poll fallback |
| `BOT_VERSION` | grid | `v4` to run legacy multi-strategy bot |
