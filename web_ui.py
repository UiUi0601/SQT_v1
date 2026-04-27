"""
web_ui.py — AQT 動態網格交易控制台 v6.0（後端）

v6 新增：
  - Bearer Token 身份驗證（CONSOLE_TOKEN 環境變數）
  - /api/ping                  → 延遲量測
  - /api/full_balance          → 完整 Binance 帳戶餘額（所有非零資產）
  - /api/cancel_all/{symbol}   → 進階 Kill Switch（直接呼叫 Binance 撤銷所有掛單）
  - /ws/logs                   → 即時串流 grid_bot.log
  - save_grid / stop_grid 觸發 Telegram 推播

端點：
  GET  /                              → 提供 index.html
  GET  /api/ping                      → 延遲量測
  GET  /api/klines/{symbol}           → 歷史 K 線（UTC 秒，多批次 >1000）
  GET  /api/ticker24h/{symbol}        → 24h 行情
  GET  /api/strategy_suggest          → 智慧策略建議
  POST /api/save_grid       🔒        → 儲存並啟動網格
  GET  /api/grid_status               → 網格機器人狀態
  POST /api/stop_grid       🔒        → 停止網格機器人
  GET  /api/history                   → 歷史設定記錄
  GET  /api/balance                   → Binance USDT 可用餘額
  GET  /api/full_balance    🔒        → 完整帳戶餘額（所有幣種）
  GET  /api/open_orders/{symbol}      → PENDING/OPEN 網格掛單
  GET  /api/recent_fills              → 最近 N 筆成交
  POST /api/cancel_all/{symbol} 🔒   → Kill Switch（Binance 直接撤單）
  WS   /ws/kline/{symbol}             → K 線 WebSocket 代理
  WS   /ws/logs                       → 即時日誌串流
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import logging
import math
import os
import signal as _sig
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Windows cp932/cp950 環境下強制 UTF-8 輸出，避免 UnicodeEncodeError
if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
if sys.stderr and hasattr(sys.stderr, "reconfigure"):
    try:
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

from dotenv import load_dotenv
load_dotenv()

import requests as req
import websockets
from fastapi import Depends, FastAPI, HTTPException, Query, Security, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
import uvicorn

# ── UI 身份驗證（PBKDF2-HMAC-SHA256）────────────────────────
from quant_system.utils.ui_auth import _load_credentials, verify_password
# ── 風險管理器（供 /api/manual_trade 風控攔截）──────────────
from quant_system.risk.risk_manager import RiskManager, RiskConfig

# ── TradeDB（統一持久層）────────────────────────────────────
# 確保能 import 同目錄的 database.py
sys.path.insert(0, str(Path(__file__).parent))
from database import TradeDB  # noqa: E402

# ── 日誌 ──────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("aqt")

# ════════════════════════════════════════════════════════════
# 常數
# ════════════════════════════════════════════════════════════
BINANCE_API  = "https://api.binance.com/api/v3"
BINANCE_WS   = "wss://stream.binance.com:9443/ws"
PID_FILE     = Path(__file__).parent / "runtime" / "trading_bot.pid"
FEE_RATE     = 0.001   # Binance Maker 0.1%

# ── Binance 帳戶 API 金鑰（來自 .env）────────────────────────
_API_KEY    = os.getenv("BINANCE_API_KEY", "")
_API_SECRET = os.getenv("BINANCE_API_SECRET", "")


def _signed_get(path: str, params: dict | None = None) -> req.Response:
    """
    向 Binance 帳戶端點發送 HMAC-SHA256 簽章 GET 請求。
    path 範例：'/account'（不含 /api/v3）
    若未設定 API KEY / SECRET，直接回傳 401 mock Response。
    """
    if not _API_KEY or not _API_SECRET:
        raise PermissionError("BINANCE_API_KEY / BINANCE_API_SECRET 未設定")

    p = params.copy() if params else {}
    p["timestamp"] = int(time.time() * 1000)
    query_str = "&".join(f"{k}={v}" for k, v in p.items())
    sig = hmac.new(
        _API_SECRET.encode(), query_str.encode(), hashlib.sha256
    ).hexdigest()
    query_str += f"&signature={sig}"

    url = f"{BINANCE_API}{path}?{query_str}"
    return req.get(url, headers={"X-MBX-APIKEY": _API_KEY}, timeout=8)


def _signed_delete(path: str, params: dict | None = None) -> req.Response:
    """向 Binance 帳戶端點發送 HMAC-SHA256 簽章 DELETE 請求。"""
    if not _API_KEY or not _API_SECRET:
        raise PermissionError("BINANCE_API_KEY / BINANCE_API_SECRET 未設定")
    p = params.copy() if params else {}
    p["timestamp"] = int(time.time() * 1000)
    query_str = "&".join(f"{k}={v}" for k, v in p.items())
    sig = hmac.new(_API_SECRET.encode(), query_str.encode(), hashlib.sha256).hexdigest()
    query_str += f"&signature={sig}"
    url = f"{BINANCE_API}{path}?{query_str}"
    return req.delete(url, headers={"X-MBX-APIKEY": _API_KEY}, timeout=10)


def _signed_post(path: str, params: dict | None = None) -> req.Response:
    """向 Binance 帳戶端點發送 HMAC-SHA256 簽章 POST 請求（用於下單）。"""
    if not _API_KEY or not _API_SECRET:
        raise PermissionError("BINANCE_API_KEY / BINANCE_API_SECRET 未設定")
    p = params.copy() if params else {}
    p["timestamp"] = int(time.time() * 1000)
    query_str = "&".join(f"{k}={v}" for k, v in p.items())
    sig = hmac.new(_API_SECRET.encode(), query_str.encode(), hashlib.sha256).hexdigest()
    body = query_str + f"&signature={sig}"
    return req.post(
        f"{BINANCE_API}{path}",
        data=body,
        headers={
            "X-MBX-APIKEY": _API_KEY,
            "Content-Type": "application/x-www-form-urlencoded",
        },
        timeout=10,
    )

# ── TradeDB 初始化（統一讀寫 trading_bot.db）────────────────
db_abs_path = str(Path(__file__).parent / "trading_bot.db")
trade_db = TradeDB(db_path=db_abs_path)
log.info(f"[DB] TradeDB 初始化完成，路徑: {db_abs_path}")

# ── Telegram 推播（網格事件）────────────────────────────────
try:
    from quant_system.utils.alert import GridAlerts
    grid_alerts = GridAlerts()
except Exception:
    class _NoOpAlerts:
        def __getattr__(self, name): return lambda *a, **kw: None
    grid_alerts = _NoOpAlerts()  # type: ignore

# ── 身份驗證（整合 ui_auth PBKDF2-HMAC-SHA256）─────────────
# 舊 CONSOLE_TOKEN 明文比對已移除；改用 UI_PASSWORD_HASH 雜湊驗證。
# 若未設定 UI_USERNAME / UI_PASSWORD_HASH，自動開放存取（開發模式）。
_bearer = HTTPBearer(auto_error=False)


def _check_auth(
    credentials: HTTPAuthorizationCredentials = Security(_bearer),
) -> bool:
    """
    驗證邏輯：
      - 未設定 UI_PASSWORD_HASH → 開放存取（本機開發模式）
      - 設定後 → Bearer token 必須通過 PBKDF2 雜湊比對
    """
    username, password_hash = _load_credentials()
    if not username or not password_hash:
        return True   # 開發模式：無憑證設定，直接放行
    if credentials is None:
        raise HTTPException(status_code=401, detail="未授權：請提供 Bearer Token")
    if not verify_password(credentials.credentials, password_hash):
        raise HTTPException(status_code=401, detail="未授權：Token 錯誤")
    return True


# ── 風控管理器（供 /api/manual_trade 攔截）──────────────────
_risk_mgr = RiskManager(RiskConfig(
    max_exposure_pct     = float(os.getenv("MAX_EXPOSURE_PCT",      "0.30")),
    max_24h_drawdown_pct = float(os.getenv("MAX_24H_DRAWDOWN_PCT",  "0.10")),
    pause_duration_hours = float(os.getenv("PAUSE_DURATION_HOURS",  "4.0")),
))

# ════════════════════════════════════════════════════════════
# FastAPI App
# ════════════════════════════════════════════════════════════
app = FastAPI(title="AQT Grid Console", version="3.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── 掛載靜態檔案 ──────────────────────────────────────────────
static_dir = Path(__file__).parent / "static"
static_dir.mkdir(exist_ok=True)
app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")


# ── 手續費設定 ────────────────────────────────────────────────
TAKER_FEE      = 0.001   # Binance 現貨 Taker 0.1%
MAKER_FEE      = 0.001   # Binance 現貨 Maker 0.1%
ROUND_TRIP_FEE = TAKER_FEE + MAKER_FEE  # 0.2% 雙邊
MIN_NET_PROFIT = 0.004   # 最低單格淨利潤率 0.4%（低於此值警告）

# ── Pydantic 模型 ─────────────────────────────────────────────
class GridSave(BaseModel):
    symbol:          str
    direction:       str
    upper_price:     float
    lower_price:     float
    grid_count:      int   = 10
    stop_loss_price: float = 0.0    # 0 = 停用止損
    trade_mode:      str   = "semi" # "auto" = 全自動ATR/EMA；"semi" = 半自動固定上下限
    qty_per_grid:    float = 0.001  # 每格下單數量


# ════════════════════════════════════════════════════════════
# 技術指標工具函式
# ════════════════════════════════════════════════════════════

def _fetch_raw_klines(symbol: str, interval: str = "1h", limit: int = 100) -> list:
    """直接從 Binance 取得原始 K 線陣列"""
    r = req.get(
        f"{BINANCE_API}/klines",
        params={"symbol": symbol.upper(), "interval": interval, "limit": limit},
        timeout=10,
    )
    r.raise_for_status()
    data = r.json()
    if isinstance(data, dict):
        raise ValueError(data.get("msg", "Binance API 錯誤"))
    return data  # [[openTime, o, h, l, c, vol, ...], ...]


def _calc_atr(klines: list, period: int = 14) -> float:
    """計算 ATR（平均真實波幅）"""
    trs = []
    for i in range(1, len(klines)):
        h      = float(klines[i][2])
        l      = float(klines[i][3])
        c_prev = float(klines[i - 1][4])
        trs.append(max(h - l, abs(h - c_prev), abs(l - c_prev)))
    if not trs:
        return 0.0
    recent = trs[-period:] if len(trs) >= period else trs
    return sum(recent) / len(recent)


def _calc_bollinger(klines: list, period: int = 20, std_mult: float = 2.0) -> tuple[float, float, float]:
    """計算布林通道（upper, mid, lower）"""
    closes = [float(k[4]) for k in klines[-period:]]
    if len(closes) < 2:
        p = closes[-1] if closes else 0.0
        return p, p, p
    mean = sum(closes) / len(closes)
    var  = sum((c - mean) ** 2 for c in closes) / len(closes)
    std  = var ** 0.5
    return mean + std_mult * std, mean, mean - std_mult * std


def _calc_hv(klines: list, period: int = 30) -> float:
    """計算歷史波動率（年化 %）"""
    closes = [float(k[4]) for k in klines[-(period + 1):]]
    if len(closes) < 2:
        return 0.0
    rets = [math.log(closes[i] / closes[i - 1]) for i in range(1, len(closes))]
    mean = sum(rets) / len(rets)
    var  = sum((r - mean) ** 2 for r in rets) / len(rets)
    return math.sqrt(var * 365) * 100  # 年化 %


def _detect_regime(klines: list, atr: float, price: float) -> str:
    """判斷市場狀態：RANGING / TRENDING / HIGH_VOL"""
    _, bb_mid, _ = _calc_bollinger(klines)
    bb_u, _, bb_l = _calc_bollinger(klines)
    bb_width_pct = (bb_u - bb_l) / bb_mid if bb_mid > 0 else 0
    atr_pct      = atr / price if price > 0 else 0
    price_vs_mid = abs(price - bb_mid) / bb_mid if bb_mid > 0 else 0

    if atr_pct > 0.03:        return "HIGH_VOL"
    if price_vs_mid > 0.025:  return "TRENDING"
    return "RANGING"


def _profit_pct(spacing: float, price: float) -> float:
    """扣除手續費後單格來回凈利潤率 %"""
    if price <= 0:
        return 0.0
    return max(0.0, spacing / price * 100 - FEE_RATE * 2 * 100)


def _build_strategies(price: float, atr: float, bb_u: float, bb_l: float,
                      bb_mid: float, regime: str) -> dict:
    """依市場狀態產生保守 / 標準 / 積極三組策略建議"""

    # ── 各策略的區間倍數與格數 ────────────────────────────────
    configs = {
        "conservative": {
            "name": "保守策略",
            "emoji": "🛡",
            "upper": bb_u,
            "lower": bb_l,
            "count": 8,
            "desc": "以布林通道為邊界，格距較密",
        },
        "standard": {
            "name": "標準策略",
            "emoji": "⚡",
            "upper": bb_mid + 2.0 * atr,
            "lower": bb_mid - 2.0 * atr,
            "count": 12,
            "desc": "EMA 中軸 ± 2×ATR，平衡風報比",
        },
        "aggressive": {
            "name": "積極策略",
            "emoji": "🔥",
            "upper": bb_mid + 3.0 * atr,
            "lower": bb_mid - 3.0 * atr,
            "count": 18,
            "desc": "寬區間高格數，適合震盪劇烈期",
        },
    }

    # HIGH_VOL 模式：全部放寬 20%
    if regime == "HIGH_VOL":
        for k in configs:
            mid = (configs[k]["upper"] + configs[k]["lower"]) / 2
            half = (configs[k]["upper"] - configs[k]["lower"]) / 2 * 1.2
            configs[k]["upper"] = mid + half
            configs[k]["lower"] = mid - half

    result = {}
    for key, cfg in configs.items():
        upper  = max(round(cfg["upper"], 2), price * 1.001)
        lower  = min(round(cfg["lower"], 2), price * 0.999)
        count  = cfg["count"]
        spacing = (upper - lower) / count
        result[key] = {
            "name":         cfg["name"],
            "emoji":        cfg["emoji"],
            "desc":         cfg["desc"],
            "upper_price":  upper,
            "lower_price":  lower,
            "grid_count":   count,
            "spacing":      round(spacing, 4),
            "spacing_pct":  f"{spacing / price * 100:.3f}%",
            "profit_pct":   f"{_profit_pct(spacing, price):.3f}%",
        }
    return result


# ════════════════════════════════════════════════════════════
# REST 路由
# ════════════════════════════════════════════════════════════

@app.get("/", include_in_schema=False)
async def serve_index():
    p = Path(__file__).parent / "index.html"
    if not p.exists():
        return JSONResponse(status_code=404, content={"error": "index.html 不存在"})
    return FileResponse(str(p), media_type="text/html")


@app.get("/api/klines/{symbol}")
async def get_klines(
    symbol:   str,
    interval: str = Query("15m"),
    limit:    int = Query(250, ge=10, le=10000),
):
    """
    歷史 K 線，支援多批次拉取（Binance 每次上限 1000）。
    1d 級別自動擴充至 1825 筆（約 5 年）。
    回傳格式：[{time(UTC秒), open, high, low, close, volume}]
    """
    # 1d 級別自動擴充至近 5 年（約 1825 根日線）
    if interval == "1d" and limit < 1825:
        limit = 1825

    try:
        BATCH = 1000
        all_data: list = []
        end_time: int | None = None
        remaining = limit

        while remaining > 0:
            batch_size = min(remaining, BATCH)
            params: dict = {
                "symbol":   symbol.upper(),
                "interval": interval,
                "limit":    batch_size,
            }
            if end_time is not None:
                params["endTime"] = end_time

            r = req.get(f"{BINANCE_API}/klines", params=params, timeout=15)
            r.raise_for_status()
            batch = r.json()

            if isinstance(batch, dict):
                raise ValueError(batch.get("msg", "Binance API 錯誤"))
            if not batch:
                break

            # 批次資料往前插入（較早的資料在前）
            all_data = batch + all_data
            remaining -= len(batch)

            # 下次請求用最早那根 K 線的開盤時間 - 1ms 作為 endTime
            end_time = int(batch[0][0]) - 1

            if len(batch) < batch_size:
                break  # Binance 已無更多資料

        return [
            {
                "time":   int(k[0]) // 1000,
                "open":   float(k[1]),
                "high":   float(k[2]),
                "low":    float(k[3]),
                "close":  float(k[4]),
                "volume": float(k[5]),
            }
            for k in all_data
        ]
    except req.exceptions.ConnectionError:
        return JSONResponse(status_code=503, content={"error": "無法連接 Binance API"})
    except req.exceptions.Timeout:
        return JSONResponse(status_code=504, content={"error": "請求逾時"})
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.get("/api/ticker24h/{symbol}")
async def get_ticker24h(symbol: str):
    """24h 行情：price, change, high, low, volume"""
    try:
        r = req.get(f"{BINANCE_API}/ticker/24hr",
                    params={"symbol": symbol.upper()}, timeout=5)
        r.raise_for_status()
        d = r.json()
        if isinstance(d, dict) and "code" in d:
            return JSONResponse(status_code=400, content={"error": d.get("msg")})
        return {
            "symbol": d["symbol"],
            "price":  float(d["lastPrice"]),
            "change": float(d["priceChangePercent"]),
            "high":   float(d["highPrice"]),
            "low":    float(d["lowPrice"]),
            "volume": float(d["volume"]),
        }
    except Exception as e:
        return JSONResponse(status_code=503, content={"error": str(e)})


# ══ ★ 智慧策略建議 ═══════════════════════════════════════════

@app.get("/api/strategy_suggest")
async def strategy_suggest(
    symbol:   str = Query("BTCUSDT"),
    interval: str = Query("1h"),
):
    """
    根據 ATR / 布林通道 / 歷史波動率 回傳三組策略建議。
    每組：upper_price, lower_price, grid_count, spacing, profit_pct
    """
    try:
        klines = _fetch_raw_klines(symbol, interval, limit=100)
        price  = float(klines[-1][4])
        atr    = _calc_atr(klines, 14)
        bb_u, bb_mid, bb_l = _calc_bollinger(klines, 20)
        hv     = _calc_hv(klines, 30)
        regime = _detect_regime(klines, atr, price)

        strategies = _build_strategies(price, atr, bb_u, bb_l, bb_mid, regime)

        return {
            "symbol":       symbol.upper(),
            "price":        price,
            "indicators": {
                "atr_14":        round(atr, 4),
                "atr_pct":       f"{atr / price * 100:.2f}%",
                "bb_upper":      round(bb_u, 4),
                "bb_mid":        round(bb_mid, 4),
                "bb_lower":      round(bb_l, 4),
                "bb_width_pct":  f"{(bb_u - bb_l) / bb_mid * 100:.2f}%" if bb_mid > 0 else "--",
                "hv_30d_pct":   f"{hv:.1f}%",
                "market_regime": regime,
            },
            "strategies": strategies,
        }
    except Exception as e:
        log.warning(f"[strategy_suggest] {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})


# ══ ★ 網格狀態 ═══════════════════════════════════════════════

@app.get("/api/grid_status")
async def grid_status():
    """
    查詢機器人執行狀態 + 所有網格設定（讀取 trading_bot.db）。
    running：依 PID 檔判斷機器人進程是否存活。
    """
    running = False
    if PID_FILE.exists():
        try:
            pid = int(PID_FILE.read_text().strip())
            # Windows 上 os.kill(pid, 0) 會因 Access Denied 而誤判為未啟動；
            # 優先使用 psutil 做跨平台進程存活檢查
            try:
                import psutil
                p = psutil.Process(pid)
                running = p.is_running() and p.status() != psutil.STATUS_ZOMBIE
            except ImportError:
                os.kill(pid, 0)
                running = True
        except (OSError, ValueError, Exception):
            running = False

    # 取最後一筆 RUNNING 設定（或任意最新設定）
    configs = trade_db.get_all_grid_configs()
    last_cfg = None
    if configs:
        running_cfgs = [c for c in configs if c.get("status") == "RUNNING"]
        last_cfg = running_cfgs[-1] if running_cfgs else configs[-1]

    log.info(f"[API] /api/grid_status 讀取設定數量: {len(configs)}, running={running}, last_cfg_id={last_cfg['id'] if last_cfg else None}")

    return {
        "running":     running,
        "last_config": last_cfg,
    }


# ══ ★ 停止網格 ═══════════════════════════════════════════════

@app.post("/api/stop_grid")
async def stop_grid(_: bool = Depends(_check_auth)):
    """發送 SIGTERM 至機器人 PID"""
    import signal as _sig
    if not PID_FILE.exists():
        return JSONResponse({"success": False, "message": "PID 檔不存在，機器人可能未啟動"})
    try:
        pid = int(PID_FILE.read_text().strip())
        os.kill(pid, _sig.SIGTERM)
        log.info(f"[stop_grid] SIGTERM → PID {pid}")
        grid_alerts.notify_bot_stopped(reason="使用者從 UI 停止")
        return {"success": True, "message": f"已傳送停止訊號（PID={pid}）"}
    except ProcessLookupError:
        PID_FILE.unlink(missing_ok=True)
        return JSONResponse({"success": False, "message": "進程不存在（已停止）"})
    except Exception as e:
        return JSONResponse(status_code=500, content={"success": False, "message": str(e)})


# ══ 儲存 / 載入 ═══════════════════════════════════════════════

@app.post("/api/save_grid")
async def save_grid(s: GridSave, _: bool = Depends(_check_auth)):
    """
    儲存網格設定至 trading_bot.db（透過 TradeDB.upsert_grid_config）。
    半自動模式（semi）：驗證當前價格在上下限之間；全自動（auto）跳過此驗證。
    """
    upper = round(s.upper_price, 4)
    lower = round(s.lower_price, 4)
    mode  = s.trade_mode.strip().lower()

    # ── 基本參數驗證 ──────────────────────────────────────────
    if mode not in ("auto", "semi"):
        return JSONResponse(status_code=400, content={"error": "trade_mode 必須為 'auto' 或 'semi'"})

    if mode == "semi":
        if upper <= lower:
            return JSONResponse(status_code=400, content={"error": "上限必須大於下限"})
        if upper - lower < 0.1:
            return JSONResponse(status_code=400, content={"error": "網格區間過小（上下限差距不足 0.1）"})
        if s.grid_count < 2:
            return JSONResponse(status_code=400, content={"error": "網格數量至少 2 格"})

    # ── ① 半自動模式：驗證當前價格在上下限之間 ──────────────
    current_price: float | None = None
    if mode == "semi":
        try:
            px_r = req.get(
                f"{BINANCE_API}/ticker/price",
                params={"symbol": s.symbol.upper()},
                timeout=5,
            )
            px_r.raise_for_status()
            current_price = float(px_r.json()["price"])

            if not (lower < current_price < upper):
                side = "高於上限" if current_price >= upper else "低於下限"
                return JSONResponse(
                    status_code=400,
                    content={
                        "error": (
                            f"當前價格 ${current_price:,.4f} {side}，"
                            f"請調整上限（${upper:,.4f}）或下限（${lower:,.4f}）"
                            f"使網格區間夾住當前價格。"
                        )
                    },
                )
        except req.exceptions.RequestException as e:
            log.warning(f"[save_grid] 無法取得即時價格（{e}），跳過價格驗證")

    # ── ② 止損價格合理性檢查（僅 semi 有意義）────────────────
    sl = s.stop_loss_price
    if sl > 0 and mode == "semi":
        if sl >= lower:
            return JSONResponse(
                status_code=400,
                content={"error": f"止損價格（${sl:,.4f}）必須低於下限（${lower:,.4f}）"},
            )

    # ── ③ 寫入 trading_bot.db（TradeDB.upsert_grid_config）──
    try:
        kwargs: dict = {
            "status":          "RUNNING",
            "trade_mode":      mode,
            "direction":       s.direction,
            "qty_per_grid":    s.qty_per_grid,
            "grid_count":      s.grid_count,
            "stop_loss_price": sl,
        }
        if mode == "semi":
            kwargs["upper_price"] = upper
            kwargs["lower_price"] = lower
        # auto 模式：upper/lower 由機器人 ATR/EMA 自動計算，不覆蓋

        row_id = trade_db.upsert_grid_config(s.symbol.upper(), **kwargs)
    except Exception as e:
        log.error(f"[save_grid] DB 寫入失敗: {e}")
        return JSONResponse(status_code=500, content={"error": f"資料庫寫入失敗：{e}"})

    spacing = (upper - lower) / s.grid_count if mode == "semi" else 0
    mid     = (upper + lower) / 2 if mode == "semi" else 0
    profit  = _profit_pct(spacing, mid) if mode == "semi" else 0

    mode_label = "半自動（固定上下限）" if mode == "semi" else "全自動（ATR/EMA 動態計算）"
    log.info(
        f"[save_grid] #{row_id} {s.symbol} mode={mode}"
        + (f" {lower}–{upper} x{s.grid_count}格 SL={sl or '停用'} 單格淨利={profit:.3f}%" if mode == "semi" else "")
    )

    # Telegram 推播：網格啟動通知
    grid_alerts.notify_bot_started(symbols=[s.symbol.upper()], mode=mode)
    if mode == "semi":
        grid_alerts.notify_grid_deployed(
            symbol=s.symbol.upper(), mode=mode,
            upper=upper, lower=lower, spacing=round(spacing, 4),
            grid_count=s.grid_count, qty=s.qty_per_grid,
        )

    return {
        "status":  "ok",
        "id":      row_id,
        "message": f"✅ [{s.symbol.upper()}] 網格已啟動（{mode_label}，id={row_id}）",
        "data": {
            "symbol":          s.symbol.upper(),
            "direction":       s.direction,
            "trade_mode":      mode,
            "upper":           upper if mode == "semi" else None,
            "lower":           lower if mode == "semi" else None,
            "grid_count":      s.grid_count,
            "stop_loss_price": sl,
            "qty_per_grid":    s.qty_per_grid,
            "spacing":         round(spacing, 4) if mode == "semi" else None,
            "profit_pct":      f"{profit:.3f}%" if mode == "semi" else "由機器人動態計算",
        },
    }


@app.get("/api/history")
async def get_history(limit: int = Query(8, ge=1, le=50)):
    """
    歷史網格設定記錄（讀取 trading_bot.db 的 grid_config 表）。
    依建立時間倒序排列，回傳最近 limit 筆。
    """
    configs = trade_db.get_all_grid_configs()
    # 依 created_at 倒序，截取前 limit 筆
    configs_sorted = sorted(
        configs,
        key=lambda c: c.get("created_at") or "",
        reverse=True,
    )
    result = configs_sorted[:limit]
    log.info(f"[API] /api/history 回傳 {len(result)} 筆歷史紀錄 (總筆數: {len(configs)})")
    return result


# ══ ★ 帳戶餘額 ════════════════════════════════════════════════

@app.get("/api/balance")
async def get_balance():
    """
    查詢 Binance 帳戶 USDT 可用餘額。
    需要 .env 設定 BINANCE_API_KEY / BINANCE_API_SECRET。
    測試網啟用時（BINANCE_TESTNET=true）改用 testnet 端點。
    """
    try:
        testnet = os.getenv("BINANCE_TESTNET", "false").lower() == "true"
        if testnet:
            # Testnet 需要使用不同域名
            api_key    = _API_KEY or os.getenv("BINANCE_API_KEY", "")
            api_secret = _API_SECRET or os.getenv("BINANCE_API_SECRET", "")
            if not api_key or not api_secret:
                return JSONResponse(
                    status_code=503,
                    content={"error": "BINANCE_API_KEY / BINANCE_API_SECRET 未設定"},
                )
            params     = {"timestamp": int(time.time() * 1000)}
            query_str  = "&".join(f"{k}={v}" for k, v in params.items())
            sig        = hmac.new(
                api_secret.encode(), query_str.encode(), hashlib.sha256
            ).hexdigest()
            url = (
                f"https://testnet.binance.vision/api/v3/account"
                f"?{query_str}&signature={sig}"
            )
            r = req.get(url, headers={"X-MBX-APIKEY": api_key}, timeout=8)
        else:
            r = _signed_get("/account")

        r.raise_for_status()
        data = r.json()

        if isinstance(data, dict) and "code" in data:
            return JSONResponse(
                status_code=400,
                content={"error": data.get("msg", "Binance API 錯誤")},
            )

        # 從 balances 陣列中取出 USDT
        balances = {b["asset"]: b for b in data.get("balances", [])}
        usdt = balances.get("USDT", {"free": "0", "locked": "0"})
        return {
            "asset":  "USDT",
            "free":   float(usdt["free"]),
            "locked": float(usdt["locked"]),
            "total":  float(usdt["free"]) + float(usdt["locked"]),
        }

    except PermissionError as e:
        return JSONResponse(status_code=503, content={"error": str(e)})
    except req.exceptions.RequestException as e:
        return JSONResponse(status_code=503, content={"error": f"無法連接 Binance: {e}"})
    except Exception as e:
        log.warning(f"[balance] {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})


# ══ ★ 訂單監控 ════════════════════════════════════════════════

@app.get("/api/open_orders/{symbol}")
async def get_open_orders(symbol: str):
    """
    讀取 trading_bot.db 中目前 PENDING/OPEN 的網格掛單。
    回傳格式：[{price, side, status, order_id, ...}]
    """
    try:
        levels = trade_db.get_open_grid_levels(symbol.upper())
        return {"symbol": symbol.upper(), "orders": levels, "count": len(levels)}
    except Exception as e:
        log.warning(f"[open_orders] {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.get("/api/recent_fills")
async def get_recent_fills(
    limit:  int = Query(20, ge=1, le=100),
    symbol: str = Query(""),
):
    """讀取 trading_bot.db 最近 N 筆成交記錄（可選 symbol 篩選）。"""
    try:
        fills = trade_db.get_recent_grid_trades(limit=limit, symbol=symbol)
        return {"fills": fills, "count": len(fills)}
    except Exception as e:
        log.warning(f"[recent_fills] {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})


# ══ ★ Ping（延遲量測）════════════════════════════════════════

@app.get("/api/ping")
async def ping():
    """延遲量測端點（前端 fetch 並計算 round-trip ms）。"""
    return {"pong": True, "ts": int(time.time() * 1000)}


# ══ ★ 完整帳戶餘額 ════════════════════════════════════════════

@app.get("/api/full_balance")
async def get_full_balance(_: bool = Depends(_check_auth)):
    """
    取得 Binance 帳戶所有非零資產餘額，並嘗試換算 USDT 估值。
    需要 .env 設定 BINANCE_API_KEY / BINANCE_API_SECRET。
    """
    try:
        r = _signed_get("/account")
        r.raise_for_status()
        data = r.json()
        if isinstance(data, dict) and "code" in data:
            return JSONResponse(status_code=400, content={"error": data.get("msg", "Binance 錯誤")})

        raw = [b for b in data.get("balances", [])
               if float(b["free"]) > 0 or float(b["locked"]) > 0]

        result = []
        for b in raw:
            asset  = b["asset"]
            free   = float(b["free"])
            locked = float(b["locked"])
            total  = free + locked
            usd_val: float | None = None
            if asset == "USDT":
                usd_val = total
            elif total > 0:
                try:
                    pr = req.get(f"{BINANCE_API}/ticker/price",
                                 params={"symbol": f"{asset}USDT"}, timeout=3)
                    if pr.ok:
                        usd_val = total * float(pr.json()["price"])
                except Exception:
                    pass
            result.append({
                "asset": asset, "free": free,
                "locked": locked, "total": total,
                "usdt_value": round(usd_val, 4) if usd_val is not None else None,
            })

        result.sort(key=lambda x: x["usdt_value"] or 0, reverse=True)
        return {"balances": result, "count": len(result)}

    except PermissionError as e:
        return JSONResponse(status_code=503, content={"error": str(e)})
    except Exception as e:
        log.warning(f"[full_balance] {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})


# ══ ★ 進階 Kill Switch（直接撤銷 Binance 掛單）══════════════

@app.post("/api/cancel_all/{symbol}")
async def cancel_all_orders(
    symbol: str,
    _: bool = Depends(_check_auth),
):
    """
    進階 Kill Switch：
      1. 呼叫 Binance DELETE /api/v3/openOrders 撤銷該幣種所有掛單
      2. 傳送 SIGTERM 至本地機器人進程
      3. 更新 DB 狀態為 STOPPED
      4. 發送 Telegram 通知
    """
    sym = symbol.upper()
    cancelled_count = 0
    exchange_error: str | None = None

    # ① 撤銷 Binance 交易所掛單
    try:
        dr = _signed_delete("/openOrders", {"symbol": sym})
        if dr.ok:
            cancelled = dr.json()
            cancelled_count = len(cancelled) if isinstance(cancelled, list) else 0
            log.info(f"[KillSwitch] {sym} 已撤銷 {cancelled_count} 筆交易所掛單")
        else:
            exchange_error = dr.json().get("msg", dr.text[:200])
            log.warning(f"[KillSwitch] Binance 撤單失敗: {exchange_error}")
    except PermissionError:
        exchange_error = "未設定 API Key，無法撤銷交易所掛單"
        log.warning(f"[KillSwitch] {exchange_error}")
    except Exception as e:
        exchange_error = str(e)[:200]
        log.warning(f"[KillSwitch] 撤單異常: {e}")

    # ② 停止本地機器人
    local_stopped = False
    if PID_FILE.exists():
        try:
            pid = int(PID_FILE.read_text().strip())
            os.kill(pid, _sig.SIGTERM)
            local_stopped = True
            log.info(f"[KillSwitch] SIGTERM → PID {pid}")
        except Exception as e:
            log.warning(f"[KillSwitch] 無法停止進程: {e}")

    # ③ 更新 DB
    try:
        trade_db.set_grid_status(sym, "STOPPED")
    except Exception:
        pass

    # ④ Telegram 通知
    reason = f"Kill Switch 觸發，撤銷 {cancelled_count} 筆 {sym} 掛單"
    if exchange_error:
        reason += f"（Binance 撤單失敗: {exchange_error}）"
    grid_alerts.notify_bot_stopped(reason=reason)

    return {
        "success": True,
        "symbol": sym,
        "cancelled_count": cancelled_count,
        "local_stopped": local_stopped,
        "exchange_error": exchange_error,
        "message": f"✅ Kill Switch 完成：撤銷 {cancelled_count} 筆掛單，機器人{'已停止' if local_stopped else '未運行'}",
    }


# ══ ★ 半自動人工下單通道（風控攔截）════════════════════════

class ManualTradeRequest(BaseModel):
    symbol: str
    side:   str   # "BUY" or "SELL"
    price:  float
    qty:    float


@app.post("/api/manual_trade")
async def manual_trade(
    order: ManualTradeRequest,
    _: bool = Depends(_check_auth),
):
    """
    半自動模式人工介入下單。
    在呼叫 Binance 真實下單前必須通過雙重風控攔截：
      1. risk_mgr.check_exposure()       — 曝險上限
      2. risk_mgr.check_24h_drawdown()   — 24h 回撤暫停
    兩項都通過後才允許送出限價單。
    """
    symbol = order.symbol.upper()
    side   = order.side.upper()

    if side not in ("BUY", "SELL"):
        return JSONResponse(status_code=400, content={"error": "side 必須為 BUY 或 SELL"})
    if order.price <= 0 or order.qty <= 0:
        return JSONResponse(status_code=400, content={"error": "price 與 qty 必須大於 0"})

    # ── 風控攔截 1：曝險上限 ──────────────────────────────
    proposed_usdt = order.price * order.qty
    try:
        acct_r = _signed_get("/account")
        acct_r.raise_for_status()
        acct_data = acct_r.json()
        balances  = {b["asset"]: float(b["free"]) + float(b["locked"])
                     for b in acct_data.get("balances", [])}
        total_equity = balances.get("USDT", 1000.0)
    except Exception:
        total_equity = float(os.getenv("MANUAL_CAPITAL", "1000"))

    exp_ok, exp_reason, _ = _risk_mgr.check_exposure(symbol, proposed_usdt, total_equity)
    if not exp_ok:
        log.warning(f"[ManualTrade] 曝險攔截: {exp_reason}")
        return JSONResponse(status_code=403, content={"error": f"風控攔截：{exp_reason}"})

    # ── 風控攔截 2：24h 回撤暫停 ──────────────────────────
    dd_ok, dd_status = _risk_mgr.check_24h_drawdown(symbol)
    if not dd_ok:
        log.warning(f"[ManualTrade] 24h 回撤攔截: {dd_status.reason}")
        return JSONResponse(
            status_code=403,
            content={"error": f"風控攔截：24h 回撤超限 — {dd_status.reason}"},
        )

    # ── 下單（LIMIT, GTC）────────────────────────────────
    try:
        r = _signed_post("/order", {
            "symbol":      symbol,
            "side":        side,
            "type":        "LIMIT",
            "timeInForce": "GTC",
            "quantity":    order.qty,
            "price":       order.price,
        })
        r.raise_for_status()
        result = r.json()
        if isinstance(result, dict) and "code" in result:
            return JSONResponse(
                status_code=400,
                content={"error": result.get("msg", "Binance 下單失敗")},
            )
        log.info(
            f"[ManualTrade] {side} {symbol} "
            f"qty={order.qty} price={order.price} → orderId={result.get('orderId')}"
        )
        return {
            "success":  True,
            "order_id": result.get("orderId"),
            "symbol":   symbol,
            "side":     side,
            "price":    order.price,
            "qty":      order.qty,
            "status":   result.get("status"),
            "message":  f"✅ 手動 {side} {symbol} 下單成功（orderId={result.get('orderId')}）",
        }
    except PermissionError as e:
        return JSONResponse(status_code=503, content={"error": str(e)})
    except req.exceptions.RequestException as e:
        return JSONResponse(status_code=503, content={"error": f"無法連接 Binance: {e}"})
    except Exception as e:
        log.error(f"[ManualTrade] 下單失敗: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})


# ══ ★ 系統健康檢查 ════════════════════════════════════════════

@app.get("/api/health_check")
async def health_check():
    """
    系統自我診斷端點，用於排查「有畫面沒資料」問題。
    回傳四項診斷結果：
      - .env 狀態         ：API 金鑰是否存在
      - 資料庫狀態        ：SQLite 是否可正常讀取
      - 機器人 PID        ：PID 檔案 + 進程存活
      - Binance API 連線  ：是否能 ping 通幣安
    """
    result: dict = {}

    # ① .env API 金鑰
    key_ok    = bool(_API_KEY)
    secret_ok = bool(_API_SECRET)
    result[".env 狀態"] = {
        "BINANCE_API_KEY":    "✅ 已設定" if key_ok    else "❌ 未設定",
        "BINANCE_API_SECRET": "✅ 已設定" if secret_ok else "❌ 未設定",
        "ok": key_ok and secret_ok,
    }

    # ② 資料庫連線
    try:
        configs = trade_db.get_all_grid_configs()
        result["資料庫狀態"] = {
            "path":   db_abs_path,
            "ok":     True,
            "detail": f"✅ 正常（grid_config 筆數: {len(configs)}）",
        }
    except Exception as e:
        result["資料庫狀態"] = {"ok": False, "detail": f"❌ 讀取失敗: {e}"}

    # ③ 機器人 PID
    pid_exists = PID_FILE.exists()
    bot_alive  = False
    bot_detail = "❌ PID 檔不存在"
    if pid_exists:
        try:
            pid = int(PID_FILE.read_text().strip())
            try:
                import psutil
                p = psutil.Process(pid)
                bot_alive  = p.is_running() and p.status() != psutil.STATUS_ZOMBIE
                bot_detail = f"✅ run_bot.py 存活（PID={pid}）" if bot_alive else f"❌ PID={pid} 進程已終止"
            except ImportError:
                os.kill(pid, 0)
                bot_alive  = True
                bot_detail = f"✅ PID={pid} 存活（psutil 未安裝，以 os.kill 確認）"
        except (OSError, ValueError):
            bot_detail = f"❌ PID 檔存在但進程不存活"
    result["機器人 PID"] = {
        "pid_file": str(PID_FILE),
        "ok":       bot_alive,
        "detail":   bot_detail,
    }

    # ④ Binance API 連線
    try:
        ping_r = req.get(f"{BINANCE_API}/ping", timeout=5)
        if ping_r.ok:
            result["Binance API 連線"] = {"ok": True, "detail": "✅ Binance /api/v3/ping 回應正常"}
        else:
            result["Binance API 連線"] = {
                "ok": False,
                "detail": f"❌ Binance 回應異常（HTTP {ping_r.status_code}）",
            }
    except req.exceptions.ConnectionError:
        result["Binance API 連線"] = {"ok": False, "detail": "❌ 無法連接 Binance（網路問題）"}
    except req.exceptions.Timeout:
        result["Binance API 連線"] = {"ok": False, "detail": "❌ Binance 請求逾時"}
    except Exception as e:
        result["Binance API 連線"] = {"ok": False, "detail": f"❌ 未知錯誤: {e}"}

    all_ok = all(v.get("ok", False) for v in result.values())
    return {"overall_ok": all_ok, "checks": result}


# ════════════════════════════════════════════════════════════
# WebSocket 代理（Binance Kline Stream → 瀏覽器）
# ════════════════════════════════════════════════════════════

@app.websocket("/ws/kline/{symbol}")
async def kline_ws_proxy(client: WebSocket, symbol: str, interval: str = Query("15m")):
    await client.accept()
    binance_url = f"{BINANCE_WS}/{symbol.lower()}@kline_{interval}"
    log.info(f"[WS] 代理啟動 {symbol}/{interval}")

    try:
        # open_timeout=10：若 Binance WS 握手超過 10 秒則拋出例外，
        # 避免 proxy 永久掛起導致瀏覽器端 WS 停在「連線中…」
        async with websockets.connect(
            binance_url,
            ping_interval=20,
            ping_timeout=15,
            open_timeout=10,
        ) as bws:
            while True:
                try:
                    msg = await asyncio.wait_for(bws.recv(), timeout=60)
                    await client.send_text(msg)
                except asyncio.TimeoutError:
                    await bws.ping()
                except (WebSocketDisconnect, Exception):
                    break
    except Exception as e:
        log.warning(f"[WS] 代理錯誤：{symbol} — {e}")
        # 連線失敗時主動推送錯誤訊息給前端，讓前端切換到輪詢備援
        try:
            await client.send_text(f"[WS_ERROR:{e}]")
        except Exception:
            pass
    finally:
        try:
            await client.close()
        except Exception:
            pass


# ════════════════════════════════════════════════════════════
# WebSocket：即時日誌串流
# ════════════════════════════════════════════════════════════

@app.websocket("/ws/logs")
async def logs_ws_stream(client: WebSocket):
    """
    即時串流 logs/grid_bot.log 到瀏覽器。
    連線後先推送最後 80 行，之後持續 tail 新行。
    """
    await client.accept()
    log_path = Path(__file__).parent / "logs" / "grid_bot.log"
    log.info("[LogWS] 客戶端連接")

    try:
        if not log_path.exists():
            await client.send_text("⚠️  日誌檔案不存在，請先啟動交易機器人")
            await client.close()
            return

        # ── 先推送最後 80 行（歷史）────────────────────────────
        with open(log_path, "r", encoding="utf-8", errors="replace") as f:
            lines = f.readlines()
        for line in lines[-80:]:
            stripped = line.rstrip()
            if stripped:
                await client.send_text(stripped)

        # ── 持續 tail 新行 ─────────────────────────────────────
        with open(log_path, "r", encoding="utf-8", errors="replace") as f:
            f.seek(0, 2)  # 跳到檔案末尾
            while True:
                line = f.readline()
                if line:
                    stripped = line.rstrip()
                    if stripped:
                        await client.send_text(stripped)
                else:
                    await asyncio.sleep(0.3)
    except WebSocketDisconnect:
        log.info("[LogWS] 客戶端斷線")
    except Exception as e:
        log.warning(f"[LogWS] 錯誤: {e}")
        try:
            await client.send_text(f"[串流錯誤: {e}]")
            await client.close()
        except Exception:
            pass


# ════════════════════════════════════════════════════════════
# 啟動入口
# ════════════════════════════════════════════════════════════
if __name__ == "__main__":
    import socket, subprocess

    _PORT = int(os.getenv("WEB_PORT", "8000"))

    # 若 port 被占用，嘗試找出並終止舊進程
    def _port_in_use(port: int) -> bool:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(0.5)
            return s.connect_ex(("127.0.0.1", port)) == 0

    if _port_in_use(_PORT):
        try:
            result = subprocess.run(
                ["netstat", "-ano"],
                capture_output=True, text=True, timeout=5
            )
            for line in result.stdout.splitlines():
                if f":{_PORT}" in line and "LISTENING" in line:
                    pid = line.strip().split()[-1]
                    subprocess.run(["taskkill", "/F", "/PID", pid],
                                   capture_output=True, timeout=5)
                    print(f"[Web] 已終止占用 port {_PORT} 的舊進程 (PID {pid})")
                    time.sleep(0.5)
                    break
        except Exception as e:
            print(f"[Web] 無法自動釋放 port {_PORT}: {e}，請手動關閉舊進程")

    print("=" * 60)
    print("  AQT  AQT 半自動動態網格交易控制台 v3.0")
    print(f"  >>  瀏覽器：http://localhost:{_PORT}")
    print(f"  >>  API 文件：http://localhost:{_PORT}/docs")
    print("=" * 60)
    uvicorn.run(app, host="127.0.0.1", port=_PORT, reload=False)
