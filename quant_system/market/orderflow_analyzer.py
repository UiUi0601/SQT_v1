"""
quant_system/market/orderflow_analyzer.py — 訂單流失衡分析器（OBI）

核心指標：
  OBI (Order Book Imbalance) = (bid_vol - ask_vol) / (bid_vol + ask_vol)
    range: [-1.0, 1.0]
    +1.0 → 完全買方壓力；-1.0 → 完全賣方壓力；0.0 → 均衡

演算法：
  ─ 監聽 WebSocket @depth20@100ms（20 檔，100ms 更新）
  ─ 取前 10 檔買賣掛單的總量計算 OBI
  ─ 計算在獨立執行緒（ThreadPoolExecutor）以避免阻塞 Event Loop
  ─ 維護滑動視窗（預設 60 秒）的 OBI 歷史，計算均值與標準差
  ─ 支援多 symbol 並行監聽（每個 symbol 一個 WebSocket 串流）

OBI 衍生指標：
  obi_mean    — 滑動視窗均值（平滑短期噪音）
  obi_std     — 標準差（衡量盤面動蕩程度）
  obi_z       — Z-score：(obi - mean) / std（標準化偏離程度）
  absorption  — 吸收比：大單被吃掉 vs 新掛單（捕捉隱性流動性）

使用範例：
    analyzer = OrderFlowAnalyzer(symbols=["BTCUSDT", "ETHUSDT"])
    analyzer.start()

    # 取得最新 OBI（非阻塞）
    snapshot = analyzer.get_snapshot("BTCUSDT")
    print(f"OBI={snapshot.obi:.4f}  Z={snapshot.obi_z:.2f}")

    # 輸出 ScoringEngine market_ctx
    ctx = analyzer.to_scoring_ctx("BTCUSDT")
    # {"obi": 0.35}
"""
from __future__ import annotations

import asyncio
import json
import logging
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any, Deque, Dict, List, Optional

import numpy as np

log = logging.getLogger(__name__)


# ════════════════════════════════════════════════════════════
# 資料類別
# ════════════════════════════════════════════════════════════
@dataclass
class OrderBookSnapshot:
    """單一時間點的訂單簿 OBI 快照"""
    symbol:   str
    obi:      float      # [-1.0, 1.0] 即時 OBI
    obi_mean: float      # 滑動視窗均值
    obi_std:  float      # 滑動視窗標準差
    obi_z:    float      # Z-score
    bid_vol:  float      # 前 N 檔總買量
    ask_vol:  float      # 前 N 檔總賣量
    spread:   float      # 買賣價差（最佳 bid/ask 之差）
    mid_price: float     # 中間價
    sample_count: int    # 滑動視窗內的樣本數
    timestamp: float = field(default_factory=time.time)

    @property
    def is_strong_buy(self) -> bool:
        return self.obi > 0.3 and self.obi_z > 1.5

    @property
    def is_strong_sell(self) -> bool:
        return self.obi < -0.3 and self.obi_z < -1.5

    @property
    def has_data(self) -> bool:
        return self.sample_count > 0


_NULL_SNAPSHOT = OrderBookSnapshot(
    symbol="", obi=0.0, obi_mean=0.0, obi_std=0.0, obi_z=0.0,
    bid_vol=0.0, ask_vol=0.0, spread=0.0, mid_price=0.0, sample_count=0
)


# ════════════════════════════════════════════════════════════
# OrderFlowAnalyzer
# ════════════════════════════════════════════════════════════
class OrderFlowAnalyzer:
    """
    訂單流失衡分析器。

    監聽合約（FUTURES）@depth20@100ms WebSocket 串流，
    計算 OBI 並維護滑動視窗統計。

    計算在 ThreadPoolExecutor 中執行（避免阻塞 WebSocket Event Loop）。
    """

    _WS_FUTURES_COMBINED = "wss://fstream.binance.com/stream?streams="

    def __init__(
        self,
        symbols:         List[str],
        top_n:           int   = 10,       # 取前幾檔計算 OBI
        window_sec:      float = 60.0,     # 滑動視窗長度（秒）
        max_history:     int   = 600,      # 每個 symbol 最多保存的 OBI 樣本數
        use_spot:        bool  = False,    # True = 現貨訂單簿；False = 合約（預設）
    ) -> None:
        self._symbols   = [s.upper() for s in symbols]
        self._top_n     = top_n
        self._window    = window_sec
        self._use_spot  = use_spot

        # OBI 歷史：{symbol: deque[(timestamp, obi)]}
        self._history: Dict[str, Deque] = {
            sym: deque(maxlen=max_history) for sym in self._symbols
        }
        self._snapshots: Dict[str, OrderBookSnapshot] = {}
        self._lock = threading.Lock()

        # 計算執行緒池（向量化計算丟入此池，不阻塞 WS 事件迴圈）
        self._executor    = ThreadPoolExecutor(max_workers=2, thread_name_prefix="OBI")
        self._ws_thread:  Optional[threading.Thread] = None
        self._stop_event  = threading.Event()
        self._ws_loop:    Optional[asyncio.AbstractEventLoop] = None

    # ════════════════════════════════════════════════════════
    # 啟動 / 停止
    # ════════════════════════════════════════════════════════
    def start(self) -> None:
        if self._ws_thread and self._ws_thread.is_alive():
            log.warning("[OBI] 已在執行中")
            return
        self._stop_event.clear()
        self._ws_thread = threading.Thread(
            target=self._run_loop,
            daemon=True,
            name="OrderFlowAnalyzer",
        )
        self._ws_thread.start()
        log.info(f"[OBI] 啟動，監聽 {self._symbols}")

    def stop(self) -> None:
        self._stop_event.set()
        if self._ws_loop and self._ws_loop.is_running():
            self._ws_loop.call_soon_threadsafe(self._ws_loop.stop)
        if self._ws_thread:
            self._ws_thread.join(timeout=10.0)
        self._executor.shutdown(wait=False)
        log.info("[OBI] 已停止")

    # ════════════════════════════════════════════════════════
    # 查詢介面
    # ════════════════════════════════════════════════════════
    def get_snapshot(self, symbol: str) -> OrderBookSnapshot:
        """取得指定交易對的最新 OBI 快照（非阻塞，無資料時回傳空快照）。"""
        with self._lock:
            snap = self._snapshots.get(symbol.upper())
        if snap is None:
            return OrderBookSnapshot(
                symbol=symbol.upper(), obi=0.0, obi_mean=0.0, obi_std=0.0,
                obi_z=0.0, bid_vol=0.0, ask_vol=0.0, spread=0.0,
                mid_price=0.0, sample_count=0
            )
        return snap

    def get_obi(self, symbol: str) -> float:
        """快速取得當前 OBI 值（即時，非滑動均值）。"""
        return self.get_snapshot(symbol).obi

    def to_scoring_ctx(self, symbol: str) -> dict:
        """輸出 ScoringEngine market_ctx 子字典。"""
        snap = self.get_snapshot(symbol)
        if not snap.has_data:
            return {}
        # 使用滑動均值（更穩定，過濾瞬間噪音）
        obi_val = snap.obi_mean if snap.sample_count >= 5 else snap.obi
        return {"obi": obi_val}

    # ════════════════════════════════════════════════════════
    # 內部：執行緒與事件迴圈
    # ════════════════════════════════════════════════════════
    def _run_loop(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._ws_loop = loop
        try:
            loop.run_until_complete(self._ws_connect_combined())
        except Exception as e:
            log.error(f"[OBI] 事件迴圈異常: {e}", exc_info=True)
        finally:
            loop.close()

    async def _ws_connect_combined(self) -> None:
        """使用 Combined Stream 同時監聽多個 symbol 的 depth20@100ms。"""
        try:
            import websockets
        except ImportError:
            log.error("[OBI] websockets 未安裝，請執行 pip install websockets")
            return

        # 組合串流 URL
        if self._use_spot:
            base_url = "wss://stream.binance.com:9443/stream?streams="
            stream_suffix = "@depth20@100ms"
        else:
            base_url = "wss://fstream.binance.com/stream?streams="
            stream_suffix = "@depth20@100ms"

        streams = "/".join(
            f"{sym.lower()}{stream_suffix}" for sym in self._symbols
        )
        url = f"{base_url}{streams}"

        while not self._stop_event.is_set():
            try:
                async with websockets.connect(
                    url,
                    ping_interval=20,
                    ping_timeout=10,
                    close_timeout=5,
                ) as ws:
                    log.info(f"[OBI] 已連接 depth 串流：{self._symbols}")
                    async for raw_msg in ws:
                        if self._stop_event.is_set():
                            break
                        # 丟入執行緒池計算，避免阻塞 Event Loop
                        asyncio.get_event_loop().run_in_executor(
                            self._executor,
                            self._process_message,
                            raw_msg,
                        )
            except Exception as e:
                log.warning(f"[OBI] WS 斷線: {e}，3s 後重連...")
                await asyncio.sleep(3)

    # ════════════════════════════════════════════════════════
    # 內部：訊息解析與 OBI 計算
    # ════════════════════════════════════════════════════════
    def _process_message(self, raw: str) -> None:
        """在執行緒池中解析訊息並計算 OBI（向量化）。"""
        try:
            msg = json.loads(raw)
            # Combined Stream 格式
            data = msg.get("data", msg)

            # 解析 bids / asks
            bids = data.get("b", [])   # [[price, qty], ...]
            asks = data.get("a", [])   # [[price, qty], ...]
            if not bids or not asks:
                return

            # 取前 N 檔（已排序：bids 由高到低，asks 由低到高）
            n       = self._top_n
            b_arr   = np.array(bids[:n], dtype=float)
            a_arr   = np.array(asks[:n], dtype=float)

            bid_vol = float(np.sum(b_arr[:, 1]))   # 買量總和
            ask_vol = float(np.sum(a_arr[:, 1]))   # 賣量總和

            total = bid_vol + ask_vol
            if total < 1e-12:
                return

            obi = (bid_vol - ask_vol) / total   # [-1, 1]

            # 價格資訊
            best_bid  = float(b_arr[0, 0]) if len(b_arr) > 0 else 0.0
            best_ask  = float(a_arr[0, 0]) if len(a_arr) > 0 else 0.0
            spread    = best_ask - best_bid
            mid_price = (best_bid + best_ask) / 2

            # 推算 symbol（從 stream 名稱）
            stream = msg.get("stream", "")
            if stream:
                symbol = stream.split("@")[0].upper()
            else:
                # 無 Combined Stream 包裝，嘗試用第一個 symbol
                symbol = self._symbols[0] if self._symbols else "UNKNOWN"

            # 更新歷史
            now = time.time()
            with self._lock:
                hist = self._history.get(symbol)
                if hist is None:
                    self._history[symbol] = deque(maxlen=600)
                    hist = self._history[symbol]
                hist.append((now, obi))

                # 清除視窗外的舊資料並計算統計
                cutoff = now - self._window
                while hist and hist[0][0] < cutoff:
                    hist.popleft()

                if len(hist) >= 2:
                    obi_arr  = np.array([x[1] for x in hist], dtype=float)
                    obi_mean = float(np.mean(obi_arr))
                    obi_std  = float(np.std(obi_arr)) or 1e-9
                    obi_z    = (obi - obi_mean) / obi_std
                else:
                    obi_mean = obi
                    obi_std  = 0.0
                    obi_z    = 0.0

                self._snapshots[symbol] = OrderBookSnapshot(
                    symbol       = symbol,
                    obi          = obi,
                    obi_mean     = obi_mean,
                    obi_std      = obi_std,
                    obi_z        = obi_z,
                    bid_vol      = bid_vol,
                    ask_vol      = ask_vol,
                    spread       = spread,
                    mid_price    = mid_price,
                    sample_count = len(hist),
                    timestamp    = now,
                )
        except Exception as e:
            log.debug(f"[OBI] 訊息處理失敗: {e}")
