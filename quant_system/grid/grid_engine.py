"""
quant_system/grid/grid_engine.py — 動態網格核心計算引擎（Phase F 強化版）

公式：
  中軸  Pc      = EMA(close, ema_period)
  ATR   ATR_t   = ATR(atr_period)
  格距  ΔP      = max(k × ATR_t, tick_size × 2)     ← ① Zero-Delta Protection
  上界  P_upper = Pc + m × ATR_t
  下界  P_lower = Pc - m × ATR_t
  格數  N       = floor((P_upper - P_lower) / ΔP)   → 約 2m/k 格

Phase F 新增功能：
  ① Zero-Delta Protection  — spacing 加 tick_size × 2 下限，防盤整期格距趨零
  ② Auto-Compounding       — compounding_ratio 參數，regrid 時按 total_equity 放大 qty
  ③ Fee Spacing Guard      — 驗證 spacing/price > fee_rate×2 + 0.05%，不足則自動擴距
  ⑤ MarketType 擴充點      — compute() 接受 market_type，為合約介面保留 TODO

依賴：
  numpy, pandas（requirements.txt 已包含）
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)

# ── 手續費保護預設值 ──────────────────────────────────────────
_DEFAULT_FEE_RATE    = 0.001    # Binance Maker 0.1%
_FEE_BUFFER          = 0.0005   # 額外 0.05% 緩衝
_MIN_PROFITABLE_MULT = 2.0      # spacing/price 必須 > fee_rate × _MIN_PROFITABLE_MULT + buffer


@dataclass
class GridLevel:
    index:        int      # 0 = 最下層
    price:        float    # 掛單價格
    side:         str      # "BUY" | "SELL"
    order_id:     str  = ""
    status:       str  = "PENDING"   # PENDING | OPEN | FILLED | CANCELLED
    filled_price: float = 0.0
    filled_qty:   float = 0.0


@dataclass
class GridSnapshot:
    center:      float
    upper:       float
    lower:       float
    spacing:     float
    atr:         float
    levels:      List[GridLevel] = field(default_factory=list)
    grid_count:  int = 0
    # ① 零格距防護相關
    spacing_source: str = "atr"   # "atr" | "tick_floor" | "fee_floor"
    # ③ 手續費驗證
    fee_ok:      bool = True


class GridEngine:
    """
    根據即時 OHLCV 資料計算動態網格參數。
    不負責下單，只負責「計算應該在哪裡掛單」。

    Phase F 新增參數：
      tick_size        — 交易對最小價格單位（① Zero-Delta 下限）
      fee_rate         — 手續費率（③ Fee Guard 計算基礎）
      compounding_ratio— 複利放大比例（② Auto-Compounding，預設 0 = 關閉）
      base_qty         — 初始每格交易數量（② 複利基準值）
    """

    def __init__(
        self,
        k:                 float = 0.5,    # 格距倍數
        m:                 float = 3.0,    # 上下界倍數
        atr_period:        int   = 14,
        ema_period:        int   = 20,
        max_levels:        int   = 20,     # 最大格數上限
        min_levels:        int   = 4,      # 最少格數
        # ① Zero-Delta Protection
        tick_size:         float = 0.0,    # 0 = 不啟用下限保護
        # ③ Fee Spacing Guard
        fee_rate:          float = _DEFAULT_FEE_RATE,
        enforce_fee_guard: bool  = True,   # 是否強制手續費驗證
        # ② Auto-Compounding（在 GridManager 層使用，此處只保留 base_qty 基準）
        base_qty:          float = 0.0,    # 由 GridManager 管理；此處僅作資訊用
        compounding_ratio: float = 0.0,    # 0.0 = 關閉複利；0.1 = 每次 regrid qty × (1 + 0.1 × equity_growth)
    ) -> None:
        self.k                  = k
        self.m                  = m
        self.atr_period         = atr_period
        self.ema_period         = ema_period
        self.max_levels         = max_levels
        self.min_levels         = min_levels
        self.tick_size          = tick_size
        self.fee_rate           = fee_rate
        self.enforce_fee_guard  = enforce_fee_guard
        self.base_qty           = base_qty
        self.compounding_ratio  = compounding_ratio
        self._last_atr: float   = 0.0

    # ── 主計算 ───────────────────────────────────────────────
    def compute(
        self,
        df:          pd.DataFrame,
        symbol_info: Optional[Dict] = None,
        market_type: str            = "spot",   # ⑤ "spot" | "usdm" — 預留合約擴充點
    ) -> Optional[GridSnapshot]:
        """
        給定 OHLCV DataFrame，回傳完整網格快照。

        Args:
            df          — 含 Open/High/Low/Close/Volume 欄位的 DataFrame
            symbol_info — Binance exchangeInfo 的交易對規則（含 tick_size, step_size 等）
            market_type — 市場類型（⑤ 預留合約介面：spot / usdm）
        """
        # ⑤ USDⓈ-M 合約擴充點
        if market_type == "usdm":
            # TODO Phase G: 載入合約專用參數（funding_rate, leverage_cap 等）
            # 目前降級為現貨邏輯，不影響現有功能
            log.info("[Grid] market_type=usdm（合約介面預留，目前以現貨邏輯執行）")

        if len(df) < max(self.atr_period, self.ema_period) + 5:
            log.warning("[Grid] 資料不足，無法計算網格")
            return None

        atr     = self._atr(df)
        center  = self._ema(df["Close"], self.ema_period)
        upper   = center + self.m * atr
        lower   = center - self.m * atr

        # ──────────────────────────────────────────────────
        # ① Zero-Delta Protection
        #   取 symbol_info['tick_size'] 或建構時傳入的 tick_size
        # ──────────────────────────────────────────────────
        effective_tick = self.tick_size
        if symbol_info:
            for f in symbol_info.get("filters", []):
                if f.get("filterType") == "PRICE_FILTER":
                    try:
                        effective_tick = float(f["tickSize"])
                    except (KeyError, ValueError):
                        pass
                    break

        raw_spacing   = self.k * atr
        tick_floor    = effective_tick * 2 if effective_tick > 0 else 0.0
        spacing       = max(raw_spacing, tick_floor) if tick_floor > 0 else raw_spacing
        spacing_source = (
            "tick_floor" if (tick_floor > 0 and raw_spacing < tick_floor)
            else "atr"
        )

        if spacing <= 0:
            log.warning("[Grid] 格距仍為零（ATR=0 且 tick_size=0），無法建立網格")
            return None

        if raw_spacing < tick_floor:
            log.info(
                f"[Grid] ① Zero-Delta: ATR 格距 {raw_spacing:.8f} < tick×2 {tick_floor:.8f}，"
                f"強制使用 {spacing:.8f}"
            )

        # ──────────────────────────────────────────────────
        # ③ Fee Spacing Guard
        #   spacing / center > fee_rate × 2 + 0.05%
        # ──────────────────────────────────────────────────
        fee_ok = True
        fee_floor_spacing = center * (self.fee_rate * _MIN_PROFITABLE_MULT + _FEE_BUFFER)
        if self.enforce_fee_guard and spacing < fee_floor_spacing:
            old_spacing = spacing
            spacing     = fee_floor_spacing
            spacing_source = "fee_floor"
            fee_ok      = False   # 標記：格距被手續費規則撐大
            log.warning(
                f"[Grid] ③ Fee Guard: 格距 {old_spacing:.6f} < 手續費下限 {fee_floor_spacing:.6f} "
                f"(fee={self.fee_rate:.3%}×2 + {_FEE_BUFFER:.3%})，"
                f"自動擴大至 {spacing:.6f}"
            )

        # 計算格位
        n = int((upper - lower) / spacing)
        n = max(self.min_levels, min(n, self.max_levels))

        levels: List[GridLevel] = []
        for i in range(n + 1):
            price = lower + i * spacing
            side  = "BUY" if price < center else "SELL"
            levels.append(GridLevel(index=i, price=round(price, 8), side=side))

        self._last_atr = atr
        snap = GridSnapshot(
            center         = round(center, 8),
            upper          = round(upper, 8),
            lower          = round(lower, 8),
            spacing        = round(spacing, 8),
            atr            = round(atr, 8),
            levels         = levels,
            grid_count     = len(levels),
            spacing_source = spacing_source,
            fee_ok         = fee_ok,
        )
        log.info(
            f"[Grid] 計算完成 center={center:.4f} ±{self.m}×ATR={atr:.6f} "
            f"spacing={spacing:.6f}({spacing_source}) levels={len(levels)} "
            f"fee_ok={fee_ok}"
        )
        return snap

    # ── 重新鋪設判斷 ─────────────────────────────────────────
    def should_regrid(self, df: pd.DataFrame, threshold: float = 0.05) -> bool:
        """
        ATR 變化超過 threshold（預設 5%）才需要重新鋪設網格，
        避免頻繁撤單/掛單觸發 Binance 頻率限制。
        """
        if self._last_atr == 0:
            return True
        new_atr = self._atr(df)
        change  = abs(new_atr - self._last_atr) / (self._last_atr + 1e-9)
        return change >= threshold

    # ── ② Auto-Compounding 工具（供 GridManager 呼叫）────────
    def compute_compounded_qty(
        self,
        base_qty:      float,
        base_equity:   float,
        current_equity: float,
    ) -> float:
        """
        ② 根據帳戶資金成長幅度，計算複利後的每格交易數量。

        公式：
          growth = (current_equity - base_equity) / base_equity
          new_qty = base_qty × (1 + compounding_ratio × max(growth, 0))

        growth 為負（虧損）時不縮減 qty（保守策略），僅在盈利時放大。

        Args:
            base_qty       — 初始每格數量（首次部署時的 qty_per_grid）
            base_equity    — 首次部署時的帳戶總資金
            current_equity — 本次 regrid 時的帳戶總資金

        Returns:
            複利放大後的 qty_per_grid（若 compounding_ratio=0 則原樣回傳）
        """
        if self.compounding_ratio <= 0 or base_equity <= 0:
            return base_qty

        growth  = (current_equity - base_equity) / base_equity
        growth  = max(growth, 0.0)   # 虧損時不縮減
        new_qty = base_qty * (1.0 + self.compounding_ratio * growth)
        log.info(
            f"[Grid] ② Compounding: equity {base_equity:.2f}→{current_equity:.2f} "
            f"growth={growth:.2%} qty {base_qty:.6f}→{new_qty:.6f}"
        )
        return new_qty

    # ── 靜態輔助 ─────────────────────────────────────────────
    @staticmethod
    def _ema(series: pd.Series, span: int) -> float:
        return float(series.ewm(span=span, adjust=False).mean().iloc[-1])

    def _atr(self, df: pd.DataFrame) -> float:
        hi = df["High"].values.astype(float)
        lo = df["Low"].values.astype(float)
        cl = df["Close"].values.astype(float)
        n  = self.atr_period
        trs = [
            max(hi[i] - lo[i], abs(hi[i] - cl[i-1]), abs(lo[i] - cl[i-1]))
            for i in range(1, len(cl))
        ]
        return float(sum(trs[-n:]) / min(n, len(trs))) if trs else cl[-1] * 0.01
