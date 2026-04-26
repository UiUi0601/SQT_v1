"""
quant_system/market/volume_profile.py — 成交量分佈分析（Volume Profile）

計算指標：
  POC (Point of Control)   — 成交量最大的價格節點
  VAH (Value Area High)    — 價值區上緣（覆蓋 70% 成交量的最高價）
  VAL (Value Area Low)     — 價值區下緣（覆蓋 70% 成交量的最低價）

演算法：
  1. 將指定周期的 [Low, High] 範圍等分為 N 個 bin（預設 100）
  2. 每根 K 線的成交量均勻分配到其 High-Low 範圍覆蓋的 bin
  3. 找出成交量最大的 bin 中心作為 POC
  4. 從 POC 向兩側擴展，直到涵蓋總成交量的 value_area_pct（預設 70%）

效能：使用 numpy 向量化，避免 Python 迴圈，快取結果直到 K 線更新。

使用範例：
    from quant_system.market.volume_profile import VolumeProfile

    vp   = VolumeProfile(bins=100, value_area_pct=0.70)
    result = vp.compute(df)     # df 需含 High, Low, Close, Volume 欄位

    print(f"POC={result.poc:.2f}  VAH={result.vah:.2f}  VAL={result.val:.2f}")

    # 當前價格相對 Value Area 的位置
    pos = vp.price_position(df["Close"].iloc[-1], result)
    # "ABOVE_VAH" | "INSIDE_VA" | "BELOW_VAL" | "AT_POC"
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Optional

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)


# ════════════════════════════════════════════════════════════
# 結果資料類別
# ════════════════════════════════════════════════════════════
@dataclass
class VolumeProfileResult:
    poc: float          # Point of Control（最大成交量節點價格）
    vah: float          # Value Area High
    val: float          # Value Area Low
    total_volume: float
    value_area_pct: float      # 實際涵蓋的成交量比例
    price_range_low:  float    # 分析範圍下緣
    price_range_high: float    # 分析範圍上緣
    bin_count: int
    computed_at: float = 0.0   # Unix timestamp

    @property
    def value_area_width(self) -> float:
        return self.vah - self.val

    @property
    def is_wide_va(self) -> bool:
        """Value Area 是否寬廣（相對區間 > 40%），暗示行情無明確方向"""
        if self.price_range_high <= self.price_range_low:
            return False
        ratio = self.value_area_width / (self.price_range_high - self.price_range_low)
        return ratio > 0.40

    def price_position(self, price: float) -> str:
        """
        判斷價格相對 Value Area 的位置：
          ABOVE_VAH | AT_POC | INSIDE_VA | BELOW_VAL
        """
        poc_tol = (self.vah - self.val) * 0.03   # POC 附近 3% 容忍帶
        if abs(price - self.poc) <= poc_tol:
            return "AT_POC"
        if price > self.vah:
            return "ABOVE_VAH"
        if price < self.val:
            return "BELOW_VAL"
        return "INSIDE_VA"


# ════════════════════════════════════════════════════════════
# VolumeProfile
# ════════════════════════════════════════════════════════════
class VolumeProfile:
    """
    基於 K 線 OHLCV 資料計算成交量分佈（Volume Profile）。

    使用 numpy 向量化運算，避免 Python 迴圈造成效能瓶頸。
    支援結果快取，相同資料不重複計算。
    """

    def __init__(
        self,
        bins:           int   = 100,
        value_area_pct: float = 0.70,
    ) -> None:
        """
        bins:           價格軸分割數量（越多 = 精度越高，計算越慢）
        value_area_pct: Value Area 涵蓋成交量百分比（預設 70%）
        """
        self._bins           = bins
        self._value_area_pct = value_area_pct
        self._cache:         Optional[VolumeProfileResult] = None
        self._cache_key:     Optional[int] = None   # 以 len(df) + last_close hash 作為快取鍵

    # ════════════════════════════════════════════════════════
    # 主計算入口
    # ════════════════════════════════════════════════════════
    def compute(
        self,
        df:      pd.DataFrame,
        force:   bool = False,
    ) -> Optional[VolumeProfileResult]:
        """
        計算 Volume Profile。

        df 必須包含 'High'、'Low'、'Volume' 欄位（不分大小寫）。
        force=True 強制重新計算（忽略快取）。
        資料不足（< 10 根）或無成交量欄位時回傳 None。
        """
        if df is None or len(df) < 10:
            log.debug("[VP] K線不足 10 根，跳過")
            return None

        # ── 欄位標準化 ───────────────────────────────────────
        col_map = {c.lower(): c for c in df.columns}
        high_col   = col_map.get("high")
        low_col    = col_map.get("low")
        vol_col    = col_map.get("volume")
        if not (high_col and low_col and vol_col):
            log.warning("[VP] DataFrame 缺少 High/Low/Volume 欄位")
            return None

        # ── 快取檢查 ─────────────────────────────────────────
        cache_key = self._make_cache_key(df, vol_col)
        if not force and self._cache and self._cache_key == cache_key:
            return self._cache

        t0 = time.perf_counter()

        high_arr = df[high_col].values.astype(float)
        low_arr  = df[low_col].values.astype(float)
        vol_arr  = df[vol_col].values.astype(float)

        # ── 建立價格 bin ─────────────────────────────────────
        price_min = float(low_arr.min())
        price_max = float(high_arr.max())
        if price_max <= price_min:
            return None

        bin_edges = np.linspace(price_min, price_max, self._bins + 1)
        bin_width = bin_edges[1] - bin_edges[0]
        bin_centers = (bin_edges[:-1] + bin_edges[1:]) / 2
        bin_volumes = np.zeros(self._bins, dtype=float)

        # ── 向量化分配成交量 ──────────────────────────────────
        # 每根 K 線的成交量均勻分配到其 [Low, High] 覆蓋的所有 bin
        for i in range(len(df)):
            lo = low_arr[i]
            hi = high_arr[i]
            vo = vol_arr[i]
            if vo <= 0 or hi <= lo:
                # 僅分配到最近的 bin 中心
                idx = int((lo - price_min) / bin_width)
                idx = max(0, min(self._bins - 1, idx))
                bin_volumes[idx] += vo
                continue

            # 找出覆蓋範圍的 bin 索引
            lo_idx = int((lo - price_min) / bin_width)
            hi_idx = int((hi - price_min) / bin_width)
            lo_idx = max(0, min(self._bins - 1, lo_idx))
            hi_idx = max(0, min(self._bins - 1, hi_idx))

            # 均勻分配
            span = hi_idx - lo_idx + 1
            bin_volumes[lo_idx : hi_idx + 1] += vo / span

        total_vol = float(bin_volumes.sum())
        if total_vol <= 0:
            return None

        # ── POC ──────────────────────────────────────────────
        poc_idx = int(np.argmax(bin_volumes))
        poc     = float(bin_centers[poc_idx])

        # ── Value Area（從 POC 向兩側擴展）─────────────────
        target = total_vol * self._value_area_pct
        va_lo  = poc_idx
        va_hi  = poc_idx
        va_vol = float(bin_volumes[poc_idx])

        while va_vol < target:
            can_expand_lo = va_lo > 0
            can_expand_hi = va_hi < self._bins - 1

            if not can_expand_lo and not can_expand_hi:
                break

            # 比較兩側下一個 bin 的成交量，優先擴展成交量大的一側
            vol_lo = float(bin_volumes[va_lo - 1]) if can_expand_lo else -1
            vol_hi = float(bin_volumes[va_hi + 1]) if can_expand_hi else -1

            if vol_lo >= vol_hi and can_expand_lo:
                va_lo  -= 1
                va_vol += float(bin_volumes[va_lo])
            elif can_expand_hi:
                va_hi  += 1
                va_vol += float(bin_volumes[va_hi])
            else:
                va_lo  -= 1
                va_vol += float(bin_volumes[va_lo])

        vah = float(bin_edges[va_hi + 1])
        val = float(bin_edges[va_lo])

        elapsed = (time.perf_counter() - t0) * 1000
        log.debug(
            f"[VP] 計算完成：POC={poc:.4f} VAH={vah:.4f} VAL={val:.4f} "
            f"({self._bins} bins, {len(df)} 根 K線) 耗時 {elapsed:.1f}ms"
        )

        result = VolumeProfileResult(
            poc              = poc,
            vah              = vah,
            val              = val,
            total_volume     = total_vol,
            value_area_pct   = va_vol / total_vol,
            price_range_low  = price_min,
            price_range_high = price_max,
            bin_count        = self._bins,
            computed_at      = time.time(),
        )

        # 存入快取
        self._cache     = result
        self._cache_key = cache_key
        return result

    # ════════════════════════════════════════════════════════
    # 便捷工具
    # ════════════════════════════════════════════════════════
    def price_position(
        self,
        price:  float,
        result: VolumeProfileResult,
    ) -> str:
        """判斷當前價格相對 Value Area 的位置。"""
        return result.price_position(price)

    def to_scoring_ctx(
        self,
        result: Optional[VolumeProfileResult],
    ) -> dict:
        """
        將 VP 結果轉換為 ScoringEngine 可用的 market_ctx 子字典。
        result 為 None 時回傳空 dict（ScoringEngine 會跳過此因子）。
        """
        if result is None:
            return {}
        return {
            "poc": result.poc,
            "vah": result.vah,
            "val": result.val,
        }

    # ════════════════════════════════════════════════════════
    # 內部工具
    # ════════════════════════════════════════════════════════
    @staticmethod
    def _make_cache_key(df: pd.DataFrame, vol_col: str) -> int:
        """以 K 線長度 + 最後一根收盤價 + 最後一根成交量 組成快取鍵。"""
        last_close  = float(df.iloc[-1].get("Close", df.iloc[-1].get("close", 0)))
        last_volume = float(df[vol_col].iloc[-1])
        return hash((len(df), round(last_close, 6), round(last_volume, 2)))
