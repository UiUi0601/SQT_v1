"""
quant_system/market/regime_detector.py — 市場狀態偵測
  TRENDING  : ADX > 25 且有方向性
  RANGING   : ADX ≤ 25，價格在均值附近震盪
  VOLATILE  : 波動率急劇放大（ATR / Close > 閾值）
"""
from __future__ import annotations
import logging
from dataclasses import dataclass
from enum import Enum
from typing import Optional

import pandas as pd
import numpy as np

log = logging.getLogger(__name__)


class MarketRegime(Enum):
    TRENDING  = "TRENDING"
    RANGING   = "RANGING"
    VOLATILE  = "VOLATILE"
    UNKNOWN   = "UNKNOWN"


@dataclass
class RegimeResult:
    regime:     MarketRegime
    adx:        float = 0.0
    atr_ratio:  float = 0.0   # ATR / Close（波動率）
    trend_dir:  str   = ""    # "UP" | "DOWN" | ""
    confidence: float = 0.0


class RegimeDetector:
    def __init__(
        self,
        adx_period:       int   = 14,
        adx_trend_thresh: float = 25.0,
        vol_thresh:       float = 0.035,   # ATR/Close 超過此值視為高波動
    ) -> None:
        self.adx_period   = adx_period
        self.adx_thresh   = adx_trend_thresh
        self.vol_thresh   = vol_thresh

    def detect(self, df: pd.DataFrame) -> RegimeResult:
        try:
            if len(df) < self.adx_period + 5:
                return RegimeResult(MarketRegime.UNKNOWN)

            adx, pdi, mdi = self._adx(df)
            atr_r         = self._atr_ratio(df)

            if atr_r > self.vol_thresh:
                return RegimeResult(
                    regime=MarketRegime.VOLATILE,
                    adx=adx, atr_ratio=atr_r,
                    confidence=min(atr_r / self.vol_thresh - 1, 1.0),
                )
            if adx > self.adx_thresh:
                direction = "UP" if pdi > mdi else "DOWN"
                conf = min((adx - self.adx_thresh) / 25, 1.0)
                return RegimeResult(
                    regime=MarketRegime.TRENDING,
                    adx=adx, atr_ratio=atr_r,
                    trend_dir=direction, confidence=conf,
                )
            return RegimeResult(
                regime=MarketRegime.RANGING,
                adx=adx, atr_ratio=atr_r,
                confidence=1 - adx / self.adx_thresh,
            )
        except Exception as e:
            log.warning(f"[Regime] 偵測失敗: {e}")
            return RegimeResult(MarketRegime.UNKNOWN)

    # ── internal ─────────────────────────────────────────────
    def _adx(self, df: pd.DataFrame):
        hi = df["High"].values.astype(float)
        lo = df["Low"].values.astype(float)
        cl = df["Close"].values.astype(float)
        n  = self.adx_period

        up_move   = hi[1:] - hi[:-1]
        down_move = lo[:-1] - lo[1:]

        pdm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
        ndm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)

        trs = np.array([
            max(hi[i] - lo[i], abs(hi[i] - cl[i-1]), abs(lo[i] - cl[i-1]))
            for i in range(1, len(cl))
        ])

        atr_s  = self._smooth(trs,  n)
        pdm_s  = self._smooth(pdm,  n)
        ndm_s  = self._smooth(ndm,  n)

        pdi = 100 * pdm_s / (atr_s + 1e-9)
        mdi = 100 * ndm_s / (atr_s + 1e-9)
        dx  = 100 * np.abs(pdi - mdi) / (pdi + mdi + 1e-9)
        adx = self._smooth(dx, n)

        return float(adx[-1]), float(pdi[-1]), float(mdi[-1])

    def _atr_ratio(self, df: pd.DataFrame) -> float:
        hi = df["High"].values[-self.adx_period:]
        lo = df["Low"].values[-self.adx_period:]
        cl = df["Close"].values[-self.adx_period:]
        trs = [max(hi[i]-lo[i], abs(hi[i]-cl[i-1]), abs(lo[i]-cl[i-1])) for i in range(1, len(cl))]
        atr = sum(trs) / len(trs) if trs else 0.0
        return atr / cl[-1] if cl[-1] > 0 else 0.0

    @staticmethod
    def _smooth(arr: np.ndarray, n: int) -> np.ndarray:
        out = np.zeros(len(arr))
        out[n-1] = arr[:n].mean()
        k = 1 / n
        for i in range(n, len(arr)):
            out[i] = out[i-1] + k * (arr[i] - out[i-1])
        return out
