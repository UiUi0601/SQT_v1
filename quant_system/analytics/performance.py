"""
quant_system/analytics/performance.py — 績效分析器
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import List, Optional

import numpy as np
import pandas as pd


@dataclass
class PerformanceSummary:
    total_return_pct:  float
    annual_return_pct: float
    max_drawdown_pct:  float
    sharpe_ratio:      float
    sortino_ratio:     float
    calmar_ratio:      float
    win_rate:          float
    profit_factor:     float
    avg_win_pct:       float
    avg_loss_pct:      float
    total_trades:      int
    total_fees:        float
    cost_drag_pct:     float   # 成本對總報酬的拖累


class PerformanceAnalyzer:
    """接收 equity_curve（list/ndarray）和 trades（list of dicts）計算績效。"""

    def __init__(self, initial_capital: float = 100_000.0) -> None:
        self.initial = initial_capital

    def analyze(
        self,
        equity_curve: List[float],
        trades: Optional[List[dict]] = None,
        bars_per_year: int = 252,
    ) -> PerformanceSummary:
        eq = np.array(equity_curve, dtype=float)
        trades = trades or []

        final      = eq[-1]
        tot_ret    = (final - self.initial) / self.initial * 100
        n          = len(eq)
        ann_ret    = ((1 + tot_ret / 100) ** (bars_per_year / max(n, 1)) - 1) * 100
        mdd        = self._max_drawdown(eq)
        sharpe     = self._sharpe(eq, bars_per_year)
        sortino    = self._sortino(eq, bars_per_year)
        calmar     = ann_ret / abs(mdd) if mdd != 0 else 0.0

        wins   = [t for t in trades if t.get("net_pnl", 0) > 0]
        losses = [t for t in trades if t.get("net_pnl", 0) <= 0]
        wr     = len(wins) / len(trades) if trades else 0.0
        gp     = sum(t.get("net_pnl", 0) for t in wins)
        gl     = abs(sum(t.get("net_pnl", 0) for t in losses))
        pf     = gp / gl if gl > 0 else float("inf")

        rets       = [t.get("return_pct", 0) for t in trades]
        avg_win    = float(np.mean([r for r in rets if r > 0])) if any(r > 0 for r in rets) else 0.0
        avg_loss   = float(np.mean([r for r in rets if r <= 0])) if any(r <= 0 for r in rets) else 0.0
        total_fees = sum(t.get("fees", 0) for t in trades)
        cost_drag  = total_fees / self.initial * 100

        return PerformanceSummary(
            total_return_pct  = tot_ret,
            annual_return_pct = ann_ret,
            max_drawdown_pct  = mdd,
            sharpe_ratio      = sharpe,
            sortino_ratio     = sortino,
            calmar_ratio      = calmar,
            win_rate          = wr,
            profit_factor     = pf,
            avg_win_pct       = avg_win,
            avg_loss_pct      = avg_loss,
            total_trades      = len(trades),
            total_fees        = total_fees,
            cost_drag_pct     = cost_drag,
        )

    # ── static helpers ───────────────────────────────────────
    @staticmethod
    def _max_drawdown(eq: np.ndarray) -> float:
        peak = np.maximum.accumulate(eq)
        dd   = (eq - peak) / (peak + 1e-9)
        return float(dd.min() * 100)

    @staticmethod
    def _sharpe(eq: np.ndarray, bpy: int) -> float:
        r   = np.diff(eq) / (eq[:-1] + 1e-9)
        return float(r.mean() / (r.std() + 1e-9) * (bpy ** 0.5))

    @staticmethod
    def _sortino(eq: np.ndarray, bpy: int) -> float:
        r      = np.diff(eq) / (eq[:-1] + 1e-9)
        down   = r[r < 0]
        d_std  = down.std() if len(down) > 0 else 1e-9
        return float(r.mean() / (d_std + 1e-9) * (bpy ** 0.5))
