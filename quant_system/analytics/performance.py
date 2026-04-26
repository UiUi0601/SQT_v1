"""
quant_system/analytics/performance.py — 績效分析模組（GBM 基準版）

核心功能：
  1. 基本績效指標（Sharpe / Sortino / 最大回撤 / 勝率 / 盈虧比）
  2. GBM（幾何布朗運動）基準模擬：以歷史對數收益率估計 μ / σ，
     蒙地卡羅模擬 n_paths 條路徑，t 檢定比較實際表現
  3. PerformanceSummary 資料類別彙整所有結果

依賴：numpy, scipy（選用；未安裝時 GBM 部分降級為 None）
"""
from __future__ import annotations

import logging
import math
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import numpy as np

log = logging.getLogger(__name__)

try:
    from scipy import stats as scipy_stats
    _SCIPY_OK = True
except ImportError:
    scipy_stats = None   # type: ignore
    _SCIPY_OK = False
    log.warning("[Perf] scipy 未安裝，GBM t 檢定將跳過（pip install scipy）")


# ── GBM 基準資料類別 ─────────────────────────────────────────
@dataclass
class GBMBenchmark:
    """幾何布朗運動基準模擬結果。"""
    mu:             float   # 估計年化漂移率（log return mean * 252）
    sigma:          float   # 估計年化波動率（log return std  * sqrt(252)）
    n_paths:        int
    n_steps:        int
    initial_capital: float

    # 模擬分佈
    final_mean:     float
    final_std:      float
    ci_lower_95:    float   # 95% 信賴區間下界
    ci_upper_95:    float   # 95% 信賴區間上界
    paths_percentile_5:  float
    paths_percentile_50: float
    paths_percentile_95: float

    # 實際對比
    actual_final:   float
    t_statistic:    float = 0.0
    p_value:        float = 1.0
    is_significant: bool  = False   # p < 0.05

    @property
    def alpha(self) -> float:
        """實際終值 vs GBM 中位數的絕對差。"""
        return self.actual_final - self.paths_percentile_50

    @property
    def alpha_pct(self) -> float:
        """Alpha 佔初始資本的百分比。"""
        if self.initial_capital == 0:
            return 0.0
        return self.alpha / self.initial_capital * 100

    @property
    def interpretation(self) -> str:
        if not self.is_significant:
            return "績效與 GBM 隨機基準無顯著差異（p≥0.05）"
        if self.actual_final > self.paths_percentile_50:
            return f"績效顯著優於 GBM 基準（alpha={self.alpha_pct:+.2f}%，p={self.p_value:.4f}）"
        return f"績效顯著劣於 GBM 基準（alpha={self.alpha_pct:+.2f}%，p={self.p_value:.4f}）"


# ── 績效摘要資料類別 ──────────────────────────────────────────
@dataclass
class PerformanceSummary:
    """完整績效摘要，包含 GBM 基準結果。"""
    symbol:          str
    market_type:     str
    total_trades:    int
    win_trades:      int
    loss_trades:     int
    win_rate:        float
    total_pnl:       float
    avg_pnl:         float
    profit_factor:   float
    max_drawdown:    float
    max_drawdown_pct: float
    sharpe_ratio:    float
    sortino_ratio:   float
    initial_capital: float
    final_capital:   float
    total_return_pct: float
    start_ts:        float = 0.0
    end_ts:          float = field(default_factory=time.time)
    gbm_benchmark:   Optional[GBMBenchmark] = None

    @property
    def duration_days(self) -> float:
        if self.end_ts <= self.start_ts:
            return 0.0
        return (self.end_ts - self.start_ts) / 86400.0

    def to_dict(self) -> Dict:
        d = {k: v for k, v in self.__dict__.items() if k != "gbm_benchmark"}
        if self.gbm_benchmark:
            d["gbm"] = {
                "mu":           self.gbm_benchmark.mu,
                "sigma":        self.gbm_benchmark.sigma,
                "alpha_pct":    self.gbm_benchmark.alpha_pct,
                "p_value":      self.gbm_benchmark.p_value,
                "is_significant": self.gbm_benchmark.is_significant,
                "interpretation": self.gbm_benchmark.interpretation,
            }
        return d


# ── 績效分析器 ───────────────────────────────────────────────
class PerformanceAnalyzer:
    """
    將交易記錄彙整為 PerformanceSummary。

    Parameters
    ----------
    symbol         : 交易對
    market_type    : SPOT / FUTURES / MARGIN
    initial_capital: 初始資本（USDT）
    risk_free_rate : 年化無風險利率（Sharpe 用，預設 0.04）
    """

    def __init__(
        self,
        symbol:          str   = "UNKNOWN",
        market_type:     str   = "SPOT",
        initial_capital: float = 10_000.0,
        risk_free_rate:  float = 0.04,
    ):
        self._symbol   = symbol
        self._market   = market_type
        self._capital  = initial_capital
        self._rfr      = risk_free_rate

    def analyze(
        self,
        trades:       List[Dict],
        equity_curve: Optional[List[float]] = None,
        run_gbm:      bool = True,
        gbm_n_paths:  int  = 1000,
    ) -> PerformanceSummary:
        """
        Parameters
        ----------
        trades       : 交易記錄列表，每筆須含 pnl (float) 欄位
        equity_curve : 逐步資產曲線（若為 None，由 trades pnl 累加）
        run_gbm      : 是否執行 GBM 基準模擬
        gbm_n_paths  : GBM 模擬路徑數
        """
        pnls = [float(t.get("pnl", 0.0)) for t in trades]

        if equity_curve is None:
            equity_curve = []
            cap = self._capital
            for p in pnls:
                cap += p
                equity_curve.append(cap)
        equity_curve = list(equity_curve)

        total     = len(pnls)
        wins      = [p for p in pnls if p > 0]
        losses    = [p for p in pnls if p < 0]
        total_pnl = sum(pnls)

        win_rate  = len(wins) / total if total else 0.0
        avg_pnl   = total_pnl / total if total else 0.0

        gross_profit = sum(wins)
        gross_loss   = abs(sum(losses))
        profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else float("inf")

        dd, dd_pct = self._max_drawdown(equity_curve, self._capital)
        sharpe     = self._sharpe(pnls, self._rfr)
        sortino    = self._sortino(pnls, self._rfr)

        final_cap    = equity_curve[-1] if equity_curve else self._capital
        total_ret    = (final_cap - self._capital) / self._capital * 100 if self._capital else 0.0

        start_ts = float(trades[0].get("timestamp", 0)) if trades else 0.0
        end_ts   = float(trades[-1].get("timestamp", time.time())) if trades else time.time()

        gbm = None
        if run_gbm and len(equity_curve) >= 5:
            try:
                gbm = self.gbm_benchmark(
                    equity_curve  = equity_curve,
                    initial_capital = self._capital,
                    n_paths       = gbm_n_paths,
                )
            except Exception as e:
                log.warning(f"[Perf] GBM 模擬失敗：{e}")

        return PerformanceSummary(
            symbol           = self._symbol,
            market_type      = self._market,
            total_trades     = total,
            win_trades       = len(wins),
            loss_trades      = len(losses),
            win_rate         = win_rate,
            total_pnl        = total_pnl,
            avg_pnl          = avg_pnl,
            profit_factor    = profit_factor,
            max_drawdown     = dd,
            max_drawdown_pct = dd_pct,
            sharpe_ratio     = sharpe,
            sortino_ratio    = sortino,
            initial_capital  = self._capital,
            final_capital    = final_cap,
            total_return_pct = total_ret,
            start_ts         = start_ts,
            end_ts           = end_ts,
            gbm_benchmark    = gbm,
        )

    @staticmethod
    def gbm_benchmark(
        equity_curve:    List[float],
        initial_capital: float,
        n_paths:         int   = 1000,
        trading_days:    int   = 252,
    ) -> GBMBenchmark:
        """
        幾何布朗運動基準模擬。

        S(t) = S0 * exp((μ - σ²/2)*t + σ*√t*Z)

        步驟：
          1. 從 equity_curve 計算對數收益率
          2. 估計日漂移 μ_d / 日波動 σ_d
          3. 年化到 μ / σ
          4. 模擬 n_paths 條路徑，取終值分佈
          5. 執行 t 檢定（實際終值 vs 模擬終值）
        """
        arr      = np.array(equity_curve, dtype=float)
        log_rets = np.diff(np.log(np.maximum(arr, 1e-8)))

        if len(log_rets) < 2:
            raise ValueError("equity_curve 長度不足，無法計算對數收益率")

        mu_d    = float(np.mean(log_rets))
        sigma_d = float(np.std(log_rets, ddof=1))

        mu_ann    = mu_d    * trading_days
        sigma_ann = sigma_d * math.sqrt(trading_days)

        n_steps = len(log_rets)
        dt      = 1.0 / trading_days

        rng      = np.random.default_rng(seed=42)
        Z        = rng.standard_normal((n_paths, n_steps))
        drift    = (mu_d - 0.5 * sigma_d ** 2)
        diffusion = sigma_d * Z
        log_path = np.cumsum(drift + diffusion, axis=1)
        finals   = initial_capital * np.exp(log_path[:, -1])

        final_mean    = float(np.mean(finals))
        final_std     = float(np.std(finals, ddof=1))
        ci_lo = float(np.percentile(finals, 2.5))
        ci_hi = float(np.percentile(finals, 97.5))
        p5    = float(np.percentile(finals, 5))
        p50   = float(np.percentile(finals, 50))
        p95   = float(np.percentile(finals, 95))

        actual_final = float(arr[-1])

        t_stat = 0.0
        p_val  = 1.0
        if _SCIPY_OK and final_std > 0:
            t_stat, p_val = scipy_stats.ttest_1samp(finals, actual_final)
            t_stat = float(t_stat)
            p_val  = float(p_val)

        return GBMBenchmark(
            mu               = mu_ann,
            sigma            = sigma_ann,
            n_paths          = n_paths,
            n_steps          = n_steps,
            initial_capital  = initial_capital,
            final_mean       = final_mean,
            final_std        = final_std,
            ci_lower_95      = ci_lo,
            ci_upper_95      = ci_hi,
            paths_percentile_5  = p5,
            paths_percentile_50 = p50,
            paths_percentile_95 = p95,
            actual_final     = actual_final,
            t_statistic      = t_stat,
            p_value          = p_val,
            is_significant   = p_val < 0.05,
        )

    # ── 靜態輔助 ────────────────────────────────────────────
    @staticmethod
    def _max_drawdown(
        equity: List[float], initial: float
    ) -> Tuple[float, float]:
        """計算最大回撤（絕對值, 百分比）。"""
        if not equity:
            return 0.0, 0.0
        arr     = np.array(equity, dtype=float)
        peak    = np.maximum.accumulate(arr)
        dd_arr  = peak - arr
        max_dd  = float(np.max(dd_arr))
        peak_val = float(arr[np.argmax(dd_arr)]) if max_dd > 0 else initial
        dd_pct  = max_dd / peak_val * 100 if peak_val > 0 else 0.0
        return max_dd, dd_pct

    @staticmethod
    def _sharpe(pnls: List[float], risk_free_rate: float = 0.04) -> float:
        """年化 Sharpe Ratio（假設日頻資料，252 交易日）。"""
        if len(pnls) < 2:
            return 0.0
        arr = np.array(pnls, dtype=float)
        std = float(np.std(arr, ddof=1))
        if std == 0:
            return 0.0
        daily_rf = risk_free_rate / 252
        excess   = arr - daily_rf
        return float(np.mean(excess) / std * math.sqrt(252))

    @staticmethod
    def _sortino(pnls: List[float], risk_free_rate: float = 0.04) -> float:
        """年化 Sortino Ratio（只懲罰下行波動）。"""
        if len(pnls) < 2:
            return 0.0
        arr      = np.array(pnls, dtype=float)
        daily_rf = risk_free_rate / 252
        excess   = arr - daily_rf
        down     = excess[excess < 0]
        if len(down) == 0:
            return float("inf")
        down_std = float(np.std(down, ddof=1))
        if down_std == 0:
            return 0.0
        return float(np.mean(excess) / down_std * math.sqrt(252))
