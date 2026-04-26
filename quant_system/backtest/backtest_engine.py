"""
quant_system/backtest/backtest_engine.py — 強化版回測引擎

Lookahead Bias 防護（全面修正）：
  ① 訊號生成：strategy.generate_signal(df.iloc[:i]) — 只使用 bar i-1 及更早資料
  ② T+1 執行：進出場均使用 bar i 的 Open（前一 bar 訊號 → 下一 bar Open 成交）
  ③ SL/TP 觸發：以 High/Low 判斷是否觸價，而非 Close（Close 是 bar 結束後才知道）
  ④ 觸價順序：OHLC 模式預設悲觀假設（多頭先碰 Low），或使用細顆粒度 1m 資料

細顆粒度回測（intrabar simulation）：
  - fine_grain=False（預設）：使用 OHLC 悲觀估算觸價順序
  - fine_grain=True + intrabar_df：傳入 1m K 線 DataFrame 進行逐分鐘模擬，
    精確重現 bar 內 SL/TP 的觸發先後順序

成本模型：
  adj_buy  = price × (1 + slip_pct) × (1 + fee_pct)
  adj_sell = price × (1 - slip_pct) × (1 - fee_pct)
"""
from __future__ import annotations
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import pandas as pd
import numpy as np

from ..strategy.base_strategy import BaseStrategy, SignalResult
from ..execution.cost_model   import CostModel

log = logging.getLogger(__name__)


@dataclass
class Trade:
    bar_in:      int
    bar_out:     int
    entry_px:    float
    exit_px:     float
    adj_entry:   float
    adj_exit:    float
    qty:         float
    net_pnl:     float
    return_pct:  float
    fees:        float
    slippage:    float
    reason:      str = ""     # "SL" | "TP" | "TRAIL" | "END" | "SIGNAL_EXIT"


@dataclass
class BacktestResult:
    symbol:         str
    total_return:   float
    annual_return:  float
    max_drawdown:   float
    sharpe:         float
    win_rate:       float
    profit_factor:  float
    total_trades:   int
    avg_trade_pnl:  float
    total_fees:     float
    total_slippage: float
    cost_impact_pct: float
    equity_curve:   List[float] = field(default_factory=list)
    trades:         List[Trade] = field(default_factory=list)


class BacktestEngine:
    """
    強化版回測引擎，支援 OHLC 觸價模擬與細顆粒度（1m）K 線內模擬。

    參數：
      fine_grain   — True = 使用 intrabar_df（1m）逐分鐘模擬；False = OHLC 悲觀估算
      pessimistic  — fine_grain=False 時，True = 多頭先碰 Low（最壞情況），
                     False = 隨機決定先碰 High 或 Low
    """

    def __init__(
        self,
        strategy:         BaseStrategy,
        cost_model:       Optional[CostModel] = None,
        initial_capital:  float = 100_000.0,
        sl_pct:           float = 0.02,
        tp_pct:           float = 0.04,
        trailing_pct:     float = 0.015,
        qty_pct:          float = 0.10,
        fine_grain:       bool  = False,
        pessimistic:      bool  = True,
    ) -> None:
        self.strategy   = strategy
        self.cost       = cost_model or CostModel()
        self.capital    = initial_capital
        self.sl_pct     = sl_pct
        self.tp_pct     = tp_pct
        self.trailing   = trailing_pct
        self.qty_pct    = qty_pct
        self.fine_grain = fine_grain
        self.pessimistic = pessimistic

    # ════════════════════════════════════════════════════════
    # 主入口
    # ════════════════════════════════════════════════════════
    def run(
        self,
        df:           pd.DataFrame,
        symbol:       str = "SYMBOL",
        intrabar_df:  Optional[pd.DataFrame] = None,
    ) -> BacktestResult:
        """
        執行回測。

        df           — 主 K 線 DataFrame（需含 Open/High/Low/Close 欄位）
        intrabar_df  — 1m 細顆粒度 DataFrame（fine_grain=True 時必須提供）
        """
        df = self._prepare_df(df)

        cash       = self.capital
        equity_cur = [cash]
        position   = None   # Dict 描述當前持倉
        trades: List[Trade] = []
        total_fees = total_slip = 0.0

        for i in range(1, len(df)):
            bar = df.iloc[i]
            bar_open  = float(bar["Open"])
            bar_high  = float(bar["High"])
            bar_low   = float(bar["Low"])
            bar_close = float(bar["Close"])

            # ── 持倉中：Bar 開盤先處理跳空停損/止盈 ─────────
            if position:
                # 跳空缺口：Open 已越過 SL/TP（強制以 Open 執行）
                gap_exit = self._check_gap(position, bar_open)
                if gap_exit:
                    cash, total_fees, total_slip = self._exit_position(
                        position, bar_open, i, gap_exit, cash, trades, total_fees, total_slip
                    )
                    position = None

            # ── 持倉中：Bar 內部觸價模擬 ─────────────────────
            if position:
                exit_px, exit_reason = self._check_intrabar(
                    position, bar_open, bar_high, bar_low,
                    bar_index=i, df=df, intrabar_df=intrabar_df
                )
                if exit_reason:
                    cash, total_fees, total_slip = self._exit_position(
                        position, exit_px, i, exit_reason, cash, trades, total_fees, total_slip
                    )
                    position = None

                # 若持倉未平，更新移動停損峰值
                if position and bar_high > position["peak"]:
                    position["peak"] = bar_high

            # ── 無持倉：訊號生成（嚴格使用 i-1 以前資料）────
            if not position:
                # ⚠️ df.iloc[:i] 不含 bar i，即只到 bar i-1（T-0），訊號在 bar i Open 執行
                sub = df.iloc[:i]
                sig: SignalResult = self.strategy.generate_signal(sub)

                if sig.signal == "BUY":
                    exe_px = bar_open   # T+1：以 bar i 的 Open 執行
                    qty    = (cash * self.qty_pct) / exe_px
                    if qty * exe_px >= 1.0:
                        exe = self.cost.simulate_buy(exe_px, qty)
                        if cash >= exe.total_cost:
                            cash -= exe.total_cost
                            total_fees += exe.fee
                            total_slip += exe.slippage_cost
                            position = {
                                "bar_in":    i,
                                "entry_px":  exe_px,
                                "entry_adj": exe.adjusted_price,
                                "qty":       qty,
                                "peak":      exe_px,
                                "sl":        exe_px * (1 - self.sl_pct),
                                "tp":        exe_px * (1 + self.tp_pct),
                            }
                            log.debug(
                                f"[Backtest] BUY  bar={i} px={exe_px:.4f} "
                                f"qty={qty:.6f} sl={position['sl']:.4f} tp={position['tp']:.4f}"
                            )

            cur_val = cash + (position["qty"] * bar_close if position else 0.0)
            equity_cur.append(cur_val)

        # ── 回測結束強平剩餘倉位 ─────────────────────────────
        if position:
            last_px = float(df["Close"].iloc[-1])
            cash, total_fees, total_slip = self._exit_position(
                position, last_px, len(df) - 1, "END", cash, trades, total_fees, total_slip
            )
            equity_cur[-1] = cash

        return self._build_result(
            symbol, equity_cur, trades, total_fees, total_slip
        )

    # ════════════════════════════════════════════════════════
    # 跳空缺口檢查（開盤已越過 SL/TP）
    # ════════════════════════════════════════════════════════
    def _check_gap(self, pos: Dict, bar_open: float) -> Optional[str]:
        """Bar 的 Open 已越過 SL 或 TP（跳空），以 Open 執行，優先處理 SL。"""
        trail_sl = pos["peak"] * (1 - self.trailing)
        eff_sl   = max(pos["sl"], trail_sl)

        if bar_open <= eff_sl:
            return "SL_GAP"
        if bar_open >= pos["tp"]:
            return "TP_GAP"
        return None

    # ════════════════════════════════════════════════════════
    # Bar 內觸價模擬（OHLC 或 1m 細顆粒度）
    # ════════════════════════════════════════════════════════
    def _check_intrabar(
        self,
        pos:          Dict,
        bar_open:     float,
        bar_high:     float,
        bar_low:      float,
        bar_index:    int,
        df:           pd.DataFrame,
        intrabar_df:  Optional[pd.DataFrame],
    ) -> tuple[Optional[float], Optional[str]]:
        """
        回傳 (exit_price, reason) 或 (None, None)。
        """
        trail_sl = pos["peak"] * (1 - self.trailing)
        eff_sl   = max(pos["sl"], trail_sl)
        tp       = pos["tp"]

        sl_hit   = bar_low  <= eff_sl
        tp_hit   = bar_high >= tp

        if not sl_hit and not tp_hit:
            return None, None

        # ── 細顆粒度模式：逐分鐘判斷觸價順序 ─────────────────
        if self.fine_grain and intrabar_df is not None:
            return self._check_intrabar_fine(pos, bar_index, df, intrabar_df, eff_sl, tp)

        # ── OHLC 悲觀/隨機模式 ───────────────────────────────
        if sl_hit and tp_hit:
            # 兩者都在 bar 內，需判斷誰先觸發
            if self.pessimistic:
                # 悲觀假設：多頭先碰 Low（SL 先觸發）
                return eff_sl, "SL"
            else:
                # 隨機：以 Open 到 High/Low 距離比較
                dist_sl = abs(bar_open - eff_sl)
                dist_tp = abs(bar_open - tp)
                if dist_sl <= dist_tp:
                    return eff_sl, "SL"
                else:
                    return tp, "TP"
        elif sl_hit:
            return eff_sl, "SL"
        else:
            return tp, "TP"

    def _check_intrabar_fine(
        self,
        pos:          Dict,
        bar_index:    int,
        df:           pd.DataFrame,
        intrabar_df:  pd.DataFrame,
        eff_sl:       float,
        tp:           float,
    ) -> tuple[Optional[float], Optional[str]]:
        """
        使用 1m 細顆粒度資料逐分鐘掃描，精確判斷 SL/TP 觸發順序。

        intrabar_df 需要有 DatetimeIndex，且包含 Open/High/Low/Close 欄位。
        """
        try:
            bar_ts = df.index[bar_index]
            # 取該 bar 所在時段的 1m 資料
            if hasattr(bar_ts, "floor"):
                # 以小時 bar 為例，取同一小時的 1m 資料
                bar_end = df.index[bar_index + 1] if bar_index + 1 < len(df) else None
                if bar_end is not None:
                    minute_bars = intrabar_df.loc[
                        (intrabar_df.index >= bar_ts) & (intrabar_df.index < bar_end)
                    ]
                else:
                    minute_bars = intrabar_df.loc[intrabar_df.index >= bar_ts]

                for _, mb in minute_bars.iterrows():
                    m_low  = float(mb["Low"])
                    m_high = float(mb["High"])
                    m_open = float(mb["Open"])

                    # 每分鐘 bar 內同樣做觸價判斷
                    sl_hit_m = m_low  <= eff_sl
                    tp_hit_m = m_high >= tp

                    if sl_hit_m and tp_hit_m:
                        # 分鐘 bar 內兩者同時，以 Open 到各觸發點距離估算
                        dist_sl = abs(m_open - eff_sl)
                        dist_tp = abs(m_open - tp)
                        return (eff_sl, "SL") if dist_sl <= dist_tp else (tp, "TP")
                    elif sl_hit_m:
                        return eff_sl, "SL"
                    elif tp_hit_m:
                        return tp, "TP"
        except Exception as e:
            log.debug(f"[Backtest] 細顆粒度查詢失敗，退化至 OHLC: {e}")

        # fallback: OHLC 悲觀模式
        return eff_sl, "SL"

    # ════════════════════════════════════════════════════════
    # 出場執行
    # ════════════════════════════════════════════════════════
    def _exit_position(
        self,
        pos:         Dict,
        exit_px:     float,
        bar_out:     int,
        reason:      str,
        cash:        float,
        trades:      List[Trade],
        total_fees:  float,
        total_slip:  float,
    ) -> tuple[float, float, float]:
        exe = self.cost.simulate_sell(exit_px, pos["qty"])
        net = exe.total_cost - pos["entry_adj"] * pos["qty"]
        ret = net / (pos["entry_adj"] * pos["qty"]) * 100
        trades.append(Trade(
            bar_in=pos["bar_in"], bar_out=bar_out,
            entry_px=pos["entry_px"], exit_px=exit_px,
            adj_entry=pos["entry_adj"], adj_exit=exe.adjusted_price,
            qty=pos["qty"], net_pnl=net, return_pct=ret,
            fees=exe.fee, slippage=exe.slippage_cost, reason=reason,
        ))
        log.debug(
            f"[Backtest] {reason} bar={bar_out} px={exit_px:.4f} "
            f"net_pnl={net:+.4f}"
        )
        return (
            cash + exe.total_cost,
            total_fees + exe.fee,
            total_slip + exe.slippage_cost,
        )

    # ════════════════════════════════════════════════════════
    # 工具
    # ════════════════════════════════════════════════════════
    @staticmethod
    def _prepare_df(df: pd.DataFrame) -> pd.DataFrame:
        """確保必要欄位存在，補充缺少的 Open/High/Low。"""
        df = df.copy()
        if "Open" not in df.columns:
            df["Open"] = df["Close"].shift(1).fillna(df["Close"])
        if "High" not in df.columns:
            df["High"] = df[["Open", "Close"]].max(axis=1)
        if "Low" not in df.columns:
            df["Low"]  = df[["Open", "Close"]].min(axis=1)
        return df

    def _build_result(
        self,
        symbol:      str,
        equity_cur:  List[float],
        trades:      List[Trade],
        total_fees:  float,
        total_slip:  float,
    ) -> BacktestResult:
        final   = equity_cur[-1]
        tot_ret = (final - self.capital) / self.capital * 100
        n_bars  = len(equity_cur)
        bars_yr = 252 * 390 if n_bars > 10000 else (252 * 78 if n_bars > 2000 else 252)
        ann_ret = ((1 + tot_ret / 100) ** (bars_yr / max(n_bars, 1)) - 1) * 100
        mdd     = self._max_drawdown(equity_cur)
        sharpe  = self._sharpe(equity_cur)

        wins    = [t for t in trades if t.net_pnl > 0]
        losses  = [t for t in trades if t.net_pnl <= 0]
        wr      = len(wins) / len(trades) if trades else 0.0
        gp      = sum(t.net_pnl for t in wins)
        gl      = abs(sum(t.net_pnl for t in losses))
        pf      = gp / gl if gl > 0 else float("inf")
        avg_pnl = sum(t.net_pnl for t in trades) / len(trades) if trades else 0.0
        ci      = (total_fees + total_slip) / self.capital * 100

        return BacktestResult(
            symbol=symbol, total_return=tot_ret, annual_return=ann_ret,
            max_drawdown=mdd, sharpe=sharpe, win_rate=wr,
            profit_factor=pf, total_trades=len(trades), avg_trade_pnl=avg_pnl,
            total_fees=total_fees, total_slippage=total_slip, cost_impact_pct=ci,
            equity_curve=equity_cur, trades=trades,
        )

    @staticmethod
    def _max_drawdown(eq: List[float]) -> float:
        arr  = np.array(eq, dtype=float)
        peak = np.maximum.accumulate(arr)
        dd   = (arr - peak) / (peak + 1e-9)
        return float(dd.min() * 100)

    @staticmethod
    def _sharpe(eq: List[float], rf: float = 0.0) -> float:
        arr  = np.diff(eq) / (np.array(eq[:-1]) + 1e-9)
        mean = arr.mean()
        std  = arr.std()
        return float((mean - rf) / (std + 1e-9) * (252 ** 0.5))
