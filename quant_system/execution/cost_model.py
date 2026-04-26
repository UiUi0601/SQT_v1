"""
quant_system/execution/cost_model.py — 完整成本模型（含動態滑價）

公式：
  adj_buy  = price × (1 + slippage) × (1 + fee)
  adj_sell = price × (1 - slippage) × (1 - fee)

動態滑價模型（SlippageModel）：
  滑價由三個因子疊加：
    1. 基礎滑價（base）：固定最低成本，反映 bid-ask spread
    2. 衝擊滑價（impact）：與 √(訂單金額 / 日均成交量) 成正比
       - 大單打穿訂單簿，深度越淺衝擊越大
    3. 波動率滑價（vol）：ATR/price 越高，滑價越大
       - 市場波動越劇烈，成交不確定性越高

  total_slippage = base + impact_factor × √(order_value / adv) + vol_factor × atr_ratio
  其中 adv（平均日成交量）可用 volume 欄位估算，或使用保守預設值
"""
from __future__ import annotations
import math
import random
import logging
from dataclasses import dataclass
from typing import Optional

import pandas as pd

log = logging.getLogger(__name__)


# ── 資料類別 ─────────────────────────────────────────────────

@dataclass
class ExecutionResult:
    raw_price:      float   # 原始市價
    adjusted_price: float   # 含滑點+手續費後的實際成交均價
    fee:            float   # 手續費（USDT）
    slippage_cost:  float   # 滑點成本（USDT）
    total_cost:     float   # 總成本：quantity × adjusted_price（買入方向）
    quantity:       float
    slippage_pct:   float   # 本次滑點百分比（用於記錄）


@dataclass
class PnLResult:
    gross_pnl:      float
    net_pnl:        float
    return_pct:     float
    total_fees:     float
    total_slippage: float


# ── 動態滑價模型 ─────────────────────────────────────────────

class SlippageModel:
    """
    動態滑價計算器。

    參數：
      base_pct       : 最低基礎滑價（反映 spread），e.g. 0.0005 = 0.05%
      impact_factor  : 市場衝擊係數，e.g. 0.1
      vol_factor     : 波動率滑價係數，e.g. 2.0
      adv_estimate   : 預設日均成交量（USDT），若無法從 df 計算時使用
      add_noise      : 是否加入隨機噪音（True = 模擬真實的不確定性）
      noise_pct      : 噪音範圍（± noise_pct）
    """

    def __init__(
        self,
        base_pct:      float = 0.0005,
        impact_factor: float = 0.10,
        vol_factor:    float = 2.0,
        adv_estimate:  float = 50_000_000.0,   # 5000 萬 USDT 日均成交量（保守估計）
        add_noise:     bool  = True,
        noise_pct:     float = 0.0003,
    ) -> None:
        self.base       = base_pct
        self.impact_f   = impact_factor
        self.vol_f      = vol_factor
        self.adv        = adv_estimate
        self.noise      = add_noise
        self.noise_pct  = noise_pct

    def calc(
        self,
        price:     float,
        quantity:  float,
        df:        Optional[pd.DataFrame] = None,
    ) -> float:
        """
        計算本次訂單的滑價百分比。

        price    : 當前市價
        quantity : 下單數量（幣）
        df       : 最近 K 線（用於計算 ATR 和日均成交量）
        回傳：滑價百分比（e.g. 0.001 = 0.1%）
        """
        order_value = price * quantity

        # ① 基礎滑價
        slip = self.base

        # ② 市場衝擊滑價
        adv = self._estimate_adv(price, df)
        if adv > 0:
            impact = self.impact_f * math.sqrt(order_value / adv)
            slip  += impact

        # ③ 波動率滑價
        atr_ratio = self._atr_ratio(price, df)
        slip     += self.vol_f * atr_ratio * self.base   # 波動越高，基礎滑點放大

        # ④ 隨機噪音（模擬真實市況的不確定性）
        if self.noise:
            noise = random.gauss(0, self.noise_pct)
            slip += noise

        # 夾住至合理範圍（最低 0，最高 2%）
        slip = max(0.0, min(slip, 0.02))
        return slip

    # ── 內部輔助 ─────────────────────────────────────────────
    def _estimate_adv(self, price: float, df: Optional[pd.DataFrame]) -> float:
        """估算日均成交量（USDT）"""
        if df is None or "Volume" not in df.columns:
            return self.adv
        try:
            # 取最近 20 根 K 線的均量 × 價格 × 一天的 K 線根數比例
            vol_mean = float(df["Volume"].tail(20).mean())
            return vol_mean * price
        except Exception:
            return self.adv

    @staticmethod
    def _atr_ratio(price: float, df: Optional[pd.DataFrame]) -> float:
        """計算 ATR / price（波動率比率）"""
        if df is None or len(df) < 5:
            return 0.01   # 預設 1%
        try:
            hi = df["High"].values[-14:].astype(float)
            lo = df["Low"].values[-14:].astype(float)
            cl = df["Close"].values[-14:].astype(float)
            trs = [
                max(hi[i] - lo[i], abs(hi[i] - cl[i-1]), abs(lo[i] - cl[i-1]))
                for i in range(1, len(cl))
            ]
            atr = sum(trs) / len(trs) if trs else price * 0.01
            return atr / (price + 1e-9)
        except Exception:
            return 0.01


# ── 主成本模型 ───────────────────────────────────────────────

class CostModel:
    def __init__(
        self,
        maker_fee_pct:       float = 0.001,
        taker_fee_pct:       float = 0.001,
        slippage_min_pct:    float = 0.0005,
        slippage_max_pct:    float = 0.002,
        spread_pct:          float = 0.0005,
        use_random_slippage: bool  = True,
        min_profit_ratio:    float = 2.0,
        use_dynamic_slippage: bool = True,     # 是否啟用動態滑價模型
        slippage_model:      Optional[SlippageModel] = None,
    ) -> None:
        self.maker_fee    = maker_fee_pct
        self.taker_fee    = taker_fee_pct
        self.slip_min     = slippage_min_pct
        self.slip_max     = slippage_max_pct
        self.spread       = spread_pct
        self.random_slip  = use_random_slippage
        self.min_profit_r = min_profit_ratio
        self.dynamic_slip = use_dynamic_slippage
        self.slip_model   = slippage_model or SlippageModel(
            base_pct=slippage_min_pct,
        )

    # ── 滑價計算 ─────────────────────────────────────────────
    def _slip(
        self,
        price:    float = 0.0,
        qty:      float = 0.0,
        df:       Optional[pd.DataFrame] = None,
    ) -> float:
        if self.dynamic_slip and price > 0:
            return self.slip_model.calc(price, qty, df)
        if self.random_slip:
            return random.uniform(self.slip_min, self.slip_max)
        return (self.slip_min + self.slip_max) / 2

    # ── 模擬買入 ─────────────────────────────────────────────
    def simulate_buy(
        self,
        price:    float,
        quantity: float,
        df:       Optional[pd.DataFrame] = None,
    ) -> ExecutionResult:
        slip     = self._slip(price, quantity, df)
        fee      = self.taker_fee
        adj      = price * (1 + slip) * (1 + fee)
        fee_c    = price * quantity * fee
        slip_c   = price * quantity * slip
        return ExecutionResult(
            raw_price      = price,
            adjusted_price = adj,
            fee            = fee_c,
            slippage_cost  = slip_c,
            total_cost     = adj * quantity,
            quantity       = quantity,
            slippage_pct   = slip,
        )

    # ── 模擬賣出 ─────────────────────────────────────────────
    def simulate_sell(
        self,
        price:    float,
        quantity: float,
        df:       Optional[pd.DataFrame] = None,
    ) -> ExecutionResult:
        slip     = self._slip(price, quantity, df)
        fee      = self.taker_fee
        adj      = price * (1 - slip) * (1 - fee)
        fee_c    = price * quantity * fee
        slip_c   = price * quantity * slip
        return ExecutionResult(
            raw_price      = price,
            adjusted_price = adj,
            fee            = fee_c,
            slippage_cost  = slip_c,
            total_cost     = adj * quantity,
            quantity       = quantity,
            slippage_pct   = slip,
        )

    # ── PnL ──────────────────────────────────────────────────
    def calculate_pnl(
        self,
        buy_result:  ExecutionResult,
        sell_result: ExecutionResult,
    ) -> PnLResult:
        gross    = (sell_result.raw_price - buy_result.raw_price) * buy_result.quantity
        net      = sell_result.total_cost - buy_result.total_cost
        cost_b   = buy_result.total_cost
        ret_pct  = (net / cost_b * 100) if cost_b > 0 else 0.0
        return PnLResult(
            gross_pnl      = gross,
            net_pnl        = net,
            return_pct     = ret_pct,
            total_fees     = buy_result.fee + sell_result.fee,
            total_slippage = buy_result.slippage_cost + sell_result.slippage_cost,
        )

    # ── 損益平衡 ─────────────────────────────────────────────
    def estimate_breakeven_return(
        self,
        price:    float,
        quantity: float,
        df:       Optional[pd.DataFrame] = None,
    ) -> float:
        buy  = self.simulate_buy(price, quantity, df)
        sell = self.simulate_sell(price, quantity, df)
        total_cost_impact = buy.fee + sell.fee + buy.slippage_cost + sell.slippage_cost
        base = price * quantity
        return (total_cost_impact / base * 100) if base > 0 else 0.0
