"""
quant_system/execution/fee_calculator.py — 手續費扣除計算

Binance 手續費邏輯：
  - 預設：買入時以「報價幣（USDT）」扣手續費
           賣出時以「報價幣（USDT）」扣手續費
  - BNB 折抵：買入時直接扣除 BNB，幣量不受影響（需另計 BNB 成本）
  - 原生幣扣除（部分幣對）：買入收到的幣量會被直接扣除手續費

問題情境：
  你用 1000 USDT 以 50000 USDT/BTC 買 BTC：
    理論買量 = 1000 / 50000 = 0.02 BTC
    若費率 0.1%，手續費 = 0.00002 BTC（從收到的幣中扣）
    實際到帳 = 0.02 - 0.00002 = 0.01998 BTC

  若之後你嘗試賣 0.02 BTC → 餘額不足 → API 報錯 LOT_SIZE
  正確做法：賣出量上限 = 實際到帳量 = 0.01998 BTC

此模組提供：
  1. 計算買入後實際到帳幣量（扣除手續費）
  2. 計算賣出前應保留的餘量（避免餘額不足）
  3. 根據 Maker/Taker 身份選擇費率
  4. BNB 折抵情境下的費率調整
"""
from __future__ import annotations
import logging
from dataclasses import dataclass
from typing import Optional

log = logging.getLogger(__name__)

# Binance 標準費率
TAKER_FEE_RATE = 0.001    # 0.10%（市價單）
MAKER_FEE_RATE = 0.001    # 0.10%（限價單，有時更低）
BNB_DISCOUNT   = 0.75     # 使用 BNB 折抵時費率打 75 折（= 0.075%）


@dataclass
class FeeResult:
    gross_qty:    float   # 下單數量（送出去的量）
    fee_qty:      float   # 手續費（以基礎幣計算，USDT 幣對中就是 BTC 等）
    fee_usdt:     float   # 手續費（以 USDT 計算）
    net_qty:      float   # 實際到帳數量 = gross_qty - fee_qty
    fee_rate:     float   # 實際使用的費率
    fee_asset:    str     # 扣費幣種（"BASE" | "BNB" | "QUOTE"）
    safe_sell_qty: float  # 安全可賣量（含精度緩衝）


class FeeCalculator:
    """
    計算 Binance 買賣的手續費，並提供「安全賣出量」以避免餘額不足。
    """

    def __init__(
        self,
        taker_fee:      float = TAKER_FEE_RATE,
        maker_fee:      float = MAKER_FEE_RATE,
        use_bnb:        bool  = False,
        bnb_discount:   float = BNB_DISCOUNT,
        qty_buffer_pct: float = 0.0002,   # 額外保留 0.02% 作為浮點數安全緩衝
    ) -> None:
        self.taker_fee    = taker_fee * (bnb_discount if use_bnb else 1.0)
        self.maker_fee    = maker_fee * (bnb_discount if use_bnb else 1.0)
        self.use_bnb      = use_bnb
        self.buffer       = qty_buffer_pct

    # ── 買入計算 ─────────────────────────────────────────────
    def calc_buy(
        self,
        order_qty:   float,   # 送出的下單數量
        price:       float,   # 成交均價
        is_maker:    bool  = False,
    ) -> FeeResult:
        """
        計算買入後實際到帳的幣量。
        Binance 現貨買入：手續費從「買到的幣」中扣（非 BNB 折抵時）。
        """
        rate     = self.maker_fee if is_maker else self.taker_fee
        fee_qty  = order_qty * rate
        net_qty  = order_qty - fee_qty
        fee_usdt = fee_qty * price
        # 安全可賣量：再保留 buffer 避免浮點尾數問題
        safe_sell = net_qty * (1 - self.buffer)

        return FeeResult(
            gross_qty    = order_qty,
            fee_qty      = fee_qty,
            fee_usdt     = fee_usdt,
            net_qty      = net_qty,
            fee_rate     = rate,
            fee_asset    = "BNB" if self.use_bnb else "BASE",
            safe_sell_qty = safe_sell,
        )

    # ── 賣出計算 ─────────────────────────────────────────────
    def calc_sell(
        self,
        sell_qty:  float,   # 欲賣出的幣量
        price:     float,   # 成交均價
        is_maker:  bool = False,
    ) -> FeeResult:
        """
        計算賣出後實際到帳的 USDT。
        Binance 現貨賣出：手續費從「收到的 USDT」中扣。
        """
        rate      = self.maker_fee if is_maker else self.taker_fee
        gross_usdt = sell_qty * price
        fee_usdt   = gross_usdt * rate
        net_usdt   = gross_usdt - fee_usdt

        return FeeResult(
            gross_qty    = sell_qty,
            fee_qty      = 0.0,      # 賣出時幣量不扣，扣的是 USDT
            fee_usdt     = fee_usdt,
            net_qty      = net_usdt / price if price > 0 else 0.0,
            fee_rate     = rate,
            fee_asset    = "BNB" if self.use_bnb else "QUOTE",
            safe_sell_qty = sell_qty,
        )

    # ── 完整買賣利潤計算 ────────────────────────────────────
    def calc_round_trip_pnl(
        self,
        buy_qty:    float,
        buy_price:  float,
        sell_price: float,
        buy_maker:  bool = False,
        sell_maker: bool = False,
    ) -> dict:
        """
        計算一個完整網格回合（BUY → SELL）的淨損益。
        buy_qty 為送出的下單量，不是到帳量。
        """
        buy_res  = self.calc_buy(buy_qty, buy_price, buy_maker)
        # 賣出量 = 買入到帳量（安全量）
        sell_res = self.calc_sell(buy_res.safe_sell_qty, sell_price, sell_maker)

        cost     = buy_qty * buy_price        # 買入花費 USDT
        proceeds = sell_res.net_qty * sell_price if sell_price > 0 else 0.0
        # 更精確：sell_res 的 net_qty 實際上是 USDT / price，直接用 gross_usdt - fee_usdt
        gross_usdt  = buy_res.safe_sell_qty * sell_price
        net_usdt    = gross_usdt * (1 - (self.maker_fee if sell_maker else self.taker_fee))
        net_pnl     = net_usdt - cost
        total_fees  = buy_res.fee_usdt + (gross_usdt - net_usdt)
        return_pct  = net_pnl / cost * 100 if cost > 0 else 0.0

        return {
            "cost_usdt":    cost,
            "proceeds_usdt": net_usdt,
            "net_pnl":      net_pnl,
            "total_fees":   total_fees,
            "return_pct":   return_pct,
            "buy_qty_received": buy_res.net_qty,
            "safe_sell_qty":    buy_res.safe_sell_qty,
        }

    # ── 最小有利可圖格距 ────────────────────────────────────
    def min_profitable_spacing(self, price: float, fee_rate: Optional[float] = None) -> float:
        """
        計算在當前費率下，BUY→SELL 需要的最小格距才能獲利。
        min_spacing = price × 2 × fee_rate / (1 - fee_rate)
        """
        r = fee_rate if fee_rate is not None else self.taker_fee
        return price * 2 * r / (1 - r)
