"""
quant_system/grid/contract_interface.py — 合約介面預留層（⑤ Phase F）

設計目標：
  解決現貨網格「單向做多」的限制：
    - 現貨：只能做多（買入後持有），遇到長期下跌只能停損，無法對沖
    - USDⓈ-M 合約（做空 + 資金費率）：可同時建立對沖空單，降低整體回撤

現階段（Phase F）：
  - 定義 MarketType enum 與 ContractGridConfig dataclass
  - 所有合約專用邏輯以 TODO 標記，供 Phase G 完整實作
  - GridEngine.compute() 與 GridManager 已透過 market_type 參數接入此介面

Phase G 預計實作：
  - BinanceUSDMClient（Binance Futures REST + WebSocket）
  - 槓桿倉位管理（開槓桿 / 計算強平價 / 保證金率監控）
  - 資金費率套利（funding_rate < -threshold → 開空）
  - 對沖網格模式（spot_long + futures_short 並行運行）

使用範例（Phase G 後）：
    from quant_system.grid.contract_interface import (
        MarketType, ContractGridConfig, validate_contract_config
    )
    cfg = ContractGridConfig(
        market_type=MarketType.USDM,
        symbol="BTCUSDT",
        leverage=5,
        allow_short=True,
        funding_rate_threshold=-0.001,  # -0.1% 時才開空
    )
    ok, reason = validate_contract_config(cfg)
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

log = logging.getLogger(__name__)


# ════════════════════════════════════════════════════════════
# 市場類型枚舉
# ════════════════════════════════════════════════════════════
class MarketType(str, Enum):
    """
    支援的市場類型。

    SPOT  — Binance 現貨市場（目前完整支援）
    USDM  — Binance USDⓈ-M 永續合約（Phase F 預留，Phase G 實作）
    COINM — Binance COIN-M 幣本位合約（未來規劃）
    """
    SPOT  = "spot"
    USDM  = "usdm"     # ⑤ Phase G TODO
    COINM = "coinm"    # 未來規劃


# ════════════════════════════════════════════════════════════
# 合約網格設定
# ════════════════════════════════════════════════════════════
@dataclass
class ContractGridConfig:
    """
    ⑤ USDⓈ-M 合約網格設定（Phase F 預留層）

    現階段欄位以注釋標記其用途，供 Phase G 實作時直接填入邏輯。
    """
    # 基本識別
    market_type:    MarketType = MarketType.SPOT
    symbol:         str        = "BTCUSDT"

    # ── 槓桿設定 ──────────────────────────────────────────
    leverage:       int   = 1           # 槓桿倍數（1 = 全倉無槓桿；USDM 建議 2-5）
    margin_type:    str   = "ISOLATED"  # "ISOLATED" | "CROSSED"
    # TODO Phase G: 呼叫 client.change_leverage(symbol, leverage) 設定槓桿

    # ── 做空設定 ──────────────────────────────────────────
    allow_short:    bool  = False       # 是否允許開空（USDM 才有意義）
    short_ratio:    float = 0.3         # 對沖空單佔總倉位比例（0.3 = 30%）
    # TODO Phase G: 當 allow_short=True 時，在現貨多單對應位置同步開合約空單

    # ── 資金費率套利 ───────────────────────────────────────
    use_funding_arb:         bool  = False    # 是否啟用資金費率套利
    funding_rate_threshold:  float = -0.001  # 負費率低於此值才開空（預設 -0.1%）
    # TODO Phase G: 定期查詢 GET /fapi/v1/premiumIndex，
    #               funding_rate < threshold → 開空，正費率 → 做多

    # ── 風險保護 ───────────────────────────────────────────
    max_leverage:       int   = 10        # 允許的最大槓桿（防止設定錯誤）
    liq_buffer_pct:     float = 0.15     # 強平價緩衝（維持 15% 距離，低於時減倉）
    # TODO Phase G: 監控 maintenanceMarginRate，接近時觸發 KillSwitch

    # ── 現貨 + 合約對沖模式 ────────────────────────────────
    hedge_mode:         bool  = False    # 是否啟用對沖網格（現貨多 + 合約空）
    hedge_symbol:       str   = ""       # 合約對沖幣種（通常與 symbol 相同）
    # TODO Phase G: HedgeGridBot 繼承 GridBotInstance，
    #               每次現貨買入時同步在合約側開對應空單

    # ── 資金費率快照（執行時填入）──────────────────────────
    last_funding_rate:  float = 0.0      # 最近一次資金費率
    next_funding_time:  int   = 0        # 下次費率結算時間（毫秒）


# ════════════════════════════════════════════════════════════
# 驗證工具
# ════════════════════════════════════════════════════════════
def validate_contract_config(cfg: ContractGridConfig) -> tuple[bool, str]:
    """
    ⑤ 驗證合約網格設定合法性。

    Returns:
        (ok, reason) — ok=False 時 reason 說明問題
    """
    if cfg.market_type == MarketType.SPOT:
        if cfg.allow_short:
            return False, "現貨市場不支援做空（allow_short=True）"
        if cfg.leverage > 1:
            return False, "現貨市場不支援槓桿（leverage > 1）"
        return True, ""

    # USDM / COINM 驗證
    if cfg.leverage < 1:
        return False, f"槓桿不得小於 1（現為 {cfg.leverage}）"
    if cfg.leverage > cfg.max_leverage:
        return False, f"槓桿 {cfg.leverage} 超過上限 {cfg.max_leverage}"
    if cfg.margin_type not in ("ISOLATED", "CROSSED"):
        return False, f"不支援的 margin_type: {cfg.margin_type}"
    if cfg.short_ratio < 0 or cfg.short_ratio > 1:
        return False, f"short_ratio 必須在 [0, 1]（現為 {cfg.short_ratio}）"
    if cfg.liq_buffer_pct < 0.05:
        return False, f"liq_buffer_pct 過低，強平風險過高（現為 {cfg.liq_buffer_pct:.1%}）"

    # TODO Phase G: 確認 symbol 在合約市場存在（GET /fapi/v1/exchangeInfo）
    log.info(
        f"[ContractCfg] ⑤ USDM 設定驗證通過 symbol={cfg.symbol} "
        f"leverage={cfg.leverage}× margin={cfg.margin_type} "
        f"short={cfg.allow_short} hedge={cfg.hedge_mode}"
    )
    return True, ""


# ════════════════════════════════════════════════════════════
# 合約專用計算工具（Phase G 實作）
# ════════════════════════════════════════════════════════════
def calc_liquidation_price(
    entry_price:   float,
    leverage:      int,
    side:          str,   # "LONG" | "SHORT"
    maintenance_margin_rate: float = 0.004,  # Binance BTCUSDT 預設 0.4%
) -> float:
    """
    ⑤ 估算合約強平價格（簡化公式，Phase G 以精確公式取代）。

    強平條件（簡化）：
      LONG:  liq_px = entry × (1 - 1/leverage + maintenance_margin_rate)
      SHORT: liq_px = entry × (1 + 1/leverage - maintenance_margin_rate)

    TODO Phase G: 使用 Binance USDM 精確強平公式
                  （含手續費、跨幣種合約系數等）
    """
    if leverage <= 0:
        return 0.0

    if side.upper() == "LONG":
        liq_px = entry_price * (1.0 - 1.0 / leverage + maintenance_margin_rate)
    else:
        liq_px = entry_price * (1.0 + 1.0 / leverage - maintenance_margin_rate)

    return round(liq_px, 8)


def get_funding_rate_signal(
    funding_rate:  float,
    threshold:     float = -0.001,
) -> str:
    """
    ⑤ 根據資金費率給出交易方向建議。

    規則：
      funding_rate < threshold（如 -0.1%）→ 建議開空（做空可收資金費）
      funding_rate > |threshold|          → 建議做多（做多可收資金費）
      其他                                → 中性

    TODO Phase G: 整合至 GridBotInstance.tick()，
                  funding 偏離時自動調整 allow_buy / allow_short
    """
    if funding_rate < threshold:
        return "SHORT_BIAS"   # 空頭優勢（可收資金費）
    elif funding_rate > abs(threshold):
        return "LONG_BIAS"    # 多頭優勢（可收資金費）
    else:
        return "NEUTRAL"
