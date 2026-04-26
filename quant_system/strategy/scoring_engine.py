"""
quant_system/strategy/scoring_engine.py — 多因子加權決策引擎

設計理念：
  ─ 取代原有的 StrategyRouter（單一策略切換），改以連續評分矩陣決策
  ─ 每個因子輸出 [-1.0, 1.0] 浮點評分：
      +1.0 = 強烈看多；-1.0 = 強烈看空；0.0 = 中性
  ─ 綜合評分 = Σ (因子評分 × 因子權重)，再歸一化至 [-1.0, 1.0]
  ─ 動態閾值決策：> long_threshold → LONG；< -short_threshold → SHORT；否則 → HOLD

加權矩陣（預設，可透過 config 覆蓋）：
  技術指標 (tech)        : 0.30
  動量突破 (momentum)    : 0.20
  均值回歸 (mean_rev)    : 0.10
  趨勢追蹤 (trend)       : 0.15
  資金費率 (funding)     : 0.10
  訂單流失衡 (orderflow) : 0.10
  市場情緒 (sentiment)   : 0.05
  合計                   : 1.00

輸出格式（JSON 日誌）：
  {
    "symbol": "BTCUSDT",
    "timestamp": "2025-01-01T00:00:00Z",
    "decision": "LONG",
    "final_score": 0.72,
    "threshold_long": 0.6,
    "threshold_short": -0.6,
    "factor_contributions": {
      "tech":       {"raw_score": 0.85, "weight": 0.30, "contribution": 0.255},
      "momentum":   {"raw_score": 0.60, "weight": 0.20, "contribution": 0.120},
      ...
    },
    "skipped_factors": ["sentiment"],
    "strategy_signals": {
      "RSI_MACD": "BUY", "MOMENTUM": "BUY", ...
    }
  }

使用範例：
    engine = ScoringEngine()

    score_result = engine.evaluate(
        df         = kline_df,          # OHLCV DataFrame
        symbol     = "BTCUSDT",
        market_ctx = {
            "funding_rate": 0.0003,     # 來自 market_sentiment
            "obi":          0.25,       # 來自 orderflow_analyzer
            "poc":          42000.0,    # 來自 volume_profile
        },
    )
    print(score_result.decision)        # LONG / SHORT / HOLD
    print(score_result.to_json())       # 完整 JSON 日誌
"""
from __future__ import annotations

import json
import logging
import math
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from .base_strategy     import BaseStrategy, SignalResult
from .rsi_macd_strategy import RSIMACDStrategy
from .momentum_strategy import MomentumStrategy
from .mean_reversion_strategy import MeanReversionStrategy
from .trend_strategy    import TrendStrategy

log = logging.getLogger(__name__)


# ════════════════════════════════════════════════════════════
# 常數
# ════════════════════════════════════════════════════════════
_DEFAULT_WEIGHTS: Dict[str, float] = {
    "tech":       0.30,
    "momentum":   0.20,
    "mean_rev":   0.10,
    "trend":      0.15,
    "funding":    0.10,
    "orderflow":  0.10,
    "sentiment":  0.05,
}
assert abs(sum(_DEFAULT_WEIGHTS.values()) - 1.0) < 1e-6, "權重總和必須為 1.0"

# 資金費率閾值（超過此值視為過熱，觸發反向衰減）
_FUNDING_HIGH_THRESHOLD = 0.0005   # 0.05%，大多數幣種的極端值
_FUNDING_LOW_THRESHOLD  = -0.0003  # 負費率，鼓勵做多


# ════════════════════════════════════════════════════════════
# 評分結果資料類別
# ════════════════════════════════════════════════════════════
@dataclass
class FactorScore:
    raw_score:    float          # [-1.0, 1.0]
    weight:       float          # 該因子的權重
    contribution: float          # raw_score × weight
    detail:       str = ""       # 觸發原因說明


@dataclass
class ScoreResult:
    """多因子加權評分的完整結果。"""
    symbol:               str
    decision:             str                      # LONG | SHORT | HOLD
    final_score:          float                    # [-1.0, 1.0]
    threshold_long:       float
    threshold_short:      float
    factor_contributions: Dict[str, FactorScore]   = field(default_factory=dict)
    skipped_factors:      List[str]                = field(default_factory=list)
    strategy_signals:     Dict[str, str]           = field(default_factory=dict)
    timestamp:            str                      = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    elapsed_ms:           float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "symbol":    self.symbol,
            "timestamp": self.timestamp,
            "decision":  self.decision,
            "final_score":       round(self.final_score, 6),
            "threshold_long":    self.threshold_long,
            "threshold_short":   self.threshold_short,
            "factor_contributions": {
                k: {
                    "raw_score":    round(v.raw_score, 6),
                    "weight":       v.weight,
                    "contribution": round(v.contribution, 6),
                    "detail":       v.detail,
                }
                for k, v in self.factor_contributions.items()
            },
            "skipped_factors":  self.skipped_factors,
            "strategy_signals": self.strategy_signals,
            "elapsed_ms":       round(self.elapsed_ms, 2),
        }

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=indent)

    @property
    def is_actionable(self) -> bool:
        return self.decision in ("LONG", "SHORT")


# ════════════════════════════════════════════════════════════
# 策略轉接器：將 BUY/SELL/HOLD → [-1.0, 1.0] 評分
# ════════════════════════════════════════════════════════════
class StrategyAdapter:
    """
    將現有策略（輸出 BUY/SELL/HOLD）包裝為評分介面。

    mapping：
      BUY  → +confidence (正評分)
      SELL → -confidence (負評分)
      HOLD → 0.0
    """

    def __init__(self, strategy: BaseStrategy) -> None:
        self._strategy = strategy

    @property
    def name(self) -> str:
        return self._strategy.name

    def score(self, df: pd.DataFrame, **kwargs) -> Tuple[float, SignalResult]:
        """
        回傳 (score, raw_signal)。
        score: [-1.0, 1.0]
        raw_signal: 原始 SignalResult（供日誌記錄）
        """
        sig = self._strategy.generate_signal(df, **kwargs)
        if sig.signal == "BUY":
            score = max(0.0, min(1.0, sig.confidence))
        elif sig.signal == "SELL":
            score = -max(0.0, min(1.0, sig.confidence))
        else:
            score = 0.0
        return score, sig


# ════════════════════════════════════════════════════════════
# 市場因子計算（內建，不需外部依賴）
# ════════════════════════════════════════════════════════════
def _score_funding_rate(funding_rate: float) -> float:
    """
    資金費率因子評分。

    邏輯（反向指標）：
      費率極高（> 0.0005）→ 多頭過熱 → 負評分（鼓勵做空或觀望）
      費率極低（< -0.0003）→ 空頭過熱 → 正評分（鼓勵做多）
      中性區間 → 0.0

    回傳 [-1.0, 1.0]
    """
    if funding_rate > _FUNDING_HIGH_THRESHOLD:
        # 線性映射：0.0005 ~ 0.002 → -0.2 ~ -1.0
        ratio = (funding_rate - _FUNDING_HIGH_THRESHOLD) / 0.0015
        return -min(1.0, 0.2 + ratio * 0.8)
    elif funding_rate < _FUNDING_LOW_THRESHOLD:
        # 費率極負 → 做多因子
        ratio = (abs(funding_rate) - _FUNDING_LOW_THRESHOLD) / 0.001
        return min(1.0, 0.2 + ratio * 0.8)
    else:
        # 費率正常，輕微傾向（正費率輕壓空；負費率輕壓多）
        return -funding_rate / _FUNDING_HIGH_THRESHOLD * 0.2


def _score_orderflow(obi: float) -> float:
    """
    訂單流失衡（OBI）評分。

    obi > 0 → 買方壓力 → 正評分
    obi < 0 → 賣方壓力 → 負評分
    obi 範圍通常在 [-1, 1]，此處假設已歸一化。
    """
    return max(-1.0, min(1.0, obi))


def _score_volume_profile(
    current_price: float,
    poc: float,
    vah: float,
    val: float,
) -> float:
    """
    成交量分佈（Volume Profile）評分。

    邏輯：
      價格在 VAL 附近（低位支撐區）→ 正評分（潛在買點）
      價格在 VAH 附近（高位壓力區）→ 負評分（潛在賣點）
      價格貼近 POC（最大成交量節點）→ 趨向中性
    """
    if poc <= 0 or vah <= val:
        return 0.0
    range_size = vah - val
    if range_size < 1e-9:
        return 0.0
    # 正規化至 [0, 1]：0 = VAL，1 = VAH
    norm = (current_price - val) / range_size
    # 轉換：底部 → 正分；頂部 → 負分
    return max(-1.0, min(1.0, 0.5 - norm))


def _score_liquidation_flow(liq_volume_usd: float, side: str) -> float:
    """
    爆倉量因子。大額空頭爆倉 → 正評分（軋空）；大額多頭爆倉 → 負評分。

    liq_volume_usd: 近期爆倉名目金額（USD）
    side: "BUY"（空頭被爆）或 "SELL"（多頭被爆）
    閾值：5M USD 以上視為有效訊號，50M USD 以上為強訊號。
    """
    if liq_volume_usd < 1_000_000:
        return 0.0
    # 以 50M 為 1.0 的基準
    score = min(1.0, liq_volume_usd / 50_000_000)
    return score if side == "BUY" else -score


# ════════════════════════════════════════════════════════════
# ScoringEngine — 多因子加權決策引擎
# ════════════════════════════════════════════════════════════
class ScoringEngine:
    """
    多因子加權決策引擎。

    內部整合：
      ─ 4 個技術策略（RSI_MACD、Momentum、MeanReversion、Trend）
      ─ 外部市場因子（資金費率、OBI、Volume Profile、爆倉流量）

    評估流程：
      1. 各技術策略 → 評分轉接器 → [-1, 1] 評分
      2. 合並技術因子（取加權平均）
      3. 接入外部市場因子評分
      4. 計算加權綜合評分
      5. 對比閾值決定 LONG / SHORT / HOLD
      6. 輸出帶完整貢獻度的 ScoreResult 與 JSON 日誌
    """

    def __init__(
        self,
        weights:         Optional[Dict[str, float]] = None,
        long_threshold:  float = 0.55,
        short_threshold: float = 0.55,
        strategy_params: Optional[Dict] = None,
        debug:           bool  = False,
    ) -> None:
        """
        weights: 因子權重（預設見模組頂端 _DEFAULT_WEIGHTS）
        long_threshold:  final_score > 此值判定 LONG（預設 0.55）
        short_threshold: final_score < -此值判定 SHORT（預設 -0.55）
        """
        params = strategy_params or {}

        # ── 技術策略轉接器 ────────────────────────────────────
        self._adapters: Dict[str, StrategyAdapter] = {
            "RSI_MACD":  StrategyAdapter(RSIMACDStrategy(params, debug)),
            "MOMENTUM":  StrategyAdapter(MomentumStrategy(params, debug)),
            "MEAN_REV":  StrategyAdapter(MeanReversionStrategy(params, debug)),
            "TREND":     StrategyAdapter(TrendStrategy(params, debug)),
        }

        # ── 因子映射（技術策略 → 引擎因子名）────────────────
        self._tech_map = {
            "tech":     ["RSI_MACD"],
            "momentum": ["MOMENTUM"],
            "mean_rev": ["MEAN_REV"],
            "trend":    ["TREND"],
        }

        # ── 加權配置 ──────────────────────────────────────────
        self._weights = dict(_DEFAULT_WEIGHTS)
        if weights:
            self._weights.update(weights)
        # 重新歸一化（避免用戶只覆蓋部分權重導致不等於 1.0）
        total = sum(self._weights.values())
        if abs(total - 1.0) > 1e-6:
            self._weights = {k: v / total for k, v in self._weights.items()}

        self._long_threshold  = long_threshold
        self._short_threshold = short_threshold
        self.debug            = debug

        log.info(
            f"[ScoringEngine] 初始化完成 "
            f"long_thr={long_threshold} short_thr={short_threshold} "
            f"weights={self._weights}"
        )

    # ════════════════════════════════════════════════════════
    # 主要評估入口
    # ════════════════════════════════════════════════════════
    def evaluate(
        self,
        df:          pd.DataFrame,
        symbol:      str             = "",
        market_ctx:  Optional[Dict]  = None,
    ) -> ScoreResult:
        """
        多因子綜合評估。

        df: OHLCV DataFrame（至少 50 根 K 線）
        symbol: 交易對（供日誌記錄）
        market_ctx: 外部市場因子字典，支援以下鍵值：
          "funding_rate"    float  資金費率（如 0.0003）
          "obi"             float  訂單簿失衡比例 [-1, 1]
          "poc"             float  Volume Profile 最大成交量節點價格
          "vah"             float  Value Area High
          "val"             float  Value Area Low
          "liq_volume_usd"  float  近期爆倉名目金額（USD）
          "liq_side"        str    爆倉方向 "BUY"/"SELL"

        回傳 ScoreResult（含完整 JSON 可記錄）
        """
        t0  = time.perf_counter()
        ctx = market_ctx or {}

        contributions: Dict[str, FactorScore] = {}
        skipped: List[str] = []
        signals: Dict[str, str] = {}

        # ── 1. 技術策略因子 ────────────────────────────────────
        if len(df) < 35:
            # 資料不足，跳過所有技術因子
            for name in ["tech", "momentum", "mean_rev", "trend"]:
                skipped.append(name)
            log.warning(f"[ScoringEngine] {symbol} K線不足 35 根，跳過技術因子")
        else:
            for factor_name, adapter_keys in self._tech_map.items():
                factor_scores = []
                for k in adapter_keys:
                    adapter = self._adapters.get(k)
                    if adapter is None:
                        continue
                    try:
                        raw, sig = adapter.score(df)
                        factor_scores.append(raw)
                        signals[k] = sig.signal
                        detail = sig.reason
                    except Exception as e:
                        log.warning(f"[ScoringEngine] {k} 評分失敗: {e}")
                        signals[k] = "ERROR"
                        detail = str(e)

                if not factor_scores:
                    skipped.append(factor_name)
                    continue

                avg_score = sum(factor_scores) / len(factor_scores)
                w         = self._weights.get(factor_name, 0.0)
                contributions[factor_name] = FactorScore(
                    raw_score    = avg_score,
                    weight       = w,
                    contribution = avg_score * w,
                    detail       = detail if len(adapter_keys) == 1 else "",
                )

        # ── 2. 資金費率因子 ───────────────────────────────────
        funding_rate = ctx.get("funding_rate")
        if funding_rate is not None:
            fs     = _score_funding_rate(float(funding_rate))
            w      = self._weights.get("funding", 0.0)
            contributions["funding"] = FactorScore(
                raw_score    = fs,
                weight       = w,
                contribution = fs * w,
                detail       = f"funding_rate={funding_rate:.6f}",
            )
        else:
            skipped.append("funding")

        # ── 3. 訂單流失衡（OBI）因子 ─────────────────────────
        obi = ctx.get("obi")
        if obi is not None:
            os_    = _score_orderflow(float(obi))
            w      = self._weights.get("orderflow", 0.0)
            contributions["orderflow"] = FactorScore(
                raw_score    = os_,
                weight       = w,
                contribution = os_ * w,
                detail       = f"OBI={obi:.4f}",
            )
        else:
            skipped.append("orderflow")

        # ── 4. Volume Profile 因子 ────────────────────────────
        poc = ctx.get("poc")
        vah = ctx.get("vah")
        val = ctx.get("val")
        if poc and vah and val and len(df) > 0:
            current_price = float(df["Close"].iloc[-1])
            vp_score = _score_volume_profile(
                current_price, float(poc), float(vah), float(val)
            )
            # Volume Profile 映射至 sentiment 因子（若沒有獨立爆倉資料）
            if "sentiment" not in contributions:
                w = self._weights.get("sentiment", 0.0)
                contributions["sentiment"] = FactorScore(
                    raw_score    = vp_score,
                    weight       = w,
                    contribution = vp_score * w,
                    detail       = f"VP poc={poc:.2f} vah={vah:.2f} val={val:.2f}",
                )
        else:
            skipped.append("sentiment")

        # ── 5. 爆倉流量因子（覆蓋 sentiment）────────────────
        liq_vol  = ctx.get("liq_volume_usd")
        liq_side = ctx.get("liq_side", "BUY")
        if liq_vol is not None and float(liq_vol) >= 1_000_000:
            liq_score = _score_liquidation_flow(float(liq_vol), str(liq_side))
            w         = self._weights.get("sentiment", 0.0)
            contributions["sentiment"] = FactorScore(
                raw_score    = liq_score,
                weight       = w,
                contribution = liq_score * w,
                detail       = f"liq={liq_vol/1e6:.1f}M side={liq_side}",
            )
            skipped = [f for f in skipped if f != "sentiment"]

        # ── 6. 計算最終加權評分 ───────────────────────────────
        # 跳過因子的權重重新歸一化，避免分母縮水
        active_weight = sum(
            self._weights.get(k, 0.0)
            for k in contributions
        )
        if active_weight > 1e-9:
            raw_total = sum(f.contribution for f in contributions.values())
            final_score = raw_total / active_weight  # 歸一化
        else:
            final_score = 0.0

        # 夾緊至 [-1, 1]
        final_score = max(-1.0, min(1.0, final_score))

        # ── 7. 閾值決策 ───────────────────────────────────────
        if final_score >= self._long_threshold:
            decision = "LONG"
        elif final_score <= -self._short_threshold:
            decision = "SHORT"
        else:
            decision = "HOLD"

        elapsed = (time.perf_counter() - t0) * 1000

        result = ScoreResult(
            symbol               = symbol,
            decision             = decision,
            final_score          = final_score,
            threshold_long       = self._long_threshold,
            threshold_short      = -self._short_threshold,
            factor_contributions = contributions,
            skipped_factors      = skipped,
            strategy_signals     = signals,
            elapsed_ms           = elapsed,
        )

        if self.debug or result.is_actionable:
            log.info(
                f"[ScoringEngine] {symbol} decision={decision} "
                f"score={final_score:.4f} "
                f"({len(contributions)} 因子活躍 / {len(skipped)} 跳過) "
                f"耗時 {elapsed:.1f}ms"
            )

        return result

    # ════════════════════════════════════════════════════════
    # 動態閾值調整
    # ════════════════════════════════════════════════════════
    def set_thresholds(
        self,
        long_threshold:  float,
        short_threshold: float,
    ) -> None:
        """動態調整決策閾值（可由風控模組在市場波動加大時收緊）。"""
        self._long_threshold  = long_threshold
        self._short_threshold = short_threshold
        log.info(
            f"[ScoringEngine] 閾值更新：long>{long_threshold} "
            f"short<{-short_threshold}"
        )

    def set_weight(self, factor: str, weight: float) -> None:
        """動態調整單一因子權重，並重新歸一化。"""
        self._weights[factor] = weight
        total = sum(self._weights.values())
        self._weights = {k: v / total for k, v in self._weights.items()}
        log.info(f"[ScoringEngine] 因子 {factor} 權重更新 → {weight:.3f}（已重歸一化）")

    @property
    def weights(self) -> Dict[str, float]:
        return dict(self._weights)

    @property
    def thresholds(self) -> Dict[str, float]:
        return {
            "long":  self._long_threshold,
            "short": -self._short_threshold,
        }
