"""
quant_system/utils/scheduler.py — 交易日誌記錄器（JSONL）
"""
from __future__ import annotations
import json, os
from datetime import datetime, timezone
from typing import Any, Dict, Optional


class TradeLogger:
    """以 JSONL 格式記錄每筆買賣、錯誤事件。"""

    def __init__(self, log_path: str = "logs/trades.jsonl") -> None:
        self.log_path = log_path
        os.makedirs(os.path.dirname(log_path) if os.path.dirname(log_path) else ".", exist_ok=True)

    # ── public ───────────────────────────────────────────────
    def log_buy(
        self,
        symbol: str,
        price: float,
        quantity: float,
        strategy: str = "",
        fee: float = 0.0,
        slippage: float = 0.0,
        adj_price: float = 0.0,
        signal_confidence: float = 0.0,
        indicators: Optional[Dict] = None,
        reason: str = "",
    ) -> None:
        self._write(
            event="BUY",
            symbol=symbol,
            price=price,
            quantity=quantity,
            strategy=strategy,
            fee=fee,
            slippage=slippage,
            adj_price=adj_price,
            signal_confidence=signal_confidence,
            indicators=indicators or {},
            reason=reason,
        )

    def log_sell(
        self,
        symbol: str,
        price: float,
        quantity: float,
        strategy: str = "",
        fee: float = 0.0,
        slippage: float = 0.0,
        adj_price: float = 0.0,
        pnl: float = 0.0,
        exit_reason: str = "",
        indicators: Optional[Dict] = None,
    ) -> None:
        self._write(
            event="SELL",
            symbol=symbol,
            price=price,
            quantity=quantity,
            strategy=strategy,
            fee=fee,
            slippage=slippage,
            adj_price=adj_price,
            pnl=pnl,
            exit_reason=exit_reason,
            indicators=indicators or {},
        )

    def log_error(self, symbol: str, error: str) -> None:
        self._write(event="ERROR", symbol=symbol, error=error)

    # ── internal ─────────────────────────────────────────────
    def _write(self, **kwargs: Any) -> None:
        record: Dict[str, Any] = {
            "ts": datetime.now(timezone.utc).isoformat(),
            **kwargs,
        }
        try:
            with open(self.log_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
        except Exception:
            pass
