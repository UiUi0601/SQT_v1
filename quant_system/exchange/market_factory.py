"""
quant_system/exchange/market_factory.py — 多市場工廠

根據 grid_config 中的 market_type 建立正確的：
  - AsyncExchangeClient（設定好業務線）
  - OrderManager（含精度元數據）
  - BaseRiskMonitor 子類（SPOT / MARGIN / FUTURES）
  - StreamMarket（供 BinanceUserStream 使用）
"""
from __future__ import annotations

import logging
import os
from typing import Dict, Optional

log = logging.getLogger(__name__)


class MarketFactory:
    """
    靜態工廠：依 cfg["market_type"] 分派正確的客戶端與風險監控器。

    支援的 market_type 值：
      "SPOT"    — 現貨
      "MARGIN"  — 全倉 / 逐倉槓桿
      "FUTURES" — U 本位永續合約（fapi）
    """

    # ── 建立 AsyncExchangeClient ──────────────────────────────
    @staticmethod
    def create_client(cfg: Dict, api_key: str = "", api_secret: str = "",
                      testnet: bool = True):
        """
        根據 market_type 建立並返回已開啟的 AsyncExchangeClient。
        呼叫者應在使用後呼叫 client.close()。
        """
        from quant_system.exchange.async_client import AsyncExchangeClient, MarketType

        mt = cfg.get("market_type", "SPOT").upper()
        market_map = {
            "SPOT":    MarketType.SPOT,
            "MARGIN":  MarketType.MARGIN,
            "FUTURES": MarketType.FUTURES,
        }
        market = market_map.get(mt, MarketType.SPOT)

        key    = api_key    or os.getenv("BINANCE_API_KEY",    "")
        secret = api_secret or os.getenv("BINANCE_API_SECRET", "")
        tn     = testnet if testnet is not None else (
            os.getenv("BINANCE_TESTNET", "true").lower() == "true"
        )

        client = AsyncExchangeClient(
            api_key=key, api_secret=secret,
            market=market, testnet=tn,
        )
        log.info("[MarketFactory] 建立客戶端 market=%s testnet=%s", mt, tn)
        return client

    # ── 建立 OrderManager ─────────────────────────────────────
    @staticmethod
    def create_order_router(cfg: Dict, client, metadata=None,
                            on_fill=None, on_reject=None):
        """
        建立 OrderManager，依 market_type 設定 hedge_mode。

        Parameters
        ----------
        cfg      : grid_config dict（需含 market_type, position_side_mode）
        client   : AsyncExchangeClient（已初始化，與 market_type 對應）
        metadata : BinanceMetadata（選填；有則啟用精度截斷）
        """
        from quant_system.execution.order_manager import OrderManager

        hedge_mode = (
            cfg.get("market_type", "SPOT").upper() == "FUTURES"
            and cfg.get("position_side_mode", "BOTH").upper() == "HEDGE"
        )

        return OrderManager(
            client     = client,
            metadata   = metadata,
            hedge_mode = hedge_mode,
            on_fill    = on_fill,
            on_reject  = on_reject,
        )

    # ── 建立風險監控器 ────────────────────────────────────────
    @staticmethod
    def create_risk_monitor(cfg: Dict, risk_mgr):
        """
        依 market_type 回傳對應的 BaseRiskMonitor 子類。

        Parameters
        ----------
        cfg      : grid_config dict
        risk_mgr : RiskManager（已 hydrate）
        """
        from quant_system.risk.market_risk_monitors import (
            SpotRiskMonitor, MarginRiskMonitor, FuturesRiskMonitor,
        )

        mt = cfg.get("market_type", "SPOT").upper()
        symbol = cfg.get("symbol", "")

        if mt == "FUTURES":
            return FuturesRiskMonitor(
                risk_mgr   = risk_mgr,
                symbol     = symbol,
                leverage   = int(cfg.get("leverage", 1)),
                liq_buffer = float(os.getenv("LIQ_BUFFER_PCT", "0.15")),
            )
        elif mt == "MARGIN":
            return MarginRiskMonitor(
                risk_mgr      = risk_mgr,
                symbol        = symbol,
                warn_level_pct = float(os.getenv("MARGIN_LEVEL_WARN_PCT", "150")),
            )
        else:
            return SpotRiskMonitor(risk_mgr=risk_mgr, symbol=symbol)

    # ── 取得 StreamMarket ─────────────────────────────────────
    @staticmethod
    def get_stream_market(cfg: Dict):
        """
        返回對應 BinanceUserStream 的 StreamMarket 枚舉值。
        """
        from quant_system.exchange.websocket_client import StreamMarket

        mt = cfg.get("market_type", "SPOT").upper()
        mapping = {
            "SPOT":    StreamMarket.SPOT,
            "MARGIN":  StreamMarket.MARGIN,
            "FUTURES": StreamMarket.FUTURES,
        }
        return mapping.get(mt, StreamMarket.SPOT)
