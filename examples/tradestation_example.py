#!/usr/bin/env python3
"""
Minimal TradeStation live-trading example.

This script demonstrates how to configure and run a simple strategy against
the TradeStation sandbox (simulation) API.  It connects, subscribes to
15-minute Gold bars, and prints each bar to the log.

Set credentials via environment variables before running:

    export TRADESTATION_CLIENT_ID="..."
    export TRADESTATION_CLIENT_SECRET="..."
    export TRADESTATION_REFRESH_TOKEN="..."
    export TRADESTATION_ACCOUNT_ID="SIM..."

Run:

    python tradestation_example.py
"""

import asyncio

from tradestation_nt_community.config import (
    TradeStationDataClientConfig,
    TradeStationExecClientConfig,
)
from tradestation_nt_community.factories import (
    TradeStationLiveDataClientFactory,
    TradeStationLiveExecClientFactory,
)
from nautilus_trader.config import LiveExecEngineConfig
from nautilus_trader.live.node import TradingNode, TradingNodeConfig
from nautilus_trader.model.data import Bar, BarType
from nautilus_trader.model.identifiers import InstrumentId, TraderId
from nautilus_trader.trading.config import StrategyConfig
from nautilus_trader.trading.strategy import Strategy



class PrintBarsStrategy(Strategy):
    """Minimal strategy — subscribes to bars and prints them."""

    def __init__(self, instrument_id: InstrumentId, bar_type: BarType):
        super().__init__(config=StrategyConfig(strategy_id="PRINT-BARS"))
        self.instrument_id = instrument_id
        self.bar_type = bar_type

    def on_start(self) -> None:
        self.subscribe_bars(self.bar_type)
        self.log.info(f"Subscribed to {self.bar_type}")

    def on_bar(self, bar: Bar) -> None:
        self.log.info(
            f"BAR | O={bar.open} H={bar.high} L={bar.low} C={bar.close} V={bar.volume}"
        )

    def on_stop(self) -> None:
        self.unsubscribe_bars(self.bar_type)



def build_config(account_id: str) -> TradingNodeConfig:
    instrument_id = "GCJ26.TRADESTATION"

    data_config = TradeStationDataClientConfig(
        use_sandbox=True,
        instrument_ids=(instrument_id,),  # Pre-load at connect
    )
    exec_config = TradeStationExecClientConfig(
        account_id=account_id,
        use_sandbox=True,
    )

    return TradingNodeConfig(
        trader_id=TraderId("EXAMPLE-001"),
        exec_engine=LiveExecEngineConfig(
            reconciliation=True,
            reconciliation_lookback_mins=1440,
        ),
        data_clients={"TRADESTATION": data_config},
        exec_clients={"TRADESTATION": exec_config},
        logging={"log_level": "INFO"},
        timeout_connection=30.0,
    )



if __name__ == "__main__":
    import os

    account_id = os.getenv("TRADESTATION_ACCOUNT_ID")
    if not account_id:
        raise RuntimeError("Set TRADESTATION_ACCOUNT_ID environment variable")
    instrument_id = InstrumentId.from_str("GCJ26.TRADESTATION")
    bar_type = BarType.from_str("GCJ26.TRADESTATION-15-MINUTE-LAST-EXTERNAL")

    strategy = PrintBarsStrategy(instrument_id=instrument_id, bar_type=bar_type)

    node = TradingNode(config=build_config(account_id))
    node.add_data_client_factory("TRADESTATION", TradeStationLiveDataClientFactory)
    node.add_exec_client_factory("TRADESTATION", TradeStationLiveExecClientFactory)
    node.add_strategy(strategy)
    node.build()

    try:
        node.run()
    finally:
        node.dispose()
