#!/usr/bin/env python3
"""
TradeStation futures strategy example.

Demonstrates a simple entry/exit pattern for Gold futures (GCJ26):
  - Enters LONG on each new 15-minute bar (demo only — not a real signal)
  - Places a stop-loss 10 ticks below entry
  - Places a take-profit 20 ticks above entry
  - Cancels any open entry orders that have not yet filled

Set credentials via environment variables before running:

    export TRADESTATION_CLIENT_ID="..."
    export TRADESTATION_CLIENT_SECRET="..."
    export TRADESTATION_REFRESH_TOKEN="..."
    export TRADESTATION_ACCOUNT_ID="SIM..."

Run:

    python tradestation_futures_example.py
"""

import os
from decimal import Decimal

from nautilus_tradestation.config import (
    TradeStationDataClientConfig,
    TradeStationExecClientConfig,
)
from nautilus_tradestation.factories import (
    TradeStationLiveDataClientFactory,
    TradeStationLiveExecClientFactory,
)
from nautilus_trader.config import LiveExecEngineConfig
from nautilus_trader.live.node import TradingNode, TradingNodeConfig
from nautilus_trader.model.data import Bar, BarType
from nautilus_trader.model.enums import OrderSide, TimeInForce
from nautilus_trader.model.identifiers import ClientOrderId, InstrumentId, TraderId
from nautilus_trader.model.objects import Price, Quantity
from nautilus_trader.trading.config import StrategyConfig
from nautilus_trader.trading.strategy import Strategy


TICK_SIZE = 0.1   # Gold tick: $0.10
TICKS_SL = 10     # Stop-loss distance in ticks
TICKS_TP = 20     # Take-profit distance in ticks


class GoldFuturesStrategy(Strategy):
    """Demo strategy: enter long on each bar, place SL/TP exits."""

    def __init__(self, instrument_id: InstrumentId, bar_type: BarType):
        super().__init__(config=StrategyConfig(strategy_id="GOLD-DEMO"))
        self.instrument_id = instrument_id
        self.bar_type = bar_type
        self._active_entry: ClientOrderId | None = None

    def on_start(self) -> None:
        self.instrument = self.cache.instrument(self.instrument_id)
        if not self.instrument:
            self.log.error(f"Instrument not found: {self.instrument_id}")
            return
        self.subscribe_bars(self.bar_type)
        self.log.info(f"Strategy started — subscribed to {self.bar_type}")

    def on_bar(self, bar: Bar) -> None:
        # Cancel any unfilled entry from the previous bar
        if self._active_entry:
            self.cancel_order(self.cache.order(self._active_entry))
            self._active_entry = None

        # Only enter when flat
        position = self._get_position()
        if position is not None:
            return

        # Demo signal: enter long on every bar close
        entry_price = bar.close
        sl_price = Price(
            round(float(entry_price) - TICKS_SL * TICK_SIZE, self.instrument.price_precision),
            self.instrument.price_precision,
        )
        tp_price = Price(
            round(float(entry_price) + TICKS_TP * TICK_SIZE, self.instrument.price_precision),
            self.instrument.price_precision,
        )

        entry = self.order_factory.limit(
            instrument_id=self.instrument_id,
            order_side=OrderSide.BUY,
            quantity=Quantity.from_int(1),
            price=entry_price,
            time_in_force=TimeInForce.DAY,
        )
        self.submit_order(entry)
        self._active_entry = entry.client_order_id
        self.log.info(f"Entry submitted @ {entry_price}  SL={sl_price}  TP={tp_price}")

    def on_stop(self) -> None:
        self.cancel_all_orders(self.instrument_id)
        self.close_all_positions(self.instrument_id)

    def _get_position(self):
        positions = self.cache.positions_open(
            instrument_id=self.instrument_id, strategy_id=self.id
        )
        return positions[0] if positions else None



def build_config(account_id: str) -> TradingNodeConfig:
    instrument_id = "GCJ26.TRADESTATION"

    return TradingNodeConfig(
        trader_id=TraderId("FUTURES-DEMO"),
        exec_engine=LiveExecEngineConfig(
            reconciliation=True,
            reconciliation_lookback_mins=1440,
        ),
        data_clients={"TRADESTATION": TradeStationDataClientConfig(
            use_sandbox=True,
            instrument_ids=(instrument_id,),
        )},
        exec_clients={"TRADESTATION": TradeStationExecClientConfig(
            account_id=account_id,
            use_sandbox=True,
        )},
        logging={"log_level": "INFO"},
        timeout_connection=30.0,
    )


if __name__ == "__main__":
    account_id = os.getenv("TRADESTATION_ACCOUNT_ID")
    if not account_id:
        raise RuntimeError("Set TRADESTATION_ACCOUNT_ID environment variable")
    instrument_id = InstrumentId.from_str("GCJ26.TRADESTATION")
    bar_type = BarType.from_str("GCJ26.TRADESTATION-15-MINUTE-LAST-EXTERNAL")

    strategy = GoldFuturesStrategy(instrument_id=instrument_id, bar_type=bar_type)

    node = TradingNode(config=build_config(account_id))
    node.add_data_client_factory("TRADESTATION", TradeStationLiveDataClientFactory)
    node.add_exec_client_factory("TRADESTATION", TradeStationLiveExecClientFactory)
    node.add_strategy(strategy)
    node.build()

    try:
        node.run()
    finally:
        node.dispose()
