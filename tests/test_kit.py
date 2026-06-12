"""
Test stubs for TradeStation adapter tests.
"""
import json
from pathlib import Path
from typing import Optional

import pandas as pd

from nautilus_trader.core.uuid import UUID4
from nautilus_trader.model.currencies import USD
from nautilus_trader.model.enums import AssetClass, OrderSide, TimeInForce
from nautilus_trader.model.identifiers import (
    ClientOrderId,
    InstrumentId,
    StrategyId,
    Symbol,
    TraderId,
    Venue,
)
from nautilus_trader.model.instruments import Equity, FuturesContract
from nautilus_trader.model.objects import Price, Quantity
from nautilus_trader.model.orders import MarketOrder


RESOURCES = Path(__file__).parent / "resources"


_DEFAULT_INSTRUMENT_ID = InstrumentId(Symbol("GCJ26"), Venue("TRADESTATION"))


def make_market_order(
    tags: Optional[list[str]] = None,
    instrument_id: InstrumentId = _DEFAULT_INSTRUMENT_ID,
    side: OrderSide = OrderSide.BUY,
    quantity: int = 1,
    client_order_id: str = "O-001",
    strategy_id: str = "S-001",
    trader_id: str = "T-001",
) -> MarketOrder:
    """Build a MarketOrder for use in tests.

    Parameters
    ----------
    tags : list[str] or None
        Optional order tags (e.g. ["purpose=exit", "heal"]).
    instrument_id : InstrumentId
        Instrument to trade (defaults to GCJ26.TRADESTATION).
    side : OrderSide
        BUY or SELL.
    quantity : int
        Number of contracts/shares.
    client_order_id : str
        Client-side order identifier string.
    strategy_id : str
        Strategy identifier string.
    trader_id : str
        Trader identifier string.
    """
    return MarketOrder(
        trader_id=TraderId(trader_id),
        strategy_id=StrategyId(strategy_id),
        instrument_id=instrument_id,
        client_order_id=ClientOrderId(client_order_id),
        order_side=side,
        quantity=Quantity.from_int(quantity),
        time_in_force=TimeInForce.DAY,
        init_id=UUID4(),
        ts_init=0,
        tags=tags,
    )


class TSTestInstrumentStubs:
    """Static factory methods returning pre-built instrument objects for tests."""

    @staticmethod
    def gc_futures_contract() -> FuturesContract:
        """Build a GCJ26 Gold futures contract matching symbol_detail_future.json."""
        instrument_id = InstrumentId(Symbol("GCJ26"), Venue("TRADESTATION"))
        return FuturesContract(
            instrument_id=instrument_id,
            raw_symbol=Symbol("GCJ26"),
            asset_class=AssetClass.COMMODITY,
            currency=USD,
            price_precision=1,
            price_increment=Price(0.1, 1),
            multiplier=Quantity.from_int(100),
            lot_size=Quantity.from_int(1),
            underlying="GC",
            activation_ns=0,
            expiration_ns=pd.Timestamp("2026-04-29", tz="UTC").value,
            ts_event=0,
            ts_init=0,
        )

    @staticmethod
    def es_futures_contract() -> FuturesContract:
        """Build an ESM26 E-mini S&P 500 futures contract."""
        instrument_id = InstrumentId(Symbol("ESM26"), Venue("TRADESTATION"))
        return FuturesContract(
            instrument_id=instrument_id,
            raw_symbol=Symbol("ESM26"),
            asset_class=AssetClass.INDEX,
            currency=USD,
            price_precision=2,
            price_increment=Price(0.25, 2),
            multiplier=Quantity.from_int(50),
            lot_size=Quantity.from_int(1),
            underlying="ES",
            activation_ns=0,
            expiration_ns=pd.Timestamp("2026-06-20", tz="UTC").value,
            ts_event=0,
            ts_init=0,
        )

    @staticmethod
    def aapl_equity() -> Equity:
        """Build an AAPL equity instrument."""
        instrument_id = InstrumentId(Symbol("AAPL"), Venue("TRADESTATION"))
        return Equity(
            instrument_id=instrument_id,
            raw_symbol=Symbol("AAPL"),
            currency=USD,
            price_precision=2,
            price_increment=Price(0.01, 2),
            lot_size=Quantity.from_int(1),
            isin=None,
            ts_event=0,
            ts_init=0,
        )


class TSTestDataStubs:
    """Static factory methods returning raw API response data for tests."""

    @staticmethod
    def bars_response() -> list[dict]:
        """Return the bars fixture as parsed list of bar dicts."""
        data = json.loads((RESOURCES / "bars_response.json").read_text())
        return data.get("Bars", [])

    @staticmethod
    def quote_response() -> dict:
        """Return the quote fixture as a parsed dict."""
        return json.loads((RESOURCES / "quote_response.json").read_text())


class TSTestOrderStubs:
    """Static factory methods returning raw order API response dicts."""

    @staticmethod
    def market_order_filled() -> dict:
        """Return a filled market order response dict."""
        return json.loads((RESOURCES / "order_market_filled.json").read_text())

    @staticmethod
    def limit_order_open() -> dict:
        """Return an open limit order response dict."""
        return json.loads((RESOURCES / "order_limit_open.json").read_text())

    @staticmethod
    def stop_order_filled() -> dict:
        """Return a filled stop-market order response dict."""
        return json.loads((RESOURCES / "order_stop_filled.json").read_text())

    @staticmethod
    def order_canceled() -> dict:
        """Return a canceled order response dict."""
        return json.loads((RESOURCES / "order_canceled.json").read_text())

    @staticmethod
    def positions() -> list[dict]:
        """Return a list of open position dicts."""
        return json.loads((RESOURCES / "positions_response.json").read_text())

    @staticmethod
    def balances() -> dict:
        """Return an account balance dict."""
        return json.loads((RESOURCES / "balances_response.json").read_text())

    @staticmethod
    def accounts() -> dict:
        """Return an accounts response dict."""
        return json.loads((RESOURCES / "accounts_response.json").read_text())

    @staticmethod
    def place_order_response() -> dict:
        """Return a successful place-order response."""
        return json.loads((RESOURCES / "place_order_response.json").read_text())
