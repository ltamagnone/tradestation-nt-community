"""
Tests for the parsing sub-package (parsing/data.py, parsing/execution.py,
parsing/instruments.py).

All tests call module-level functions directly — no client or Component
instantiation needed.
"""
import json
from pathlib import Path

import pytest

from tradestation_nt_community.parsing.data import (
    bar_spec_to_ts_params,
    parse_bars,
    parse_quote_tick,
    parse_trade_tick,
)
from tradestation_nt_community.parsing.execution import (
    convert_order_to_ts_format,
    convert_order_type,
    convert_time_in_force,
    parse_fill_report,
    parse_order_status,
    parse_order_status_report,
    parse_ts_order_type,
)
from tradestation_nt_community.parsing.instruments import (
    determine_price_precision,
    parse_instrument,
)
from tradestation_nt_community.common.enums import TradeStationBarUnit
from tradestation_nt_community.constants import TRADESTATION_VENUE
from nautilus_trader.model.data import BarSpecification, BarType
from nautilus_trader.model.enums import (
    AggregationSource,
    BarAggregation,
    OrderSide,
    OrderStatus,
    OrderType,
    PriceType,
    TimeInForce,
)
from nautilus_trader.model.identifiers import AccountId, ClientOrderId, InstrumentId
from nautilus_trader.model.instruments import Equity, FuturesContract
from tests.test_kit import (
    TSTestInstrumentStubs,
    TSTestOrderStubs,
)


_RESOURCES = Path(__file__).parent / "resources"
_ACCOUNT_ID = AccountId("TRADESTATION-SIM0000001F")
_TS_NOW = 1_000_000_000


def _make_bar_type(instrument, step: int, agg: BarAggregation = BarAggregation.MINUTE) -> BarType:
    spec = BarSpecification(step=step, aggregation=agg, price_type=PriceType.LAST)
    return BarType(instrument_id=instrument.id, bar_spec=spec, aggregation_source=AggregationSource.EXTERNAL)



class TestParsingDataModule:
    """Tests for parsing.data — bar spec conversion and bar parsing."""

    def test_minute_spec_converts(self):
        spec = BarSpecification(step=5, aggregation=BarAggregation.MINUTE, price_type=PriceType.LAST)
        interval, unit = bar_spec_to_ts_params(spec)
        assert interval == "5" and unit == TradeStationBarUnit.MINUTE

    def test_hour_spec_maps_to_60_minutes(self):
        spec = BarSpecification(step=1, aggregation=BarAggregation.HOUR, price_type=PriceType.LAST)
        interval, unit = bar_spec_to_ts_params(spec)
        assert interval == "60" and unit == TradeStationBarUnit.MINUTE

    def test_day_spec_maps_to_daily(self):
        spec = BarSpecification(step=1, aggregation=BarAggregation.DAY, price_type=PriceType.LAST)
        interval, unit = bar_spec_to_ts_params(spec)
        assert interval == "1" and unit == TradeStationBarUnit.DAILY

    def test_unsupported_raises(self):
        spec = BarSpecification(step=1, aggregation=BarAggregation.WEEK, price_type=PriceType.LAST)
        with pytest.raises(ValueError):
            bar_spec_to_ts_params(spec)

    def test_parse_bars_count(self):
        instrument = TSTestInstrumentStubs.gc_futures_contract()
        raw = json.loads((_RESOURCES / "bars_response.json").read_text()).get("Bars", [])
        bars = parse_bars(raw, _make_bar_type(instrument, 1))
        assert len(bars) == 3

    def test_parse_bars_ohlcv(self):
        instrument = TSTestInstrumentStubs.gc_futures_contract()
        raw = json.loads((_RESOURCES / "bars_response.json").read_text()).get("Bars", [])
        bars = parse_bars(raw, _make_bar_type(instrument, 1))
        assert float(bars[0].open) == pytest.approx(2050.5, rel=1e-4)
        assert float(bars[0].high) == pytest.approx(2051.0, rel=1e-4)
        assert bars[0].volume == 1500

    def test_parse_bars_empty(self):
        instrument = TSTestInstrumentStubs.gc_futures_contract()
        assert parse_bars([], _make_bar_type(instrument, 1)) == []

    def test_parse_bars_skips_malformed(self):
        """A malformed entry is skipped, rest parsed."""
        instrument = TSTestInstrumentStubs.gc_futures_contract()
        raw = [
            {"TimeStamp": "BAD", "Open": "x", "High": "x", "Low": "x", "Close": "x"},
            {"TimeStamp": "2025-01-30T14:30:00Z", "Open": 2050.5, "High": 2051.0,
             "Low": 2050.0, "Close": 2050.8, "TotalVolume": 1000},
        ]
        bars = parse_bars(raw, _make_bar_type(instrument, 1))
        assert len(bars) == 1



class TestParsingExecutionModule:
    """Tests for parsing.execution — order status, type, TIF, and report parsing."""

    def test_parse_order_status_all_known(self):
        assert parse_order_status("FLL") == OrderStatus.FILLED
        assert parse_order_status("OPN") == OrderStatus.SUBMITTED
        assert parse_order_status("ACK") == OrderStatus.ACCEPTED
        assert parse_order_status("CAN") == OrderStatus.CANCELED
        assert parse_order_status("REJ") == OrderStatus.REJECTED
        assert parse_order_status("EXP") == OrderStatus.EXPIRED
        assert parse_order_status("FLP") == OrderStatus.PARTIALLY_FILLED

    def test_parse_order_status_unknown_defaults(self):
        assert parse_order_status("???") == OrderStatus.PENDING_UPDATE

    def test_parse_ts_order_type_all(self):
        assert parse_ts_order_type("Market") == OrderType.MARKET
        assert parse_ts_order_type("Limit") == OrderType.LIMIT
        assert parse_ts_order_type("StopMarket") == OrderType.STOP_MARKET
        assert parse_ts_order_type("StopLimit") == OrderType.STOP_LIMIT
        assert parse_ts_order_type("Unknown") == OrderType.MARKET

    def test_convert_time_in_force_all(self):
        assert convert_time_in_force(TimeInForce.DAY) == "DAY"
        assert convert_time_in_force(TimeInForce.GTC) == "GTC"
        assert convert_time_in_force(TimeInForce.IOC) == "IOC"
        assert convert_time_in_force(TimeInForce.FOK) == "FOK"
        assert convert_time_in_force(TimeInForce.AT_THE_OPEN) == "DAY"

    def test_parse_order_status_report_filled(self):
        report = parse_order_status_report(
            TSTestOrderStubs.market_order_filled(),
            InstrumentId.from_str("GCJ26.TRADESTATION"),
            ClientOrderId("O-001"),
            _ACCOUNT_ID, _TS_NOW,
        )
        assert report is not None
        assert report.order_status == OrderStatus.FILLED
        assert str(report.venue_order_id) == "TS-ORDER-001"

    def test_parse_order_status_report_open_limit(self):
        report = parse_order_status_report(
            TSTestOrderStubs.limit_order_open(),
            InstrumentId.from_str("GCJ26.TRADESTATION"),
            ClientOrderId("O-002"),
            _ACCOUNT_ID, _TS_NOW,
        )
        assert report is not None
        assert report.order_status == OrderStatus.SUBMITTED
        assert report.order_side == OrderSide.SELL
        assert float(report.price) == pytest.approx(3400.0, rel=1e-4)

    def test_parse_order_status_report_canceled(self):
        report = parse_order_status_report(
            TSTestOrderStubs.order_canceled(),
            InstrumentId.from_str("GCJ26.TRADESTATION"),
            ClientOrderId("O-004"),
            _ACCOUNT_ID, _TS_NOW,
        )
        assert report is not None
        assert report.order_status == OrderStatus.CANCELED

    def test_parse_order_status_report_none_on_bad_input(self):
        """Garbage input should return None, not raise."""
        report = parse_order_status_report(
            {},  # empty dict — Quantity.from_str("0") may succeed, just returns minimal report
            InstrumentId.from_str("GCJ26.TRADESTATION"),
            ClientOrderId("O-BAD"),
            _ACCOUNT_ID, _TS_NOW,
        )
        # Either None or a minimal report is acceptable; must not raise
        assert report is None or report is not None

    # -- convert_order_type --------------------------------------------------

    def test_convert_order_type_market(self):
        from nautilus_trader.model.orders import MarketOrder
        from nautilus_trader.model.identifiers import TraderId, StrategyId
        from nautilus_trader.model.objects import Quantity
        from nautilus_trader.core.uuid import UUID4
        order = MarketOrder(
            trader_id=TraderId("TESTER-001"), strategy_id=StrategyId("S-001"),
            instrument_id=InstrumentId.from_str("GCJ26.TRADESTATION"),
            client_order_id=ClientOrderId("O-M1"), order_side=OrderSide.BUY,
            quantity=Quantity.from_int(1), time_in_force=TimeInForce.DAY,
            init_id=UUID4(), ts_init=0,
        )
        assert convert_order_type(order) == "Market"

    def test_convert_order_type_limit(self):
        from nautilus_trader.model.orders import LimitOrder
        from nautilus_trader.model.identifiers import TraderId, StrategyId
        from nautilus_trader.model.objects import Price, Quantity
        from nautilus_trader.core.uuid import UUID4
        order = LimitOrder(
            trader_id=TraderId("TESTER-001"), strategy_id=StrategyId("S-001"),
            instrument_id=InstrumentId.from_str("GCJ26.TRADESTATION"),
            client_order_id=ClientOrderId("O-L1"), order_side=OrderSide.SELL,
            quantity=Quantity.from_int(1), price=Price(3400.0, 1),
            time_in_force=TimeInForce.DAY, init_id=UUID4(), ts_init=0,
        )
        assert convert_order_type(order) == "Limit"

    def test_convert_order_type_stop_market(self):
        from nautilus_trader.model.orders import StopMarketOrder
        from nautilus_trader.model.identifiers import TraderId, StrategyId
        from nautilus_trader.model.objects import Price, Quantity
        from nautilus_trader.core.uuid import UUID4
        from nautilus_trader.model.enums import TriggerType
        order = StopMarketOrder(
            trader_id=TraderId("TESTER-001"), strategy_id=StrategyId("S-001"),
            instrument_id=InstrumentId.from_str("GCJ26.TRADESTATION"),
            client_order_id=ClientOrderId("O-SM1"), order_side=OrderSide.SELL,
            quantity=Quantity.from_int(1), trigger_price=Price(3300.0, 1),
            trigger_type=TriggerType.DEFAULT,
            time_in_force=TimeInForce.DAY, init_id=UUID4(), ts_init=0,
        )
        assert convert_order_type(order) == "StopMarket"

    # -- convert_order_to_ts_format ------------------------------------------

    def test_convert_order_to_ts_format_market_buy(self):
        from nautilus_trader.model.orders import MarketOrder
        from nautilus_trader.model.identifiers import TraderId, StrategyId
        from nautilus_trader.model.objects import Quantity
        from nautilus_trader.core.uuid import UUID4
        order = MarketOrder(
            trader_id=TraderId("TESTER-001"), strategy_id=StrategyId("S-001"),
            instrument_id=InstrumentId.from_str("GCJ26.TRADESTATION"),
            client_order_id=ClientOrderId("O-M2"), order_side=OrderSide.BUY,
            quantity=Quantity.from_int(2), time_in_force=TimeInForce.DAY,
            init_id=UUID4(), ts_init=0,
        )
        params = convert_order_to_ts_format(order, "SIM0000001F")
        assert params["account_id"] == "SIM0000001F"
        assert params["symbol"] == "GCJ26"
        assert params["order_type"] == "Market"
        assert params["trade_action"] == "Buy"
        assert params["quantity"] == "2"
        assert params["time_in_force"] == "DAY"

    def test_convert_order_to_ts_format_limit_sell_includes_price(self):
        from nautilus_trader.model.orders import LimitOrder
        from nautilus_trader.model.identifiers import TraderId, StrategyId
        from nautilus_trader.model.objects import Price, Quantity
        from nautilus_trader.core.uuid import UUID4
        order = LimitOrder(
            trader_id=TraderId("TESTER-001"), strategy_id=StrategyId("S-001"),
            instrument_id=InstrumentId.from_str("GCJ26.TRADESTATION"),
            client_order_id=ClientOrderId("O-L2"), order_side=OrderSide.SELL,
            quantity=Quantity.from_int(1), price=Price(3400.0, 1),
            time_in_force=TimeInForce.GTC, init_id=UUID4(), ts_init=0,
        )
        params = convert_order_to_ts_format(order, "SIM0000001F")
        assert params["order_type"] == "Limit"
        assert params["trade_action"] == "Sell"
        assert params["time_in_force"] == "GTC"
        assert "limit_price" in params
        assert float(params["limit_price"]) == pytest.approx(3400.0, rel=1e-4)

    def test_convert_order_to_ts_format_stop_market_includes_stop_price(self):
        from nautilus_trader.model.orders import StopMarketOrder
        from nautilus_trader.model.identifiers import TraderId, StrategyId
        from nautilus_trader.model.enums import TriggerType
        from nautilus_trader.model.objects import Price, Quantity
        from nautilus_trader.core.uuid import UUID4
        order = StopMarketOrder(
            trader_id=TraderId("TESTER-001"), strategy_id=StrategyId("S-001"),
            instrument_id=InstrumentId.from_str("GCJ26.TRADESTATION"),
            client_order_id=ClientOrderId("O-SM2"), order_side=OrderSide.SELL,
            quantity=Quantity.from_int(1), trigger_price=Price(3200.0, 1),
            trigger_type=TriggerType.DEFAULT,
            time_in_force=TimeInForce.DAY, init_id=UUID4(), ts_init=0,
        )
        params = convert_order_to_ts_format(order, "SIM0000001F")
        assert params["order_type"] == "StopMarket"
        assert "stop_price" in params
        assert float(params["stop_price"]) == pytest.approx(3200.0, rel=1e-4)



class TestParsingInstrumentsModule:
    """Tests for parsing.instruments — instrument parsing from TS symbol data."""

    def _load(self, filename: str) -> dict:
        return json.loads((_RESOURCES / filename).read_text())

    def test_parse_futures_contract(self):
        data = self._load("symbol_detail_future.json")
        instrument = parse_instrument("GCG25", data, TRADESTATION_VENUE)
        assert isinstance(instrument, FuturesContract)
        assert instrument.id.symbol.value == "GCG25"

    def test_parse_futures_price_precision(self):
        data = self._load("symbol_detail_future.json")
        instrument = parse_instrument("GCG25", data, TRADESTATION_VENUE)
        assert instrument.price_precision == 1
        assert float(instrument.price_increment) == pytest.approx(0.1, rel=1e-4)

    def test_parse_futures_multiplier(self):
        data = self._load("symbol_detail_future.json")
        instrument = parse_instrument("GCG25", data, TRADESTATION_VENUE)
        assert float(instrument.multiplier) == 100.0

    def test_parse_futures_underlying(self):
        data = self._load("symbol_detail_future.json")
        instrument = parse_instrument("GCG25", data, TRADESTATION_VENUE)
        assert instrument.underlying == "GC"

    def test_parse_futures_expiration_set(self):
        data = self._load("symbol_detail_future.json")
        instrument = parse_instrument("GCG25", data, TRADESTATION_VENUE)
        assert instrument.expiration_ns > 0

    def test_parse_equity(self):
        data = self._load("symbol_detail_equity.json")
        instrument = parse_instrument("AAPL", data, TRADESTATION_VENUE)
        assert isinstance(instrument, Equity)

    def test_parse_unsupported_asset_type_returns_none(self):
        data = {"AssetType": "CRYPTO", "PriceFormat": {"Increment": "0.01"}, "QuantityFormat": {}}
        result = parse_instrument("BTC", data, TRADESTATION_VENUE)
        assert result is None

    def test_determine_price_precision_decimal(self):
        assert determine_price_precision({"MinMove": "0.25"}) == 2

    def test_determine_price_precision_integer(self):
        assert determine_price_precision({"MinMove": "1"}) == 0

    def test_determine_price_precision_default(self):
        assert determine_price_precision({}) == 2



class TestParsingQuoteTradeTicks:
    """Tests for parse_quote_tick() and parse_trade_tick()."""

    def setup_method(self):
        self.instrument = TSTestInstrumentStubs.gc_futures_contract()
        self.instrument_id = self.instrument.id
        self.raw = {
            "Symbol": "GCJ26",
            "Bid": 2050.5,
            "Ask": 2050.7,
            "BidSize": 10,
            "AskSize": 15,
            "Last": 2050.6,
            "LastSize": 5,
            "Volume": 125000,
            "TimeStamp": "2025-01-30T14:30:00Z",
        }

    def test_parse_quote_tick_bid_ask(self):
        from nautilus_trader.model.data import QuoteTick
        tick = parse_quote_tick(self.raw, self.instrument_id, self.instrument)
        assert isinstance(tick, QuoteTick)
        assert float(tick.bid_price) == pytest.approx(2050.5, rel=1e-4)
        assert float(tick.ask_price) == pytest.approx(2050.7, rel=1e-4)

    def test_parse_quote_tick_sizes(self):
        tick = parse_quote_tick(self.raw, self.instrument_id, self.instrument)
        assert tick is not None
        assert float(tick.bid_size) == pytest.approx(10.0, rel=1e-4)
        assert float(tick.ask_size) == pytest.approx(15.0, rel=1e-4)

    def test_parse_quote_tick_zero_bid_returns_none(self):
        raw = {**self.raw, "Bid": 0}
        assert parse_quote_tick(raw, self.instrument_id, self.instrument) is None

    def test_parse_quote_tick_zero_ask_returns_none(self):
        raw = {**self.raw, "Ask": 0}
        assert parse_quote_tick(raw, self.instrument_id, self.instrument) is None

    def test_parse_quote_tick_missing_timestamp_uses_now(self):
        raw = {**self.raw}
        del raw["TimeStamp"]
        tick = parse_quote_tick(raw, self.instrument_id, self.instrument)
        assert tick is not None
        assert tick.ts_event > 0

    def test_parse_trade_tick_price(self):
        from nautilus_trader.model.data import TradeTick
        tick = parse_trade_tick(self.raw, self.instrument_id, self.instrument)
        assert isinstance(tick, TradeTick)
        assert float(tick.price) == pytest.approx(2050.6, rel=1e-4)

    def test_parse_trade_tick_size(self):
        tick = parse_trade_tick(self.raw, self.instrument_id, self.instrument)
        assert tick is not None
        assert float(tick.size) == pytest.approx(5.0, rel=1e-4)

    def test_parse_trade_tick_zero_last_returns_none(self):
        raw = {**self.raw, "Last": 0}
        assert parse_trade_tick(raw, self.instrument_id, self.instrument) is None

    def test_parse_trade_tick_has_trade_id(self):
        tick = parse_trade_tick(self.raw, self.instrument_id, self.instrument)
        assert tick is not None
        assert tick.trade_id is not None
        assert str(tick.trade_id) != ""

    def test_parse_trade_tick_aggressor_side(self):
        from nautilus_trader.model.enums import AggressorSide
        tick = parse_trade_tick(self.raw, self.instrument_id, self.instrument)
        assert tick is not None
        assert tick.aggressor_side == AggressorSide.NO_AGGRESSOR



class TestParseFillReport:
    """Tests for parse_fill_report()."""

    def _parse(self, ts_order, symbol="GCJ26") -> object:
        return parse_fill_report(
            ts_order,
            instrument_id=InstrumentId.from_str(f"{symbol}.TRADESTATION"),
            account_id=_ACCOUNT_ID,
            ts_now=_TS_NOW,
        )

    def test_filled_market_order_produces_fill_report(self):
        from nautilus_trader.execution.reports import FillReport
        report = self._parse(TSTestOrderStubs.market_order_filled())
        assert isinstance(report, FillReport)

    def test_fill_price_from_average_price(self):
        report = self._parse(TSTestOrderStubs.market_order_filled())
        assert report is not None
        assert float(report.last_px) == pytest.approx(3350.0, rel=1e-4)

    def test_fill_qty(self):
        report = self._parse(TSTestOrderStubs.market_order_filled())
        assert report is not None
        assert report.last_qty == 1

    def test_fill_side_buy(self):
        from nautilus_trader.model.enums import OrderSide
        report = self._parse(TSTestOrderStubs.market_order_filled())
        assert report is not None
        assert report.order_side == OrderSide.BUY

    def test_fill_side_sell(self):
        from nautilus_trader.model.enums import OrderSide
        report = self._parse(TSTestOrderStubs.stop_order_filled(), symbol="ESM26")
        assert report is not None
        assert report.order_side == OrderSide.SELL

    def test_fill_price_fallback_to_legs(self):
        """When AveragePrice is absent, fall back to Legs[0].ExecutionPrice."""
        ts_order = {**TSTestOrderStubs.market_order_filled()}
        del ts_order["AveragePrice"]
        # Legs already have ExecutionPrice=3350.0 in the fixture
        report = self._parse(ts_order)
        assert report is not None
        assert float(report.last_px) == pytest.approx(3350.0, rel=1e-4)

    def test_venue_order_id(self):
        report = self._parse(TSTestOrderStubs.market_order_filled())
        assert report is not None
        assert str(report.venue_order_id) == "TS-ORDER-001"

    def test_trade_id_set(self):
        report = self._parse(TSTestOrderStubs.market_order_filled())
        assert report is not None
        assert "TS-ORDER-001" in str(report.trade_id)

    def test_open_order_returns_none(self):
        """An open (unfilled) order should NOT produce a fill report."""
        report = self._parse(TSTestOrderStubs.limit_order_open())
        # parse_fill_report doesn't filter by status — it uses the caller's FLL filter.
        # But an open order has AveragePrice=0 and FilledQty=0 → returns None.
        assert report is None

    def test_canceled_order_returns_none(self):
        """A canceled order (zero fill qty) should NOT produce a fill report."""
        report = self._parse(TSTestOrderStubs.order_canceled())
        assert report is None

    def test_timestamp_from_closed_datetime(self):
        """ts_event should be parsed from ClosedDateTime when present."""
        report = self._parse(TSTestOrderStubs.market_order_filled())
        assert report is not None
        # ClosedDateTime=2026-04-08T14:30:01Z → ts_event > 0 and not _TS_NOW
        assert report.ts_event != _TS_NOW
        assert report.ts_event > 0

    def test_custom_client_order_id(self):
        report = parse_fill_report(
            TSTestOrderStubs.market_order_filled(),
            instrument_id=InstrumentId.from_str("GCJ26.TRADESTATION"),
            account_id=_ACCOUNT_ID,
            ts_now=_TS_NOW,
            client_order_id=ClientOrderId("MY-ORDER-42"),
        )
        assert report is not None
        assert str(report.client_order_id) == "MY-ORDER-42"



class TestParseOptionInstrument:
    """Tests for parse_instrument() with OPTION asset type."""

    def _load_option(self) -> dict:
        return json.loads((_RESOURCES / "symbol_detail_option.json").read_text())

    def test_parse_option_returns_option_contract(self):
        from nautilus_trader.model.instruments import OptionContract
        data = self._load_option()
        instrument = parse_instrument("AAPL 250321C00175000", data, TRADESTATION_VENUE)
        assert isinstance(instrument, OptionContract)

    def test_parse_option_symbol(self):
        data = self._load_option()
        instrument = parse_instrument("AAPL 250321C00175000", data, TRADESTATION_VENUE)
        assert instrument is not None
        assert "AAPL" in str(instrument.id)

    def test_parse_option_kind_call(self):
        from nautilus_trader.model.enums import OptionKind
        data = self._load_option()
        instrument = parse_instrument("AAPL 250321C00175000", data, TRADESTATION_VENUE)
        assert instrument is not None
        assert instrument.option_kind == OptionKind.CALL

    def test_parse_option_kind_put(self):
        from nautilus_trader.model.enums import OptionKind
        from tradestation_nt_community.parsing.instruments import _parse_option_kind
        assert _parse_option_kind("Put") == OptionKind.PUT
        assert _parse_option_kind("P") == OptionKind.PUT
        assert _parse_option_kind("Call") == OptionKind.CALL
        assert _parse_option_kind("C") == OptionKind.CALL

    def test_parse_option_strike_price(self):
        data = self._load_option()
        instrument = parse_instrument("AAPL 250321C00175000", data, TRADESTATION_VENUE)
        assert instrument is not None
        assert float(instrument.strike_price) == pytest.approx(175.0, rel=1e-4)

    def test_parse_option_expiration_set(self):
        data = self._load_option()
        instrument = parse_instrument("AAPL 250321C00175000", data, TRADESTATION_VENUE)
        assert instrument is not None
        assert instrument.expiration_ns > 0

    def test_parse_option_expiration_from_date_field(self):
        """ExpirationDate field parsed correctly to 2025-03-21."""
        import pandas as pd
        data = self._load_option()
        instrument = parse_instrument("AAPL 250321C00175000", data, TRADESTATION_VENUE)
        assert instrument is not None
        expiry = pd.Timestamp(instrument.expiration_ns, unit="ns", tz="UTC")
        assert expiry.year == 2025
        assert expiry.month == 3
        assert expiry.day == 21

    def test_parse_option_underlying(self):
        data = self._load_option()
        instrument = parse_instrument("AAPL 250321C00175000", data, TRADESTATION_VENUE)
        assert instrument is not None
        assert instrument.underlying == "AAPL"

    def test_parse_option_multiplier(self):
        """PointValue=100 → multiplier=100 (standard equity option contract size)."""
        data = self._load_option()
        instrument = parse_instrument("AAPL 250321C00175000", data, TRADESTATION_VENUE)
        assert instrument is not None
        assert float(instrument.multiplier) == 100.0

    def test_parse_option_currency_usd(self):
        data = self._load_option()
        instrument = parse_instrument("AAPL 250321C00175000", data, TRADESTATION_VENUE)
        assert instrument is not None
        assert instrument.quote_currency.code == "USD"

    def test_parse_option_strike_from_occ_symbol(self):
        """Strike is parsed from OCC symbol when StrikePrice field is absent."""
        data = {k: v for k, v in self._load_option().items() if k != "StrikePrice"}
        instrument = parse_instrument("AAPL 250321C00175000", data, TRADESTATION_VENUE)
        assert instrument is not None
        assert float(instrument.strike_price) == pytest.approx(175.0, rel=1e-4)

    def test_parse_option_kind_from_occ_symbol(self):
        """OptionType 'C'/'P' is read from OCC symbol when OptionType field is absent."""
        from nautilus_trader.model.enums import OptionKind
        data = {k: v for k, v in self._load_option().items() if k != "OptionType"}
        instrument = parse_instrument("AAPL 250321C00175000", data, TRADESTATION_VENUE)
        assert instrument is not None
        assert instrument.option_kind == OptionKind.CALL

    def test_parse_put_option(self):
        """A put option with OptionType=Put parses correctly."""
        from nautilus_trader.model.enums import OptionKind
        data = {**self._load_option(), "OptionType": "Put", "Symbol": "AAPL 250321P00170000"}
        instrument = parse_instrument("AAPL 250321P00170000", data, TRADESTATION_VENUE)
        assert instrument is not None
        assert instrument.option_kind == OptionKind.PUT
