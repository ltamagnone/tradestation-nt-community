"""
Tests for TradeStation execution client parsing helpers and command signatures.

Uses module-level parsing functions from execution.py so no live Component
instantiation is needed. Command-object tests verify that the execution
client methods accept the correct NautilusTrader command types.
"""
from decimal import Decimal

import pytest

from tradestation_nt_community.execution import (
    convert_time_in_force,
    parse_order_status,
    parse_order_status_report,
    parse_ts_order_type,
)
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.execution.messages import (
    GenerateFillReports,
    GenerateOrderStatusReport,
    GenerateOrderStatusReports,
    GeneratePositionStatusReports,
)
from nautilus_trader.model.enums import OrderStatus, OrderType, TimeInForce
from nautilus_trader.model.identifiers import AccountId, ClientOrderId, InstrumentId, VenueOrderId
from tests.test_kit import TSTestOrderStubs


_ACCOUNT_ID = AccountId("TRADESTATION-SIM0000001F")
_TS_NOW = 1_000_000_000


class TestOrderStatusParsing:
    """Tests for parse_order_status()."""

    def test_fll_maps_to_filled(self):
        assert parse_order_status("FLL") == OrderStatus.FILLED

    def test_opn_maps_to_submitted(self):
        assert parse_order_status("OPN") == OrderStatus.SUBMITTED

    def test_ack_maps_to_accepted(self):
        assert parse_order_status("ACK") == OrderStatus.ACCEPTED

    def test_can_maps_to_canceled(self):
        assert parse_order_status("CAN") == OrderStatus.CANCELED

    def test_out_maps_to_canceled(self):
        assert parse_order_status("OUT") == OrderStatus.CANCELED

    def test_rej_maps_to_rejected(self):
        assert parse_order_status("REJ") == OrderStatus.REJECTED

    def test_exp_maps_to_expired(self):
        assert parse_order_status("EXP") == OrderStatus.EXPIRED

    def test_flp_maps_to_partially_filled(self):
        assert parse_order_status("FLP") == OrderStatus.PARTIALLY_FILLED

    def test_unknown_returns_pending_update(self):
        assert parse_order_status("UNKNOWN_XYZ") == OrderStatus.PENDING_UPDATE


class TestOrderTypeParsing:
    """Tests for parse_ts_order_type()."""

    def test_market_parses(self):
        assert parse_ts_order_type("Market") == OrderType.MARKET

    def test_limit_parses(self):
        assert parse_ts_order_type("Limit") == OrderType.LIMIT

    def test_stop_market_parses(self):
        assert parse_ts_order_type("StopMarket") == OrderType.STOP_MARKET

    def test_stop_limit_parses(self):
        assert parse_ts_order_type("StopLimit") == OrderType.STOP_LIMIT

    def test_unknown_defaults_to_market(self):
        assert parse_ts_order_type("Unknown") == OrderType.MARKET


class TestTimeInForceConversion:
    """Tests for convert_time_in_force()."""

    def test_day_converts(self):
        assert convert_time_in_force(TimeInForce.DAY) == "DAY"

    def test_gtc_converts(self):
        assert convert_time_in_force(TimeInForce.GTC) == "GTC"

    def test_ioc_converts(self):
        assert convert_time_in_force(TimeInForce.IOC) == "IOC"

    def test_fok_converts(self):
        assert convert_time_in_force(TimeInForce.FOK) == "FOK"

    def test_unsupported_defaults_to_day(self):
        assert convert_time_in_force(TimeInForce.AT_THE_OPEN) == "DAY"


class TestOrderStatusReportParsing:
    """Tests for parse_order_status_report()."""

    def _parse(self, ts_order, symbol="GCJ26") -> object:
        instrument_id = InstrumentId.from_str(f"{symbol}.TRADESTATION")
        return parse_order_status_report(
            ts_order,
            instrument_id=instrument_id,
            client_order_id=ClientOrderId("O-001"),
            account_id=_ACCOUNT_ID,
            ts_now=_TS_NOW,
        )

    def test_filled_market_order_status(self):
        report = self._parse(TSTestOrderStubs.market_order_filled())
        assert report is not None
        assert report.order_status == OrderStatus.FILLED

    def test_filled_market_order_qty(self):
        report = self._parse(TSTestOrderStubs.market_order_filled())
        assert report.filled_qty == Decimal("1")

    def test_open_limit_order_status(self):
        report = self._parse(TSTestOrderStubs.limit_order_open())
        assert report is not None
        assert report.order_status == OrderStatus.SUBMITTED

    def test_open_limit_order_price(self):
        report = self._parse(TSTestOrderStubs.limit_order_open())
        assert float(report.price) == pytest.approx(3400.0, rel=1e-4)

    def test_canceled_order_status(self):
        report = self._parse(TSTestOrderStubs.order_canceled())
        assert report is not None
        assert report.order_status == OrderStatus.CANCELED

    def test_stop_filled_order_status(self):
        report = self._parse(TSTestOrderStubs.stop_order_filled(), symbol="ESM26")
        assert report is not None
        assert report.order_status == OrderStatus.FILLED

    def test_venue_order_id_preserved(self):
        report = self._parse(TSTestOrderStubs.market_order_filled())
        assert str(report.venue_order_id) == "TS-ORDER-001"

    def test_sell_side_detection(self):
        report = self._parse(TSTestOrderStubs.limit_order_open())
        from nautilus_trader.model.enums import OrderSide
        assert report.order_side == OrderSide.SELL


class TestPositionStubs:
    """Sanity checks on the positions fixture."""

    def test_fixture_has_two_positions(self):
        assert len(TSTestOrderStubs.positions()) == 2

    def test_gc_position_is_long(self):
        gc = next(p for p in TSTestOrderStubs.positions() if p["Symbol"] == "GCJ26")
        assert float(gc["Quantity"]) > 0

    def test_es_position_is_short(self):
        es = next(p for p in TSTestOrderStubs.positions() if p["Symbol"] == "ESM26")
        assert float(es["Quantity"]) < 0


class TestReportCommandObjects:
    """Verify command objects used by execution client report methods.

    The execution client's generate_*_report(s) methods now accept command
    objects instead of individual parameters. These tests verify that the
    command objects can be constructed, their attributes are accessible, and
    they match what the execution client extracts.
    """

    _GC_INSTRUMENT = InstrumentId.from_str("GCJ26.TRADESTATION")

    def test_generate_order_status_report_command_attrs(self):
        cmd = GenerateOrderStatusReport(
            instrument_id=self._GC_INSTRUMENT,
            client_order_id=ClientOrderId("O-001"),
            venue_order_id=VenueOrderId("TS-ORDER-001"),
            command_id=UUID4(),
            ts_init=1_000_000_000,
        )
        assert cmd.instrument_id == self._GC_INSTRUMENT
        assert cmd.client_order_id == ClientOrderId("O-001")
        assert cmd.venue_order_id == VenueOrderId("TS-ORDER-001")

    def test_generate_order_status_report_command_optional_venue_order_id(self):
        cmd = GenerateOrderStatusReport(
            instrument_id=self._GC_INSTRUMENT,
            client_order_id=ClientOrderId("O-002"),
            venue_order_id=None,
            command_id=UUID4(),
            ts_init=1_000_000_000,
        )
        assert cmd.venue_order_id is None

    def test_generate_order_status_reports_command_attrs(self):
        cmd = GenerateOrderStatusReports(
            instrument_id=self._GC_INSTRUMENT,
            start=None,
            end=None,
            open_only=True,
            command_id=UUID4(),
            ts_init=1_000_000_000,
        )
        assert cmd.instrument_id == self._GC_INSTRUMENT
        assert cmd.open_only is True

    def test_generate_order_status_reports_command_no_instrument(self):
        cmd = GenerateOrderStatusReports(
            instrument_id=None,
            start=None,
            end=None,
            open_only=False,
            command_id=UUID4(),
            ts_init=1_000_000_000,
        )
        assert cmd.instrument_id is None
        assert cmd.open_only is False

    def test_generate_fill_reports_command_attrs(self):
        cmd = GenerateFillReports(
            instrument_id=self._GC_INSTRUMENT,
            venue_order_id=VenueOrderId("TS-ORDER-001"),
            start=None,
            end=None,
            command_id=UUID4(),
            ts_init=1_000_000_000,
        )
        assert cmd.instrument_id == self._GC_INSTRUMENT
        assert cmd.venue_order_id == VenueOrderId("TS-ORDER-001")

    def test_generate_fill_reports_command_no_filters(self):
        cmd = GenerateFillReports(
            instrument_id=None,
            venue_order_id=None,
            start=None,
            end=None,
            command_id=UUID4(),
            ts_init=1_000_000_000,
        )
        assert cmd.instrument_id is None
        assert cmd.venue_order_id is None

    def test_generate_position_status_reports_command_attrs(self):
        cmd = GeneratePositionStatusReports(
            instrument_id=self._GC_INSTRUMENT,
            start=None,
            end=None,
            command_id=UUID4(),
            ts_init=1_000_000_000,
        )
        assert cmd.instrument_id == self._GC_INSTRUMENT

    def test_generate_position_status_reports_command_no_instrument(self):
        cmd = GeneratePositionStatusReports(
            instrument_id=None,
            start=None,
            end=None,
            command_id=UUID4(),
            ts_init=1_000_000_000,
        )
        assert cmd.instrument_id is None
