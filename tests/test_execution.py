"""
Tests for TradeStation execution client parsing helpers and command signatures.

Uses module-level parsing functions from execution.py so no live Component
instantiation is needed. Command-object tests verify that the execution
client methods accept the correct NautilusTrader command types.
"""
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from tradestation_nt_community.execution import (
    TradeStationExecutionClient,
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
    ModifyOrder,
)
from nautilus_trader.model.enums import OrderStatus, OrderType, TimeInForce
from nautilus_trader.model.identifiers import (
    AccountId, ClientOrderId, InstrumentId, Symbol, StrategyId,
    TraderId, Venue, VenueOrderId,
)
from nautilus_trader.model.objects import Price, Quantity
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

    def test_fok_raises(self):
        """FOK is explicitly rejected — TradeStation always rejects these orders."""
        with pytest.raises(ValueError, match="FOK"):
            convert_time_in_force(TimeInForce.FOK)

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


# ---------------------------------------------------------------------------
# SSE reconnect catch-up poll
# ---------------------------------------------------------------------------

class TestStreamFillEventsReconnectCatchup:
    """
    When the order fill SSE stream reconnects, _stream_order_fills must call
    _check_order_statuses() to recover fills missed during the gap.
    """

    @pytest.mark.asyncio
    async def test_reconnect_sentinel_triggers_catch_up_poll(self):
        """
        _stream_order_fills calls _check_order_statuses() when it receives
        the {"_reconnected": True} sentinel from stream_orders.
        """
        from unittest.mock import AsyncMock, MagicMock, patch

        exec_client = MagicMock()
        poll_calls = []

        async def fake_check_order_statuses():
            poll_calls.append(1)

        async def fake_stream_orders(account_id):
            yield {"OrderID": "A1", "Status": "OPN"}  # normal event
            yield {"_reconnected": True}                # sentinel
            yield {"OrderID": "A1", "Status": "FLL"}   # post-reconnect event

        async def fake_process_event(event):
            pass

        exec_client._account_id = "SIM001"
        exec_client._stream_client = MagicMock()
        exec_client._stream_client.stream_orders = fake_stream_orders
        exec_client._check_order_statuses = fake_check_order_statuses
        exec_client._process_order_event = fake_process_event
        exec_client._log = MagicMock()

        # Run the real _stream_order_fills logic (extracted for test)
        from tradestation_nt_community.execution import TradeStationExecutionClient
        await TradeStationExecutionClient._stream_order_fills(exec_client)

        # The catch-up poll must have been triggered exactly once
        assert len(poll_calls) == 1

    @pytest.mark.asyncio
    async def test_no_catch_up_without_sentinel(self):
        """Normal events (no sentinel) do not trigger the catch-up poll."""
        from unittest.mock import MagicMock

        exec_client = MagicMock()
        poll_calls = []

        async def fake_check_order_statuses():
            poll_calls.append(1)

        async def fake_stream_orders(account_id):
            yield {"OrderID": "B1", "Status": "OPN"}
            yield {"OrderID": "B1", "Status": "FLL"}

        async def fake_process_event(event):
            pass

        exec_client._account_id = "SIM001"
        exec_client._stream_client = MagicMock()
        exec_client._stream_client.stream_orders = fake_stream_orders
        exec_client._check_order_statuses = fake_check_order_statuses
        exec_client._process_order_event = fake_process_event
        exec_client._log = MagicMock()

        from tradestation_nt_community.execution import TradeStationExecutionClient
        await TradeStationExecutionClient._stream_order_fills(exec_client)

        assert len(poll_calls) == 0


# =============================================================================
# _modify_order: 4xx vs 5xx error handling
# =============================================================================

def _make_modify_command(
    client_order_id: str = "O-001",
    ts_order_id: str = "TS-001",
) -> ModifyOrder:
    return ModifyOrder(
        TraderId("TRADER-001"),
        StrategyId("S-001"),
        InstrumentId(Symbol("GCJ26"), Venue("TRADESTATION")),
        ClientOrderId(client_order_id),
        VenueOrderId(ts_order_id),
        Quantity.from_int(1),
        Price(3350.0, 1),
        None,   # trigger_price
        UUID4(),
        0,      # ts_init
    )


def _make_exec_mock(command: ModifyOrder, ts_order_id: str = "TS-001") -> MagicMock:
    """Minimal mock that passes _modify_order's pre-checks and cache lookup."""
    m = MagicMock()
    m._client_order_id_to_ts_order_id = {command.client_order_id: ts_order_id}
    order = MagicMock()
    order.quantity = Quantity.from_int(1)
    m._cache = MagicMock()
    m._cache.order.return_value = order
    m._clock = MagicMock()
    m._clock.timestamp_ns.return_value = 0
    m._log = MagicMock()
    m._account_id = "SIM001"
    return m


class TestModifyOrderErrorHandling:
    """_modify_order emits generate_order_modify_rejected on 4xx only."""

    @pytest.mark.asyncio
    async def test_400_emits_modify_rejected(self):
        """HTTP 400 from broker triggers generate_order_modify_rejected."""
        cmd = _make_modify_command()
        m = _make_exec_mock(cmd)
        m._client.replace_order = AsyncMock(
            side_effect=Exception("Replace order failed (HTTP 400): Invalid Parameter")
        )
        await TradeStationExecutionClient._modify_order(m, cmd)
        m.generate_order_modify_rejected.assert_called_once()
        _, kwargs = m.generate_order_modify_rejected.call_args
        assert kwargs["client_order_id"] == cmd.client_order_id
        assert "400" in kwargs["reason"]

    @pytest.mark.asyncio
    async def test_422_emits_modify_rejected(self):
        """HTTP 422 (order not modifiable) also triggers the rejection event."""
        cmd = _make_modify_command()
        m = _make_exec_mock(cmd)
        m._client.replace_order = AsyncMock(
            side_effect=Exception("Replace order failed (HTTP 422): Order not modifiable")
        )
        await TradeStationExecutionClient._modify_order(m, cmd)
        m.generate_order_modify_rejected.assert_called_once()

    @pytest.mark.asyncio
    async def test_5xx_does_not_emit_modify_rejected(self):
        """HTTP 5xx is ambiguous — modify may have succeeded, so no rejection event."""
        cmd = _make_modify_command()
        m = _make_exec_mock(cmd)
        m._client.replace_order = AsyncMock(
            side_effect=Exception("Replace order failed (HTTP 503): Service Unavailable")
        )
        await TradeStationExecutionClient._modify_order(m, cmd)
        m.generate_order_modify_rejected.assert_not_called()

    @pytest.mark.asyncio
    async def test_network_error_does_not_emit_modify_rejected(self):
        """A generic network error is also ambiguous — no rejection event."""
        cmd = _make_modify_command()
        m = _make_exec_mock(cmd)
        m._client.replace_order = AsyncMock(
            side_effect=Exception("Connection timeout")
        )
        await TradeStationExecutionClient._modify_order(m, cmd)
        m.generate_order_modify_rejected.assert_not_called()

    @pytest.mark.asyncio
    async def test_success_emits_order_updated_not_rejected(self):
        """On success generate_order_updated is called and rejected is not."""
        cmd = _make_modify_command()
        m = _make_exec_mock(cmd)
        m._client.replace_order = AsyncMock(return_value={"OrderID": "TS-001"})
        m._ts_order_id_to_client_order_id = {}
        await TradeStationExecutionClient._modify_order(m, cmd)
        m.generate_order_updated.assert_called_once()
        m.generate_order_modify_rejected.assert_not_called()


# =============================================================================
# _update_account_state: retry and graceful degradation
# =============================================================================

def _make_account_state_mock() -> MagicMock:
    m = MagicMock()
    m._account_id = "SIM001"
    m._account_id_nautilus = AccountId("TRADESTATION-SIM001")
    m.account_id = AccountId("TRADESTATION-SIM001")
    m._clock = MagicMock()
    m._clock.timestamp_ns.return_value = 0
    m._log = MagicMock()
    return m


_BALANCES_OK = {"CashBalance": "50000", "Equity": "55000", "MarketValue": "5000"}


class TestUpdateAccountState:
    """_update_account_state retries on failure and never raises."""

    @pytest.mark.asyncio
    async def test_success_on_first_attempt(self, monkeypatch):
        """Happy path: account state fetched and generate_account_state called."""
        async def mock_sleep(s): pass
        monkeypatch.setattr("asyncio.sleep", mock_sleep)

        m = _make_account_state_mock()
        m._client.get_balances = AsyncMock(return_value=_BALANCES_OK)
        await TradeStationExecutionClient._update_account_state(m)
        m.generate_account_state.assert_called_once()

    @pytest.mark.asyncio
    async def test_retries_on_transient_error_then_succeeds(self, monkeypatch):
        """First attempt fails, second succeeds — generate_account_state called once."""
        sleep_calls = []
        async def mock_sleep(s): sleep_calls.append(s)
        monkeypatch.setattr("asyncio.sleep", mock_sleep)

        m = _make_account_state_mock()
        m._client.get_balances = AsyncMock(
            side_effect=[Exception("HTTP 500"), _BALANCES_OK]
        )
        await TradeStationExecutionClient._update_account_state(m)
        m.generate_account_state.assert_called_once()
        assert len(sleep_calls) == 1

    @pytest.mark.asyncio
    async def test_all_retries_exhausted_does_not_raise(self, monkeypatch):
        """Three consecutive failures log a warning but do not raise."""
        async def mock_sleep(s): pass
        monkeypatch.setattr("asyncio.sleep", mock_sleep)

        m = _make_account_state_mock()
        m._client.get_balances = AsyncMock(side_effect=Exception("HTTP 503"))
        await TradeStationExecutionClient._update_account_state(m)  # must not raise
        m.generate_account_state.assert_not_called()
        assert m._log.warning.call_count >= 1

    @pytest.mark.asyncio
    async def test_exhausted_retries_logs_trading_continues_message(self, monkeypatch):
        """Final warning message tells operator trading continues."""
        async def mock_sleep(s): pass
        monkeypatch.setattr("asyncio.sleep", mock_sleep)

        m = _make_account_state_mock()
        m._client.get_balances = AsyncMock(side_effect=Exception("HTTP 503"))
        await TradeStationExecutionClient._update_account_state(m)

        warning_messages = " ".join(
            str(call) for call in m._log.warning.call_args_list
        )
        assert "Trading will continue" in warning_messages


# =============================================================================
# _cancel_all_orders: instrument/strategy-scoped cancellation
# =============================================================================

from nautilus_trader.execution.messages import CancelAllOrders
from nautilus_trader.model.enums import OrderSide


def _make_cancel_all_command(
    instrument_str: str = "GCJ26.TRADESTATION",
    strategy_str: str = "S-001",
    order_side: OrderSide = OrderSide.NO_ORDER_SIDE,
) -> CancelAllOrders:
    return CancelAllOrders(
        trader_id=TraderId("TRADER-001"),
        strategy_id=StrategyId(strategy_str),
        instrument_id=InstrumentId.from_str(instrument_str),
        order_side=order_side,
        command_id=UUID4(),
        ts_init=0,
    )


def _make_mock_order(
    client_order_id: str,
    venue_order_id: str,
    side: OrderSide = OrderSide.BUY,
    status: OrderStatus = OrderStatus.ACCEPTED,
):
    order = MagicMock()
    order.client_order_id = ClientOrderId(client_order_id)
    order.venue_order_id = VenueOrderId(venue_order_id)
    order.side = side
    order.status = status
    return order


def _make_cancel_all_exec_mock(
    open_orders=None,
    inflight_orders=None,
):
    m = MagicMock()
    m._cache = MagicMock()
    m._cache.orders_open.return_value = open_orders or []
    m._cache.orders_inflight.return_value = inflight_orders or []
    m._client = MagicMock()
    m._client.cancel_order = AsyncMock()
    m._clock = MagicMock()
    m._clock.timestamp_ns.return_value = 0
    m._log = MagicMock()
    m.generate_order_canceled = MagicMock()
    return m


class TestCancelAllOrdersFiltering:
    """_cancel_all_orders must only cancel orders for the specified
    instrument and strategy, never touching other strategies' orders."""

    @pytest.mark.asyncio
    async def test_cancels_only_matching_orders(self):
        """Only orders returned by cache for this instrument+strategy get canceled."""
        gc_order = _make_mock_order("O-GC-1", "TS-100", OrderSide.SELL)
        m = _make_cancel_all_exec_mock(open_orders=[gc_order])

        cmd = _make_cancel_all_command("GCJ26.TRADESTATION", "S-001")
        await TradeStationExecutionClient._cancel_all_orders(m, cmd)

        m._cache.orders_open.assert_called_once_with(
            instrument_id=cmd.instrument_id,
            strategy_id=cmd.strategy_id,
            side=cmd.order_side,
        )
        m._client.cancel_order.assert_called_once_with(order_id="TS-100")
        m.generate_order_canceled.assert_called_once()

    @pytest.mark.asyncio
    async def test_no_orders_skips_cancel(self):
        """When cache returns no orders, no TS API calls are made."""
        m = _make_cancel_all_exec_mock(open_orders=[])

        cmd = _make_cancel_all_command("NQU26.TRADESTATION", "S-002")
        await TradeStationExecutionClient._cancel_all_orders(m, cmd)

        m._client.cancel_order.assert_not_called()
        m.generate_order_canceled.assert_not_called()

    @pytest.mark.asyncio
    async def test_multiple_orders_all_canceled(self):
        """All orders for the instrument+strategy are canceled."""
        stop = _make_mock_order("O-1", "TS-200", OrderSide.SELL)
        target = _make_mock_order("O-2", "TS-201", OrderSide.SELL)
        m = _make_cancel_all_exec_mock(open_orders=[stop, target])

        cmd = _make_cancel_all_command()
        await TradeStationExecutionClient._cancel_all_orders(m, cmd)

        assert m._client.cancel_order.call_count == 2
        assert m.generate_order_canceled.call_count == 2

    @pytest.mark.asyncio
    async def test_inflight_submitted_orders_included(self):
        """Inflight orders with SUBMITTED status are also canceled."""
        inflight = _make_mock_order(
            "O-INF", "TS-300", OrderSide.BUY, OrderStatus.SUBMITTED,
        )
        m = _make_cancel_all_exec_mock(inflight_orders=[inflight])

        cmd = _make_cancel_all_command()
        await TradeStationExecutionClient._cancel_all_orders(m, cmd)

        m._client.cancel_order.assert_called_once_with(order_id="TS-300")

    @pytest.mark.asyncio
    async def test_not_an_open_order_is_warning_not_error(self):
        """Broker 'Not an open order' is logged as warning, not error."""
        order = _make_mock_order("O-1", "TS-400")
        m = _make_cancel_all_exec_mock(open_orders=[order])
        m._client.cancel_order = AsyncMock(
            side_effect=Exception("Not an open order"),
        )

        cmd = _make_cancel_all_command()
        await TradeStationExecutionClient._cancel_all_orders(m, cmd)

        m._log.warning.assert_called()
        m.generate_order_canceled.assert_not_called()

    @pytest.mark.asyncio
    async def test_order_without_venue_id_skipped(self):
        """Orders with no venue_order_id (not yet acknowledged) are skipped."""
        order = _make_mock_order("O-1", "TS-500")
        order.venue_order_id = None
        m = _make_cancel_all_exec_mock(open_orders=[order])

        cmd = _make_cancel_all_command()
        await TradeStationExecutionClient._cancel_all_orders(m, cmd)

        m._client.cancel_order.assert_not_called()
        m._log.warning.assert_called()

    @pytest.mark.asyncio
    async def test_side_filter_applied(self):
        """When command specifies a side, only that side is canceled."""
        buy = _make_mock_order("O-BUY", "TS-600", OrderSide.BUY)
        sell = _make_mock_order("O-SELL", "TS-601", OrderSide.SELL)
        m = _make_cancel_all_exec_mock(open_orders=[buy, sell])

        cmd = _make_cancel_all_command(order_side=OrderSide.SELL)
        await TradeStationExecutionClient._cancel_all_orders(m, cmd)

        m._client.cancel_order.assert_called_once_with(order_id="TS-601")
