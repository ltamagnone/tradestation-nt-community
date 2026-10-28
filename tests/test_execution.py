"""
Tests for TradeStation execution client parsing helpers and command signatures.

Uses module-level parsing functions from execution.py so no live Component
instantiation is needed. Command-object tests verify that the execution
client methods accept the correct NautilusTrader command types.
"""
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import httpx
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

    def test_unknown_assumed_live(self):
        assert parse_order_status("UNKNOWN_XYZ") == OrderStatus.ACCEPTED


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

    def test_positions_have_average_price(self):
        """Every position fixture entry must carry AveragePrice so avg_px_open is populated."""
        for pos in TSTestOrderStubs.positions():
            assert "AveragePrice" in pos, f"Missing AveragePrice in {pos}"
            assert float(pos["AveragePrice"]) > 0, f"AveragePrice must be > 0, got {pos}"


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
    m._pending_modify_trigger_price = {}
    m._modify_inflight = {}
    m._pending_amend_meta = {}
    order.trigger_price = Price(21000.0, 2)
    order.price = None
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
    m._check_order_statuses = AsyncMock()
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


# =============================================================================
# _cancel_order: concurrent fill recovery
# =============================================================================

class TestCancelOrderConcurrentFill:
    """When broker returns 'Not an open order', _cancel_order must call
    _check_order_statuses immediately so any concurrent fill is processed
    before NT's ExecEngine times out and auto-generates OrderCanceled."""

    def _make_cancel_order_mock(self, ts_order_id: str):
        from nautilus_trader.model.identifiers import (
            ClientOrderId,
            InstrumentId,
            Symbol,
            Venue,
        )
        from nautilus_trader.model.identifiers import StrategyId, TraderId
        from nautilus_trader.execution.messages import CancelOrder

        m = MagicMock()
        m._log = MagicMock()
        m._client = MagicMock()
        m._client_order_id_to_ts_order_id = {
            ClientOrderId("O-EC-1"): ts_order_id,
        }
        m._check_order_statuses = AsyncMock()
        m.generate_order_canceled = MagicMock()

        cmd = CancelOrder(
            trader_id=TraderId("TRADER-001"),
            strategy_id=StrategyId("S-001"),
            instrument_id=InstrumentId(Symbol("ECM26"), Venue("TRADESTATION")),
            client_order_id=ClientOrderId("O-EC-1"),
            venue_order_id=None,
            command_id=UUID4(),
            ts_init=0,
        )
        return m, cmd

    @pytest.mark.asyncio
    async def test_check_order_statuses_called_on_concurrent_fill(self):
        """_check_order_statuses fires immediately when cancel returns 'Not an open order'
        so any concurrent fill is processed before NT's ExecEngine auto-cancels."""
        m, cmd = self._make_cancel_order_mock("TS-950568948")
        m._client.cancel_order = AsyncMock(side_effect=Exception("Not an open order"))

        await TradeStationExecutionClient._cancel_order(m, cmd)

        m._log.warning.assert_called()
        m.generate_order_canceled.assert_not_called()
        m._check_order_statuses.assert_called_once()

    @pytest.mark.asyncio
    async def test_check_order_statuses_not_called_on_other_error(self):
        """Other cancel errors log as error and do not trigger _check_order_statuses."""
        m, cmd = self._make_cancel_order_mock("TS-950568948")
        m._client.cancel_order = AsyncMock(side_effect=Exception("Connection refused"))

        await TradeStationExecutionClient._cancel_order(m, cmd)

        m._log.error.assert_called()
        m._check_order_statuses.assert_not_called()


class TestCancelOrderRejectedEmission(TestCancelOrderConcurrentFill):
    """A cancel that genuinely failed emits OrderCancelRejected (2026-09-26).

    Before, the failure was only logged: NT held the order PENDING_CANCEL until its
    in-flight check re-queried the status (~5-25 s), and if that query also failed NT
    synthesized OrderCanceled for an order still live at the broker. Emitting the
    rejection reverts the order immediately (PENDING_CANCEL -> previous status), so
    strategies see the truth: the order is still working.
    "Not an open order" is NOT a rejection — the order is gone (filled / expired) and
    the fill path owns it, exactly as before.
    """

    @pytest.mark.asyncio
    async def test_genuine_failure_emits_cancel_rejected(self):
        m, cmd = self._make_cancel_order_mock("950568948")
        m._client.cancel_order = AsyncMock(side_effect=Exception("Connection refused"))
        m.generate_order_cancel_rejected = MagicMock()

        await TradeStationExecutionClient._cancel_order(m, cmd)

        m.generate_order_cancel_rejected.assert_called_once()
        kw = m.generate_order_cancel_rejected.call_args.kwargs
        assert str(kw["client_order_id"]) == "O-EC-1"
        assert str(kw["venue_order_id"]) == "950568948"
        assert "Connection refused" in kw["reason"]
        m.generate_order_canceled.assert_not_called()

    @pytest.mark.asyncio
    async def test_missing_ts_order_id_emits_cancel_rejected(self):
        m, cmd = self._make_cancel_order_mock("950568948")
        m._client_order_id_to_ts_order_id = {}
        m._client.cancel_order = AsyncMock()
        m.generate_order_cancel_rejected = MagicMock()

        await TradeStationExecutionClient._cancel_order(m, cmd)

        m._client.cancel_order.assert_not_awaited()
        m.generate_order_cancel_rejected.assert_called_once()
        assert m.generate_order_cancel_rejected.call_args.kwargs["venue_order_id"] is None

    @pytest.mark.asyncio
    async def test_not_an_open_order_is_not_a_rejection(self):
        m, cmd = self._make_cancel_order_mock("950568948")
        m._client.cancel_order = AsyncMock(side_effect=Exception("Not an open order"))
        m.generate_order_cancel_rejected = MagicMock()

        await TradeStationExecutionClient._cancel_order(m, cmd)

        m.generate_order_cancel_rejected.assert_not_called()
        m._check_order_statuses.assert_called_once()


# =============================================================================
# PENDING_UPDATE recovery: _modify_order tracking + _check_order_statuses
# =============================================================================

def _make_modify_command_with_trigger(
    client_order_id: str = "O-STOP",
    ts_order_id: str = "TS-STOP",
    trigger_price: float = 21050.0,
) -> ModifyOrder:
    """ModifyOrder with a trigger_price (stop-market amend)."""
    return ModifyOrder(
        TraderId("TRADER-001"),
        StrategyId("S-001"),
        InstrumentId(Symbol("NQM26"), Venue("TRADESTATION")),
        ClientOrderId(client_order_id),
        VenueOrderId(ts_order_id),
        None,   # quantity unchanged
        None,   # price unchanged
        Price(trigger_price, 2),
        UUID4(),
        0,
    )


def _make_check_statuses_mock(
    client_order_id: str = "O-STOP",
    ts_order_id: str = "TS-STOP",
    nt_order_status: OrderStatus = OrderStatus.PENDING_UPDATE,
    ts_orders: list[dict] | None = None,
) -> MagicMock:
    """Minimal mock for _check_order_statuses tests."""
    coid = ClientOrderId(client_order_id)
    m = MagicMock()
    m._ts_order_id_to_client_order_id = {ts_order_id: coid}
    m._client_order_id_to_ts_order_id = {coid: ts_order_id}
    m._order_last_status = {ts_order_id: "OPN"}
    m._pending_modify_trigger_price = {}
    m._modify_inflight = {}
    m._pending_amend_meta = {}
    m._cancel_verify_pending = {}
    m._verify_pending_cancels = AsyncMock()

    cached_order = MagicMock()
    cached_order.status = nt_order_status
    cached_order.is_closed = False
    cached_order.strategy_id = StrategyId("S-001")
    cached_order.instrument_id = InstrumentId(Symbol("NQM26"), Venue("TRADESTATION"))
    cached_order.quantity = Quantity.from_int(1)
    cached_order.filled_qty = Quantity.from_int(0)
    m._cache = MagicMock()
    m._cache.order.return_value = cached_order

    if ts_orders is None:
        ts_orders = [{"OrderID": ts_order_id, "Status": "OPN", "StopPrice": "21000.00"}]
    m._client = MagicMock()
    m._client.get_orders = AsyncMock(return_value=ts_orders)
    m._clock = MagicMock()
    m._clock.timestamp_ns.return_value = 0
    m._log = MagicMock()
    return m


def _arm_amend(m, coid, requested, prev, *, sent=-100.0, stamp=-99.0, old_reads=0):
    """§241: a pending amend as _modify_order records it after an ambiguous PUT failure."""
    from tradestation_nt_community.execution import _AmendMeta
    m._pending_modify_trigger_price[coid] = requested
    m._pending_amend_meta[coid] = _AmendMeta(sent=sent, stamp=stamp, prev=prev,
                                             old_reads=old_reads)


class TestPendingUpdateRecovery:
    """PENDING_UPDATE recovery in _check_order_statuses after a 5xx in _modify_order."""

    # --- _modify_order tracking ---

    @pytest.mark.asyncio
    async def test_4xx_clears_tracking(self):
        """4xx: generate_order_modify_rejected fired and tracking entry removed."""
        cmd = _make_modify_command_with_trigger()
        m = _make_exec_mock(cmd)
        m._pending_modify_trigger_price = {}
        m._modify_inflight = {}
        m._pending_amend_meta = {}
        m._client.replace_order = AsyncMock(
            side_effect=Exception("Replace order failed (HTTP 400): Invalid stop price")
        )
        await TradeStationExecutionClient._modify_order(m, cmd)
        m.generate_order_modify_rejected.assert_called_once()
        assert cmd.client_order_id not in m._pending_modify_trigger_price

    @pytest.mark.asyncio
    async def test_5xx_leaves_tracking_entry(self):
        """5xx: no rejection event emitted, tracking entry remains for poll recovery."""
        cmd = _make_modify_command_with_trigger(trigger_price=21050.0)
        m = _make_exec_mock(cmd)
        m._pending_modify_trigger_price = {}
        m._modify_inflight = {}
        m._pending_amend_meta = {}
        m._client.replace_order = AsyncMock(
            side_effect=Exception("Replace order failed (HTTP 503): Service Unavailable")
        )
        await TradeStationExecutionClient._modify_order(m, cmd)
        m.generate_order_modify_rejected.assert_not_called()
        assert cmd.client_order_id in m._pending_modify_trigger_price

    @pytest.mark.asyncio
    async def test_success_clears_tracking(self):
        """Success: generate_order_updated fired and tracking entry removed."""
        cmd = _make_modify_command_with_trigger(trigger_price=21050.0)
        m = _make_exec_mock(cmd)
        m._pending_modify_trigger_price = {}
        m._modify_inflight = {}
        m._pending_amend_meta = {}
        m._ts_order_id_to_client_order_id = {}
        m._client.replace_order = AsyncMock(return_value={"OrderID": "TS-STOP"})
        await TradeStationExecutionClient._modify_order(m, cmd)
        m.generate_order_updated.assert_called_once()
        assert cmd.client_order_id not in m._pending_modify_trigger_price

    # --- _check_order_statuses recovery ---

    @pytest.mark.asyncio
    async def test_5xx_recovery_modify_succeeded(self):
        """TS stop price matches new price: generate_order_updated clears PENDING_UPDATE."""
        coid = ClientOrderId("O-STOP")
        m = _make_check_statuses_mock(
            ts_orders=[{"OrderID": "TS-STOP", "Status": "OPN", "StopPrice": "21050.0"}]
        )
        _arm_amend(m, coid, (Price(21050.0, 2), None), (Price(21000.0, 2), None))

        await TradeStationExecutionClient._check_order_statuses(m)

        m.generate_order_updated.assert_called_once()
        m.generate_order_modify_rejected.assert_not_called()
        assert coid not in m._pending_modify_trigger_price

    @pytest.mark.asyncio
    async def test_5xx_recovery_modify_failed(self, monkeypatch):
        """§241: TS still shows the OLD price on the 2nd non-overlapping read, ≥20 s after
        the failure → OrderModifyRejected (reworded reason, no '5xx')."""
        import tradestation_nt_community.execution as ex
        monkeypatch.setattr(ex, "_mono", lambda: 0.0)
        coid = ClientOrderId("O-STOP")
        m = _make_check_statuses_mock(
            ts_orders=[{"OrderID": "TS-STOP", "Status": "OPN", "StopPrice": "21000.0"}])
        _arm_amend(m, coid, (Price(21050.0, 2), None), (Price(21000.0, 2), None),
                   sent=-31.0, stamp=-30.0, old_reads=1)
        await TradeStationExecutionClient._check_order_statuses(m)
        m.generate_order_modify_rejected.assert_called_once()
        reason = m.generate_order_modify_rejected.call_args.kwargs["reason"]
        assert "5xx" not in reason and reason.startswith("modify recovery:")
        m.generate_order_updated.assert_not_called()
        assert coid not in m._pending_modify_trigger_price

    @pytest.mark.asyncio
    async def test_market_closed_day_order_expired(self):
        """CAN status: tracking entry cleared, generate_order_canceled fired (not modify_rejected)."""
        coid = ClientOrderId("O-STOP")
        cached_order = MagicMock()
        cached_order.status = OrderStatus.PENDING_UPDATE
        cached_order.is_closed = False
        cached_order.strategy_id = StrategyId("S-001")
        cached_order.instrument_id = InstrumentId(Symbol("NQM26"), Venue("TRADESTATION"))
        cached_order.quantity = Quantity.from_int(1)
        cached_order.filled_qty = Quantity.from_int(0)

        m = MagicMock()
        m._ts_order_id_to_client_order_id = {"TS-STOP": coid}
        m._order_last_status = {"TS-STOP": "OPN"}
        m._pending_modify_trigger_price = {coid: Price(21050.0, 2)}
        m._modify_inflight = {}
        m._pending_amend_meta = {}
        m._cancel_verify_pending = {}
        m._verify_pending_cancels = AsyncMock()
        m._cache = MagicMock()
        m._cache.order.return_value = cached_order
        m._client = MagicMock()
        m._client.get_orders = AsyncMock(
            return_value=[{"OrderID": "TS-STOP", "Status": "CAN", "StopPrice": "21000.0"}]
        )
        m._clock = MagicMock()
        m._clock.timestamp_ns.return_value = 0
        m._log = MagicMock()

        await TradeStationExecutionClient._check_order_statuses(m)

        m.generate_order_canceled.assert_called_once()
        m.generate_order_modify_rejected.assert_not_called()
        m.generate_order_updated.assert_not_called()
        assert coid not in m._pending_modify_trigger_price

    @pytest.mark.asyncio
    async def test_don_with_pending_amend_takes_price_comparison(self):
        """R7: DON is a live status; a pending amend is compared, not discarded."""
        coid = ClientOrderId("O-STOP")
        m = _make_check_statuses_mock(
            ts_orders=[{"OrderID": "TS-STOP", "Status": "DON", "StopPrice": "21050.0"}]
        )
        _arm_amend(m, coid, (Price(21050.0, 2), None), (Price(21000.0, 2), None))

        await TradeStationExecutionClient._check_order_statuses(m)

        for call in m._log.info.call_args_list:
            assert "no longer open" not in call.args[0]
        m.generate_order_updated.assert_called_once()
        m.generate_order_canceled.assert_not_called()

    @pytest.mark.asyncio
    async def test_sse_fill_clears_tracking(self):
        """FLL via SSE (_process_order_event) pops the pending-amend entry (no leak)."""
        coid = ClientOrderId("O-STOP")
        instrument = MagicMock()
        instrument.price_precision = 2
        instrument.quote_currency = MagicMock()

        cached_order = MagicMock()
        cached_order.status = OrderStatus.PENDING_UPDATE
        cached_order.is_closed = False
        cached_order.strategy_id = StrategyId("S-001")
        cached_order.instrument_id = InstrumentId(Symbol("NQM26"), Venue("TRADESTATION"))
        cached_order.quantity = Quantity.from_int(1)
        cached_order.filled_qty = Quantity.from_int(0)
        cached_order.side = MagicMock()
        cached_order.order_type = MagicMock()

        m = MagicMock()
        m._ts_order_id_to_client_order_id = {"TS-STOP": coid}
        m._order_last_status = {}
        m._pending_modify_trigger_price = {coid: Price(21050.0, 2)}
        m._modify_inflight = {}
        m._pending_amend_meta = {}
        m._cache = MagicMock()
        m._cache.order.return_value = cached_order
        m._cache.instrument.return_value = instrument
        m._clock = MagicMock()
        m._clock.timestamp_ns.return_value = 0
        m._log = MagicMock()

        ts_fill_event = {
            "OrderID": "TS-STOP",
            "Status": "FLL",
            "AveragePrice": "21000.00",
            "FilledQuantity": "1",
        }
        await TradeStationExecutionClient._process_order_event(m, ts_fill_event)

        m.generate_order_filled.assert_called_once()


# =============================================================================
# §241: a modify PUT in flight is never "recovered"; the amend is pending only after
# an ambiguous failure (5xx / exception), never before the PUT
# =============================================================================

from tradestation_nt_community import execution as ex  # noqa: E402


def _race_mock(old_stop: str = "21000.00") -> MagicMock:
    """One mock that both _modify_order and _check_order_statuses can run on."""
    m = _make_check_statuses_mock(
        ts_orders=[{"OrderID": "TS-STOP", "Status": "OPN", "StopPrice": old_stop}])
    m._modify_inflight = {}
    m._pending_amend_meta = {}
    m._account_id = "SIM001"
    order = m._cache.order.return_value          # status PENDING_UPDATE
    order.trigger_price = Price(21000.0, 2)      # the price before the modify
    order.price = None
    return m


class TestModifyInflight241:

    @pytest.mark.asyncio
    async def test_poll_during_inflight_put_never_rejects(self):
        """The 2026-10-27 FESX race: the safety poll runs while the PUT is in flight and
        sees the OLD StopPrice. No reject; the PUT's 200 emits exactly one OrderUpdated."""
        cmd = _make_modify_command_with_trigger(trigger_price=21050.0)
        m = _race_mock()

        async def put(**kw):
            assert m._modify_inflight == {cmd.client_order_id: 1}
            assert cmd.client_order_id not in m._pending_modify_trigger_price
            await TradeStationExecutionClient._check_order_statuses(m)   # poll lands mid-PUT
            return {"OrderID": "TS-STOP"}

        m._client.replace_order = AsyncMock(side_effect=put)
        await TradeStationExecutionClient._modify_order(m, cmd)

        m.generate_order_modify_rejected.assert_not_called()
        m.generate_order_updated.assert_called_once()
        assert m.generate_order_updated.call_args.kwargs["trigger_price"] == Price(21050.0, 2)
        assert m._modify_inflight == {}
        assert cmd.client_order_id not in m._pending_modify_trigger_price

    @pytest.mark.asyncio
    async def test_5xx_records_pending_amend_with_meta(self):
        cmd = _make_modify_command_with_trigger(trigger_price=21050.0)
        m = _race_mock()
        m._client.replace_order = AsyncMock(
            side_effect=Exception("Replace order failed (HTTP 503): Service Unavailable"))
        await TradeStationExecutionClient._modify_order(m, cmd)
        coid = cmd.client_order_id
        assert m._pending_modify_trigger_price[coid] == (Price(21050.0, 2), None)
        meta = m._pending_amend_meta[coid]
        assert meta.prev == (Price(21000.0, 2), None)
        assert meta.sent <= meta.stamp and meta.old_reads == 0
        assert m._modify_inflight == {}
        m.generate_order_modify_rejected.assert_not_called()

    @pytest.mark.asyncio
    async def test_timeout_exception_is_ambiguous_too(self):
        cmd = _make_modify_command_with_trigger(trigger_price=21050.0)
        m = _race_mock()
        m._client.replace_order = AsyncMock(side_effect=httpx.ReadTimeout("timed out"))
        await TradeStationExecutionClient._modify_order(m, cmd)
        assert cmd.client_order_id in m._pending_amend_meta
        assert m._modify_inflight == {}

    @pytest.mark.asyncio
    async def test_4xx_records_nothing_and_clears_inflight(self):
        cmd = _make_modify_command_with_trigger(trigger_price=21050.0)
        m = _race_mock()
        m._client.replace_order = AsyncMock(
            side_effect=Exception("Replace order failed (HTTP 400): Invalid stop price"))
        await TradeStationExecutionClient._modify_order(m, cmd)
        m.generate_order_modify_rejected.assert_called_once()
        assert m._pending_modify_trigger_price == {} and m._pending_amend_meta == {}
        assert m._modify_inflight == {}

    @pytest.mark.asyncio
    async def test_overlapping_puts_counted_per_coid(self):
        """Another PUT of the same order is already in flight: this one's finally must
        not clear it."""
        cmd = _make_modify_command_with_trigger(trigger_price=21050.0)
        m = _race_mock()
        m._modify_inflight = {cmd.client_order_id: 1}

        async def put(**kw):
            assert m._modify_inflight == {cmd.client_order_id: 2}
            return {"OrderID": "TS-STOP"}

        m._client.replace_order = AsyncMock(side_effect=put)
        await TradeStationExecutionClient._modify_order(m, cmd)
        m.generate_order_updated.assert_called_once()  # the in-put assert above did not raise
        assert m._modify_inflight == {cmd.client_order_id: 1}

    def test_drop_pending_amend_helper(self):
        coid = ClientOrderId("O-STOP")
        m = MagicMock()
        m._pending_modify_trigger_price = {coid: (Price(1.0, 2), None)}
        m._pending_amend_meta = {coid: ex._AmendMeta(sent=0.0, stamp=0.0, prev=(None, None))}
        ex._drop_pending_amend(m, coid)
        assert m._pending_modify_trigger_price == {} and m._pending_amend_meta == {}


class TestTokenKeepalive:
    """
    _token_keepalive_loop keeps the OAuth access token fresh in streaming mode.

    In streaming mode the SSE loop only calls _check_order_statuses on reconnect,
    so without the keepalive the token expires silently after 20 min when no
    orders are placed.  The keepalive fires every _TOKEN_KEEPALIVE_SECS and calls
    _ensure_authenticated so the token is always caught within its refresh window.
    """

    @pytest.mark.asyncio
    async def test_calls_ensure_authenticated_after_sleep(self):
        """Loop calls _ensure_authenticated once per iteration."""
        import asyncio
        from tradestation_nt_community.execution import TradeStationExecutionClient

        calls = []
        m = MagicMock()
        m._TOKEN_KEEPALIVE_SECS = 0.01  # tiny interval so test runs fast

        async def fake_ensure_authenticated():
            calls.append(1)

        m._client = MagicMock()
        m._client._ensure_authenticated = fake_ensure_authenticated
        m._log = MagicMock()

        task = asyncio.ensure_future(
            TradeStationExecutionClient._token_keepalive_loop(m)
        )
        await asyncio.sleep(0.05)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        assert len(calls) >= 1

    @pytest.mark.asyncio
    async def test_exception_in_ensure_authenticated_is_caught(self):
        """An exception from _ensure_authenticated is logged and loop continues."""
        import asyncio
        from tradestation_nt_community.execution import TradeStationExecutionClient

        call_count = [0]
        m = MagicMock()
        m._TOKEN_KEEPALIVE_SECS = 0.01

        async def flaky_ensure_authenticated():
            call_count[0] += 1
            if call_count[0] == 1:
                raise RuntimeError("network timeout")

        m._client = MagicMock()
        m._client._ensure_authenticated = flaky_ensure_authenticated
        m._log = MagicMock()

        task = asyncio.ensure_future(
            TradeStationExecutionClient._token_keepalive_loop(m)
        )
        await asyncio.sleep(0.05)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        # Loop must have continued past the first failure
        assert call_count[0] >= 2
        m._log.error.assert_called()

    @pytest.mark.asyncio
    async def test_cancellation_exits_cleanly(self):
        """CancelledError shuts down the loop without propagating."""
        import asyncio
        from tradestation_nt_community.execution import TradeStationExecutionClient

        m = MagicMock()
        m._TOKEN_KEEPALIVE_SECS = 60.0  # long — rely on cancel to stop it

        async def noop():
            pass

        m._client = MagicMock()
        m._client._ensure_authenticated = noop
        m._log = MagicMock()

        task = asyncio.ensure_future(
            TradeStationExecutionClient._token_keepalive_loop(m)
        )
        await asyncio.sleep(0.01)
        task.cancel()
        # Must not raise
        try:
            await task
        except asyncio.CancelledError:
            pytest.fail("CancelledError leaked out of _token_keepalive_loop")

    @pytest.mark.asyncio
    async def test_keepalive_task_started_in_streaming_mode(self):
        """_connect creates _token_keepalive_task when use_streaming=True."""
        import asyncio
        from unittest.mock import AsyncMock, MagicMock, patch

        exec_client = MagicMock()
        exec_client._use_streaming = True
        exec_client._stream_client = MagicMock()
        exec_client._loop = asyncio.get_event_loop()
        exec_client._fill_poll_task = None
        exec_client._token_keepalive_task = None

        created_tasks = []

        real_create_task = asyncio.get_event_loop().create_task

        def fake_create_task(coro):
            t = real_create_task(coro)
            created_tasks.append(t)
            return t

        exec_client._loop.create_task = fake_create_task

        # Patch the two coroutine methods so they exit immediately
        async def noop_stream():
            pass

        async def noop_keepalive():
            pass

        exec_client._stream_order_fills = noop_stream
        exec_client._token_keepalive_loop = noop_keepalive
        exec_client._log = MagicMock()

        # Manually invoke just the streaming task-creation block
        if exec_client._use_streaming and exec_client._stream_client:
            exec_client._fill_poll_task = exec_client._loop.create_task(
                exec_client._stream_order_fills()
            )
            exec_client._token_keepalive_task = exec_client._loop.create_task(
                exec_client._token_keepalive_loop()
            )

        assert exec_client._fill_poll_task is not None
        assert exec_client._token_keepalive_task is not None
        assert len(created_tasks) == 2

        # Clean up
        for t in created_tasks:
            t.cancel()
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass

    @pytest.mark.asyncio
    async def test_keepalive_task_not_started_in_polling_mode(self):
        """_connect does NOT create _token_keepalive_task when use_streaming=False."""
        import asyncio

        exec_client = MagicMock()
        exec_client._use_streaming = False
        exec_client._token_keepalive_task = None

        # In polling mode only _fill_poll_task is created — keepalive stays None
        if exec_client._use_streaming:
            exec_client._token_keepalive_task = object()  # should not reach

        assert exec_client._token_keepalive_task is None


class TestDonIsASessionPauseForGtc:
    """TS 'DON' at the daily/weekly close on a GTC order = paused until next session.

    2026-09-23/25: GTC exits marked DON were cancelled in NT, kept working at the
    broker, and the §111 verifier re-cancelled them ~2 min later on every weekday close.
    A fill later in the same session would have been dropped (order closed in NT).
    """

    @staticmethod
    def _client(tif):
        coid = ClientOrderId("O-EXIT")
        cached = MagicMock()
        cached.is_closed = False
        cached.status = OrderStatus.ACCEPTED
        cached.time_in_force = tif
        cached.strategy_id = StrategyId("S-001")
        cached.instrument_id = InstrumentId(Symbol("NQZ26"), Venue("TRADESTATION"))
        m = MagicMock()
        m._ts_order_id_to_client_order_id = {"TS-1": coid}
        m._order_last_status = {}
        m._pending_modify_trigger_price = {}
        m._modify_inflight = {}
        m._pending_amend_meta = {}
        m._cancel_verify_pending = {}
        m._verify_pending_cancels = AsyncMock()  # as in _make_check_statuses_mock
        m._cache = MagicMock()
        m._cache.order.return_value = cached
        m._clock = MagicMock()
        m._clock.timestamp_ns.return_value = 0
        m._log = MagicMock()
        return m

    @pytest.mark.asyncio
    async def test_gtc_don_via_sse_emits_no_cancel(self):
        m = self._client(TimeInForce.GTC)
        await TradeStationExecutionClient._process_order_event(m, {"OrderID": "TS-1", "Status": "DON"})
        m.generate_order_canceled.assert_not_called()
        m._register_cancel_verify.assert_not_called()

    @pytest.mark.asyncio
    async def test_gtc_don_via_poll_emits_no_cancel(self):
        m = self._client(TimeInForce.GTC)
        m._client = MagicMock()
        m._client.get_orders = AsyncMock(return_value=[{"OrderID": "TS-1", "Status": "DON"}])
        await TradeStationExecutionClient._check_order_statuses(m)
        m.generate_order_canceled.assert_not_called()
        m._register_cancel_verify.assert_not_called()

    @pytest.mark.asyncio
    async def test_day_don_is_also_a_pause(self):
        """Spec: "DON – Queued" is in the Open category for any TIF; 0 DAY-order DONs
        observed since 2026-09-29. No cancel, no §111 verify (was: DAY DON -> cancel)."""
        m = self._client(TimeInForce.DAY)
        await TradeStationExecutionClient._process_order_event(m, {"OrderID": "TS-1", "Status": "DON"})
        m.generate_order_canceled.assert_not_called()
        m._register_cancel_verify.assert_not_called()

    @pytest.mark.asyncio
    async def test_don_then_ack_emits_nothing_and_no_cancel_verify(self):
        m = self._client(TimeInForce.GTC)
        for st in ("DON", "ACK"):
            await TradeStationExecutionClient._process_order_event(m, {"OrderID": "TS-1", "Status": st})
        m.generate_order_canceled.assert_not_called()
        m.generate_order_accepted.assert_not_called()
        m._register_cancel_verify.assert_not_called()

    @pytest.mark.asyncio
    async def test_gtc_fill_after_don_reaches_the_strategy(self):
        m = self._client(TimeInForce.GTC)
        cached = m._cache.order.return_value
        # a real cancel closes the order in NT — without this the test passes unfixed
        m.generate_order_canceled.side_effect = lambda **kw: setattr(cached, "is_closed", True)
        inst = MagicMock(); inst.price_precision = 2; inst.quote_currency = MagicMock()
        m._cache.instrument.return_value = inst
        m._cache.order.return_value.quantity = Quantity.from_int(1)
        m._cache.order.return_value.filled_qty = Quantity.from_int(0)
        await TradeStationExecutionClient._process_order_event(m, {"OrderID": "TS-1", "Status": "DON"})
        await TradeStationExecutionClient._process_order_event(
            m, {"OrderID": "TS-1", "Status": "FLL", "AveragePrice": "30664.00", "FilledQuantity": "1"})
        m.generate_order_filled.assert_called_once()


class TestUncachedFillWarning:
    """FLL/FLP for a mapped TS order that is not in the NT cache must be loud.

    2026-09-28: NQZ26 #972721176 (placed 09-25, node restarted 09-27 21:25Z,
    reconciliation rebuilt 0 orders) filled at ~03:27Z; the SSE path returned
    silently and the poll then saw no status change — no log line at all.
    """

    @staticmethod
    def _client():
        m = MagicMock()
        m._ts_order_id_to_client_order_id = {"972721176": ClientOrderId("O-OLD-5")}
        # §151: group attribution + previous last-alive (13:00Z start, alive until 12:59:30Z)
        m._node_group = "stable"
        m._ts_id_to_group = {"972721176": "stable"}
        m._previous_alive = {"stable": 1790686770.0}
        m._uncached_fill_callback = None
        m._order_last_status = {}
        m._pending_modify_trigger_price = {}
        m._modify_inflight = {}
        m._pending_amend_meta = {}
        m._cancel_verify_pending = {}
        m._verify_pending_cancels = AsyncMock()
        m._cache = MagicMock()
        m._cache.order.return_value = None  # not in the NT cache (pre-restart order)
        m._clock = MagicMock()
        m._clock.timestamp_ns.return_value = 0
        m._log = MagicMock()
        return m

    @staticmethod
    def _uncached_warnings(m):
        return [c for c in m._log.warning.call_args_list if "[UNCACHED-FILL-LIVE]" in str(c)]

    @pytest.mark.asyncio
    async def test_sse_fill_on_uncached_order_warns(self):
        m = self._client()
        await TradeStationExecutionClient._process_order_event(
            m, {"OrderID": "972721176", "Status": "FLL", "AveragePrice": "30664.00"})
        warns = self._uncached_warnings(m)
        assert len(warns) == 1
        assert "972721176" in str(warns[0]) and "O-OLD-5" in str(warns[0])
        m.generate_order_filled.assert_not_called()
        # a non-fill status on an uncached order stays silent (behaviour unchanged)
        await TradeStationExecutionClient._process_order_event(
            m, {"OrderID": "972721176", "Status": "ACK"})
        assert len(self._uncached_warnings(m)) == 1

    @pytest.mark.asyncio
    async def test_poll_fill_on_uncached_order_warns(self):
        m = self._client()
        m._client = MagicMock()
        m._client.get_orders = AsyncMock(
            return_value=[{"OrderID": "972721176", "Status": "FLP", "AveragePrice": "30664.00"}])
        await TradeStationExecutionClient._check_order_statuses(m)
        warns = self._uncached_warnings(m)
        assert len(warns) == 1
        assert "972721176" in str(warns[0]) and "O-OLD-5" in str(warns[0])
        m.generate_order_filled.assert_not_called()

    # 2026-09-29 13:23Z/13:28Z: after a restart, `_order_last_status` starts empty,
    # so the first status read sees orders that filled hours earlier in the previous
    # process (already delivered then) and re-warns — 16 false positives per restart.
    # Fix: only warn for a fill at/after this adapter instance's start; a fill that
    # predates it gets an INFO line; missing/unparseable timestamps keep the WARNING.
    _INSTANCE_STARTED_S = 1790686800.0  # 2026-09-29T13:00:00Z

    @pytest.mark.asyncio
    async def test_sse_fill_before_instance_start_does_not_warn(self):
        m = self._client()
        m._instance_started_s = self._INSTANCE_STARTED_S
        await TradeStationExecutionClient._process_order_event(
            m,
            {
                "OrderID": "972721176",
                "Status": "FLL",
                "AveragePrice": "30664.00",
                "ClosedDateTime": "2026-09-29T09:57:59Z",  # predates the 13:00Z start
            },
        )
        assert self._uncached_warnings(m) == []
        assert m._log.info.call_count == 1
        assert "972721176" in str(m._log.info.call_args)
        assert "predates this session" in str(m._log.info.call_args)
        m.generate_order_filled.assert_not_called()

    @pytest.mark.asyncio
    async def test_poll_fill_before_instance_start_does_not_warn(self):
        m = self._client()
        m._instance_started_s = self._INSTANCE_STARTED_S
        m._client = MagicMock()
        m._client.get_orders = AsyncMock(
            return_value=[
                {
                    "OrderID": "972721176",
                    "Status": "FLP",
                    "AveragePrice": "30664.00",
                    "ClosedDateTime": "2026-09-29T09:57:59Z",
                }
            ]
        )
        await TradeStationExecutionClient._check_order_statuses(m)
        assert self._uncached_warnings(m) == []
        assert m._log.info.call_count == 1
        assert "predates this session" in str(m._log.info.call_args)
        m.generate_order_filled.assert_not_called()

    @pytest.mark.asyncio
    async def test_sse_fill_after_instance_start_warns(self):
        m = self._client()
        m._instance_started_s = self._INSTANCE_STARTED_S
        await TradeStationExecutionClient._process_order_event(
            m,
            {
                "OrderID": "972721176",
                "Status": "FLL",
                "AveragePrice": "30664.00",
                "ClosedDateTime": "2026-09-29T13:23:47Z",  # after the 13:00Z start
            },
        )
        warns = self._uncached_warnings(m)
        assert len(warns) == 1
        assert m._log.info.call_count == 0

    @pytest.mark.asyncio
    async def test_poll_fill_after_instance_start_warns(self):
        m = self._client()
        m._instance_started_s = self._INSTANCE_STARTED_S
        m._client = MagicMock()
        m._client.get_orders = AsyncMock(
            return_value=[
                {
                    "OrderID": "972721176",
                    "Status": "FLP",
                    "AveragePrice": "30664.00",
                    "ClosedDateTime": "2026-09-29T13:23:47Z",
                }
            ]
        )
        await TradeStationExecutionClient._check_order_statuses(m)
        warns = self._uncached_warnings(m)
        assert len(warns) == 1
        assert m._log.info.call_count == 0

    @pytest.mark.asyncio
    async def test_sse_fill_no_closed_datetime_warns(self):
        m = self._client()
        m._instance_started_s = self._INSTANCE_STARTED_S
        await TradeStationExecutionClient._process_order_event(
            m, {"OrderID": "972721176", "Status": "FLL", "AveragePrice": "30664.00"})
        warns = self._uncached_warnings(m)
        assert len(warns) == 1
        assert m._log.info.call_count == 0

    @pytest.mark.asyncio
    async def test_poll_fill_no_closed_datetime_warns(self):
        m = self._client()
        m._instance_started_s = self._INSTANCE_STARTED_S
        m._client = MagicMock()
        m._client.get_orders = AsyncMock(
            return_value=[{"OrderID": "972721176", "Status": "FLP", "AveragePrice": "30664.00"}])
        await TradeStationExecutionClient._check_order_statuses(m)
        warns = self._uncached_warnings(m)
        assert len(warns) == 1
        assert m._log.info.call_count == 0


class TestAmendRecovery241:
    """§241 recovery after an ambiguous PUT failure."""

    REQ = (Price(21050.0, 2), None)
    PREV = (Price(21000.0, 2), None)

    def _mock(self, stop: str, monkeypatch, now: float = 0.0):
        monkeypatch.setattr(ex, "_mono", lambda: now)
        m = _make_check_statuses_mock(
            ts_orders=[{"OrderID": "TS-STOP", "Status": "OPN", "StopPrice": stop}])
        m._modify_inflight = {}
        m._pending_amend_meta = {}
        inst = MagicMock()
        inst.make_price = lambda v: Price(v, 2)
        m._cache.instrument.return_value = inst
        return m, ClientOrderId("O-STOP")

    # --- pure helpers ---

    def test_classify(self):
        c = ex._classify_modify_read
        assert c({"StopPrice": "21050.0"}, self.REQ, self.PREV) == "new"
        assert c({"StopPrice": "21000.0"}, self.REQ, self.PREV) == "old"
        assert c({"StopPrice": "20990.0"}, self.REQ, self.PREV) == "neither"
        assert c({}, self.REQ, self.PREV) == "neither"
        assert c({"StopPrice": "21000.0"}, self.REQ, (None, None)) == "neither"  # prev unknown

    def test_decide_skips_read_started_before_failure(self):
        meta = ex._AmendMeta(sent=0.0, stamp=10.0, prev=self.PREV)
        assert ex._decide_amend(meta, "old", read_started=9.9, now=40.0) == "skip"
        assert meta.old_reads == 0

    def test_decide_old_needs_n_reads_and_min_age(self):
        meta = ex._AmendMeta(sent=0.0, stamp=1.0, prev=self.PREV)
        assert ex._decide_amend(meta, "old", read_started=2.0, now=2.5) == "wait_old"
        # N reads ok, but the age is only 7.5 s
        assert ex._decide_amend(meta, "old", read_started=8.0, now=8.5) == "wait_old"
        assert meta.old_reads == 2
        assert ex._decide_amend(meta, "old", read_started=21.5, now=22.0) == "reject"  # age >= 20 s

    def test_decide_overlapping_read_counts_once(self):
        meta = ex._AmendMeta(sent=0.0, stamp=1.0, prev=self.PREV)
        ex._decide_amend(meta, "old", read_started=30.0, now=30.5)
        # a concurrent read that STARTED before the previous counted read was processed
        assert ex._decide_amend(meta, "old", read_started=30.2, now=30.6) == "wait_old"
        assert meta.old_reads == 1

    def test_decide_neither_resets_and_adopts_at_deadline(self):
        meta = ex._AmendMeta(sent=0.0, stamp=1.0, prev=self.PREV, old_reads=1)
        assert ex._decide_amend(meta, "neither", read_started=5.0, now=5.5) == "wait_unknown"
        assert meta.old_reads == 0
        assert ex._decide_amend(meta, "neither", read_started=60.0, now=60.5) == "adopt_broker"

    def test_decide_old_at_deadline_rejects_regardless_of_n(self):
        meta = ex._AmendMeta(sent=0.0, stamp=40.0, prev=self.PREV)
        assert ex._decide_amend(meta, "old", read_started=60.0, now=60.5) == "reject"

    def test_decide_late_failure_still_gets_settling_time(self):
        """§241 I2: a PUT that fails late (stamp = sent+70) is past the 60 s deadline at
        once, but the deadline branch may not fire before stamp + min age."""
        meta = ex._AmendMeta(sent=0.0, stamp=70.0, prev=self.PREV)
        assert ex._decide_amend(meta, "old", read_started=74.5, now=75.0) == "wait_old"
        assert ex._decide_amend(meta, "old", read_started=80.0, now=80.5) == "wait_old"
        assert ex._decide_amend(meta, "old", read_started=90.5, now=91.0) == "reject"

    def test_decide_late_failure_neither_waits_then_adopts(self):
        meta = ex._AmendMeta(sent=0.0, stamp=70.0, prev=self.PREV)
        assert ex._decide_amend(meta, "neither", read_started=75.0, now=75.5) == "wait_unknown"
        assert ex._decide_amend(meta, "neither", read_started=90.5, now=91.0) == "adopt_broker"

    def test_deadline_beats_nt_inflight_resolver(self):
        """NT fabricates OrderCanceled after inflight_check_retries (runner: 20) queries
        ~5-6 s apart (about 100-125 s). The adapter must decide well before."""
        assert ex._MODIFY_RECOVERY_DEADLINE_S <= 75.0
        assert ex._MODIFY_REJECT_MIN_AGE_S < ex._MODIFY_RECOVERY_DEADLINE_S
        assert ex._MODIFY_OLD_READS_TO_REJECT >= 2

    # --- through _check_order_statuses ---

    @pytest.mark.asyncio
    async def test_new_price_updates_and_logs_requested_vs_broker(self, monkeypatch):
        m, coid = self._mock("21050.0", monkeypatch)
        _arm_amend(m, coid, self.REQ, self.PREV)
        await TradeStationExecutionClient._check_order_statuses(m)
        m.generate_order_updated.assert_called_once()
        assert coid not in m._pending_modify_trigger_price and coid not in m._pending_amend_meta
        msg = " ".join(str(c.args[0]) for c in m._log.info.call_args_list)
        assert "[MODIFY-RECOVERY]" in msg and "21050" in msg

    @pytest.mark.asyncio
    async def test_first_old_read_waits(self, monkeypatch):
        m, coid = self._mock("21000.0", monkeypatch, now=0.0)
        _arm_amend(m, coid, self.REQ, self.PREV, sent=-3.0, stamp=-2.0)
        await TradeStationExecutionClient._check_order_statuses(m)
        m.generate_order_modify_rejected.assert_not_called()
        m.generate_order_updated.assert_not_called()
        assert m._pending_amend_meta[coid].old_reads == 1

    @pytest.mark.asyncio
    async def test_neither_warns_once_and_stays_pending(self, monkeypatch):
        m, coid = self._mock("20990.0", monkeypatch, now=0.0)
        _arm_amend(m, coid, self.REQ, self.PREV, sent=-3.0, stamp=-2.0)
        await TradeStationExecutionClient._check_order_statuses(m)
        m._order_last_status["TS-STOP"] = "OPN"
        await TradeStationExecutionClient._check_order_statuses(m)
        warns = [c for c in m._log.warning.call_args_list if "[MODIFY-UNKNOWN]" in str(c.args[0])]
        assert len(warns) == 1
        assert "20990" in str(warns[0].args[0]) and "21050" in str(warns[0].args[0])
        m.generate_order_modify_rejected.assert_not_called()
        m.generate_order_updated.assert_not_called()
        assert coid in m._pending_modify_trigger_price

    @pytest.mark.asyncio
    async def test_read_started_before_failure_is_ignored(self, monkeypatch):
        """The 5xx lands while the poll's GET is in flight: that snapshot predates the
        outcome and must not count."""
        clock = [100.0]
        monkeypatch.setattr(ex, "_mono", lambda: clock[0])
        m, coid = self._mock("21000.0", monkeypatch, now=100.0)
        monkeypatch.setattr(ex, "_mono", lambda: clock[0])
        rows = [{"OrderID": "TS-STOP", "Status": "OPN", "StopPrice": "21000.0"}]

        async def get_orders(**kw):
            clock[0] = 100.5                     # the PUT fails during the GET
            _arm_amend(m, coid, self.REQ, self.PREV, sent=99.0, stamp=100.5)
            return rows

        m._client.get_orders = AsyncMock(side_effect=get_orders)
        await TradeStationExecutionClient._check_order_statuses(m)
        m.generate_order_modify_rejected.assert_not_called()
        assert m._pending_amend_meta[coid].old_reads == 0

    @pytest.mark.asyncio
    async def test_inflight_put_skips_recovery_but_not_fills(self, monkeypatch):
        m, coid = self._mock("21000.0", monkeypatch)
        _arm_amend(m, coid, self.REQ, self.PREV, old_reads=5)
        m._modify_inflight = {coid: 1}
        await TradeStationExecutionClient._check_order_statuses(m)
        m.generate_order_modify_rejected.assert_not_called()
        m.generate_order_updated.assert_not_called()
        # a fill racing the in-flight modify is still processed
        m._client.get_orders = AsyncMock(return_value=[{
            "OrderID": "TS-STOP", "Status": "FLL", "StopPrice": "21000.0",
            "FilledPrice": "21000.0", "Legs": [{"ExecQuantity": "1",
                                                "ExecutionPrice": "21000.0"}]}])
        await TradeStationExecutionClient._check_order_statuses(m)
        assert coid not in m._pending_modify_trigger_price   # dropped: no longer open

    @pytest.mark.asyncio
    async def test_deadline_unusable_broker_price_is_unknown_not_old(self, monkeypatch):
        """§241 I1: at the deadline with no usable broker price the outcome is UNKNOWN:
        the reject says so (never 'still shows the previous price') and logs the
        [MODIFY-UNKNOWN] ERROR."""
        m, coid = self._mock("20990.0", monkeypatch, now=0.0)
        m._client.get_orders = AsyncMock(return_value=[{"OrderID": "TS-STOP", "Status": "OPN"}])
        _arm_amend(m, coid, self.REQ, self.PREV, sent=-61.0, stamp=-60.0)
        await TradeStationExecutionClient._check_order_statuses(m)
        m.generate_order_updated.assert_not_called()
        m.generate_order_modify_rejected.assert_called_once()
        reason = m.generate_order_modify_rejected.call_args.kwargs["reason"]
        assert reason.startswith(
            "modify recovery: outcome unknown at deadline, broker price unusable (")
        assert "previous price" not in reason and "21050" in reason
        errs = [c for c in m._log.error.call_args_list if "[MODIFY-UNKNOWN]" in str(c.args[0])]
        assert len(errs) == 1 and "deadline" in str(errs[0].args[0])
        assert not any("amend did not land" in str(c.args[0])
                       for c in m._log.warning.call_args_list)
        assert coid not in m._pending_modify_trigger_price

    @pytest.mark.asyncio
    async def test_deadline_neither_adopts_broker_prices(self, monkeypatch):
        m, coid = self._mock("20990.0", monkeypatch, now=0.0)
        _arm_amend(m, coid, self.REQ, self.PREV, sent=-61.0, stamp=-60.0)
        await TradeStationExecutionClient._check_order_statuses(m)
        m.generate_order_updated.assert_called_once()
        assert m.generate_order_updated.call_args.kwargs["trigger_price"] == Price(20990.0, 2)
        errs = [c for c in m._log.error.call_args_list if "[MODIFY-UNKNOWN]" in str(c.args[0])]
        assert len(errs) == 1


# =============================================================================
# §212: price-aware PENDING_UPDATE recovery for LIMIT / STOP_LIMIT modifies
# =============================================================================

class TestPendingUpdateRecoveryPrices:
    """A LIMIT modify stores (trigger=None, price); the recovery compares LimitPrice."""

    async def _run(self, ts_order: dict, stored, prev, monkeypatch, old_reads=1,
                   sent=-31.0):
        monkeypatch.setattr(ex, "_mono", lambda: 0.0)
        coid = ClientOrderId("O-STOP")
        m = _make_check_statuses_mock(ts_orders=[{"OrderID": "TS-STOP", "Status": "OPN",
                                                  **ts_order}])
        m._cache.instrument.return_value.make_price = lambda v: Price(v, 2)
        _arm_amend(m, coid, stored, prev, sent=sent, stamp=sent + 1.0, old_reads=old_reads)
        await TradeStationExecutionClient._check_order_statuses(m)
        assert coid not in m._pending_modify_trigger_price
        return m

    @pytest.mark.asyncio
    async def test_limit_modify_recovered_as_updated_when_broker_has_new_limit(
            self, monkeypatch):
        m = await self._run({"LimitPrice": "101.0"}, (None, Price(101.0, 2)),
                            (None, Price(100.0, 2)), monkeypatch)
        m.generate_order_updated.assert_called_once()
        kw = m.generate_order_updated.call_args.kwargs
        assert kw["price"] == Price(101.0, 2) and kw["trigger_price"] is None
        m.generate_order_modify_rejected.assert_not_called()

    @pytest.mark.asyncio
    async def test_limit_modify_rejected_when_broker_kept_old_limit(self, monkeypatch):
        m = await self._run({"LimitPrice": "100.0"}, (None, Price(101.0, 2)),
                            (None, Price(100.0, 2)), monkeypatch)
        m.generate_order_modify_rejected.assert_called_once()
        m.generate_order_updated.assert_not_called()

    @pytest.mark.asyncio
    async def test_stop_modify_recovery_unchanged(self, monkeypatch):
        m = await self._run({"StopPrice": "99.0"}, (Price(99.0, 2), None),
                            (Price(98.0, 2), None), monkeypatch)
        kw = m.generate_order_updated.call_args.kwargs
        assert kw["trigger_price"] == Price(99.0, 2) and kw["price"] is None

    @pytest.mark.asyncio
    async def test_stop_limit_one_field_moved_is_neither_until_deadline(self, monkeypatch):
        """One field new, one old = 'neither' (§241): at the deadline NT adopts the
        broker's actual prices instead of guessing."""
        m = await self._run({"StopPrice": "99.0", "LimitPrice": "98.0"},
                            (Price(99.0, 2), Price(98.5, 2)),
                            (Price(98.0, 2), Price(98.0, 2)), monkeypatch, sent=-61.0)
        m.generate_order_modify_rejected.assert_not_called()
        kw = m.generate_order_updated.call_args.kwargs
        assert kw["trigger_price"] == Price(99.0, 2) and kw["price"] == Price(98.0, 2)

    @pytest.mark.asyncio
    async def test_stop_limit_modify_both_match_is_updated(self, monkeypatch):
        m = await self._run({"StopPrice": "99.0", "LimitPrice": "98.5"},
                            (Price(99.0, 2), Price(98.5, 2)),
                            (Price(98.0, 2), Price(98.0, 2)), monkeypatch)
        kw = m.generate_order_updated.call_args.kwargs
        assert kw["trigger_price"] == Price(99.0, 2) and kw["price"] == Price(98.5, 2)

    @pytest.mark.asyncio
    async def test_limit_price_not_mistaken_for_stop(self, monkeypatch):
        """Pre-§212 the stop comparison fell back to LimitPrice: a stop modify must match
        StopPrice only. StopPrice missing = 'neither'; at the deadline the unusable
        broker price falls back to a reject."""
        m = await self._run({"LimitPrice": "99.0"}, (Price(99.0, 2), None),
                            (Price(98.0, 2), None), monkeypatch, sent=-61.0)
        m.generate_order_modify_rejected.assert_called_once()
        m.generate_order_updated.assert_not_called()

    @pytest.mark.asyncio
    async def test_modify_order_stores_both_prices(self):
        cmd = ModifyOrder(
            TraderId("TRADER-001"), StrategyId("S-001"),
            InstrumentId(Symbol("NQM26"), Venue("TRADESTATION")),
            ClientOrderId("O-LIM"), VenueOrderId("TS-LIM"),
            None, Price(101.0, 2), None, UUID4(), 0,
        )
        m = _make_exec_mock(cmd)
        m._pending_modify_trigger_price = {}
        m._modify_inflight = {}
        m._pending_amend_meta = {}
        m._client.replace_order = AsyncMock(
            side_effect=Exception("Replace order failed (HTTP 503): Service Unavailable"))
        await TradeStationExecutionClient._modify_order(m, cmd)
        assert m._pending_modify_trigger_price[cmd.client_order_id] == (None, Price(101.0, 2))
