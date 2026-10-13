"""A POST that succeeds after NT already closed the order (in-flight check resolved a
slow SUBMITTED order as REJECTED) must cancel the broker order, never emit OrderAccepted."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

from tradestation_nt_community.execution import TradeStationExecutionClient
from tests.test_kit import make_market_order
from tests.test_order_attribution import _build_submit_command, _make_submit_order_stub


def _stub_with_cache(order_closed: bool):
    stub = _make_submit_order_stub()
    cached = MagicMock()
    cached.is_closed = order_closed
    cached.status_string.return_value = "REJECTED" if order_closed else "SUBMITTED"
    stub._cache.order.return_value = cached
    stub._client.cancel_order = AsyncMock(return_value={})
    stub._cancel_if_closed_before_accept = (
        lambda coid, tsid: TradeStationExecutionClient._cancel_if_closed_before_accept(
            stub, coid, tsid))
    return stub


async def test_late_accept_on_closed_order_cancels_at_broker():
    order = make_market_order(tags=None)
    stub = _stub_with_cache(order_closed=True)
    await TradeStationExecutionClient._submit_order(stub, _build_submit_command(order))
    stub._client.cancel_order.assert_awaited_once_with("TS-123")
    stub.generate_order_accepted.assert_not_called()
    stub.generate_order_rejected.assert_not_called()
    assert any("[LATE-ACCEPT-CANCELLED]" in str(c.args[0])
               for c in stub._log.error.call_args_list)


async def test_normal_accept_unchanged():
    order = make_market_order(tags=None)
    stub = _stub_with_cache(order_closed=False)
    await TradeStationExecutionClient._submit_order(stub, _build_submit_command(order))
    stub._client.cancel_order.assert_not_called()
    stub.generate_order_accepted.assert_called_once()


async def test_late_accept_cancel_failure_still_skips_accept():
    order = make_market_order(tags=None)
    stub = _stub_with_cache(order_closed=True)
    stub._client.cancel_order = AsyncMock(side_effect=RuntimeError("HTTP 500"))
    await TradeStationExecutionClient._submit_order(stub, _build_submit_command(order))
    stub.generate_order_accepted.assert_not_called()
    assert sum("[LATE-ACCEPT-CANCELLED]" in str(c.args[0])
               for c in stub._log.error.call_args_list) == 2


def _group_stub(closed_flags):
    stub = MagicMock()
    orders = []
    cached = {}
    for k, closed in enumerate(closed_flags):
        o = MagicMock()
        o.client_order_id = f"O-{k}"
        orders.append(o)
        c = MagicMock()
        c.is_closed = closed
        cached[f"O-{k}"] = c
    stub._cache.order.side_effect = lambda coid: cached[coid]
    stub._client.place_order_group = AsyncMock(
        return_value={"Orders": [{"OrderID": f"9{k}"} for k in range(len(orders))]})
    stub._client.cancel_order = AsyncMock(return_value={})
    stub._ts_order_id_to_client_order_id = {}
    stub._client_order_id_to_ts_order_id = {}
    cmd = MagicMock()
    cmd.order_list.orders = orders
    return stub, cmd


async def test_group_late_accept_cancels_every_leg_and_rejects_open_ones():
    stub, cmd = _group_stub([True, False])          # leg 0 closed by NT, leg 1 open
    await TradeStationExecutionClient._submit_order_group(stub, cmd, "OCO", [{}, {}])
    assert sorted(c.args[0] for c in stub._client.cancel_order.await_args_list) == ["90", "91"]
    stub.generate_order_accepted.assert_not_called()
    rejected = [c.kwargs["client_order_id"] for c in stub.generate_order_rejected.call_args_list]
    assert rejected == ["O-1"]
    assert any("[LATE-ACCEPT-CANCELLED]" in str(c.args[0]) for c in stub._log.error.call_args_list)


async def test_group_normal_accept_unchanged():
    stub, cmd = _group_stub([False, False])
    await TradeStationExecutionClient._submit_order_group(stub, cmd, "OCO", [{}, {}])
    stub._client.cancel_order.assert_not_called()
    assert stub.generate_order_accepted.call_count == 2


def test_fill_on_rejected_order_is_logged():
    from nautilus_trader.model.enums import OrderStatus
    from tradestation_nt_community.execution import _log_fill_after_cancel
    log = MagicMock()
    order = MagicMock()
    order.status = OrderStatus.REJECTED
    order.instrument_id.symbol.value = "ESZ26"
    _log_fill_after_cancel(log, "FLL", "91", "O-1", order)
    assert "[FILL-AFTER-CANCEL]" in log.error.call_args.args[0]
    assert "REJECTED" in log.error.call_args.args[0]
