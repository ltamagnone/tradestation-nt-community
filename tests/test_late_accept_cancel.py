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
