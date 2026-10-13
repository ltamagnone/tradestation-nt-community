"""A cancel that arrives BEFORE TradeStation acknowledges the order must still cancel it.

Live 2026-10-12, fc_5_699: an exit stop was cancelled 0.6 s after submit; the adapter had
no TS id yet, rejected the cancel and never retried, and the order (#974504866) was
accepted 2.6 s later and stayed working for 8 minutes. Now the cancel is still reported
rejected to NT (no state-machine change), and sent for real right after the accept.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

from tradestation_nt_community.execution import TradeStationExecutionClient
from tests import test_execution as _te
from tests.test_kit import make_market_order
from tests.test_order_attribution import _build_submit_command, _make_submit_order_stub


def _real(stub, *names):
    """Bind the real helper methods onto a MagicMock stub."""
    for n in names:
        fn = getattr(TradeStationExecutionClient, n)
        setattr(stub, n, (lambda f: (lambda *a, **k: f(stub, *a, **k)))(fn))
    stub._cancel_after_ack_coids = {}
    stub._cancel_requested_coids = {}
    return stub


def _submit_stub():
    stub = _make_submit_order_stub()
    cached = MagicMock()
    cached.is_closed = False
    stub._cache.order.return_value = cached
    stub._client.cancel_order = AsyncMock(return_value={})
    stub._cancel_if_closed_before_accept = AsyncMock(return_value=False)
    return _real(stub, "_remember_cancel_after_ack", "_note_cancel_requested")


async def test_cancel_before_ack_is_sent_right_after_accept():
    order = make_market_order(tags=None)
    stub = _submit_stub()
    stub._remember_cancel_after_ack(order.client_order_id)

    await TradeStationExecutionClient._submit_order(stub, _build_submit_command(order))

    stub.generate_order_accepted.assert_called_once()
    stub._client.cancel_order.assert_awaited_once_with("TS-123")
    assert order.client_order_id in stub._cancel_requested_coids      # §111/F4: ours
    assert order.client_order_id not in stub._cancel_after_ack_coids  # consumed once


async def test_no_pending_cancel_means_no_cancel():
    order = make_market_order(tags=None)
    stub = _submit_stub()
    await TradeStationExecutionClient._submit_order(stub, _build_submit_command(order))
    stub.generate_order_accepted.assert_called_once()
    stub._client.cancel_order.assert_not_called()


async def test_failed_submit_forgets_the_pending_cancel():
    order = make_market_order(tags=None)
    stub = _submit_stub()
    stub._client.place_order = AsyncMock(side_effect=RuntimeError("HTTP 500"))
    stub._remember_cancel_after_ack(order.client_order_id)

    await TradeStationExecutionClient._submit_order(stub, _build_submit_command(order))

    stub.generate_order_rejected.assert_called_once()
    stub._client.cancel_order.assert_not_called()
    assert order.client_order_id not in stub._cancel_after_ack_coids


async def test_cancel_after_ack_failure_is_logged_not_raised():
    order = make_market_order(tags=None)
    stub = _submit_stub()
    stub._client.cancel_order = AsyncMock(side_effect=RuntimeError("HTTP 503"))
    stub._remember_cancel_after_ack(order.client_order_id)

    await TradeStationExecutionClient._submit_order(stub, _build_submit_command(order))

    stub.generate_order_accepted.assert_called_once()
    stub.generate_order_rejected.assert_not_called()
    assert any("[CANCEL-AFTER-ACK]" in str(c.args[0]) and "failed" in str(c.args[0])
               for c in stub._log.error.call_args_list)


async def test_missing_ts_id_rejects_and_queues():
    m, cmd = _te.TestCancelOrderRejectedEmission._make_cancel_order_mock(None, "950568948")
    m._client_order_id_to_ts_order_id = {}
    m._client.cancel_order = AsyncMock()
    m.generate_order_cancel_rejected = MagicMock()
    _real(m, "_remember_cancel_after_ack")

    await TradeStationExecutionClient._cancel_order(m, cmd)

    m._client.cancel_order.assert_not_awaited()            # nothing to cancel yet
    m.generate_order_cancel_rejected.assert_called_once()  # NT contract unchanged
    assert cmd.client_order_id in m._cancel_after_ack_coids


def test_pending_map_is_bounded():
    stub = _real(MagicMock(), "_remember_cancel_after_ack")
    for i in range(600):
        stub._remember_cancel_after_ack(f"O-{i}")
    assert len(stub._cancel_after_ack_coids) == 500
    assert "O-0" not in stub._cancel_after_ack_coids and "O-599" in stub._cancel_after_ack_coids
