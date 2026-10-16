"""A submit that failed AFTER the POST was sent is flagged [SUBMIT-AMBIGUOUS] (LESSONS §207).

Observe-only: the order is still rejected in NT; the log names it so the operator checks /orders.
"""

from unittest.mock import AsyncMock

import httpx

from tests.test_kit import make_market_order
from tests.test_order_attribution import _build_submit_command, _make_submit_order_stub
from tradestation_nt_community.execution import TradeStationExecutionClient, _is_ambiguous_submit
from tradestation_nt_community.http.client import DuplicateOrderConfirmIdException


def test_classification():
    assert _is_ambiguous_submit(httpx.ReadTimeout("t"))
    assert _is_ambiguous_submit(httpx.RemoteProtocolError("peer closed"))
    assert _is_ambiguous_submit(DuplicateOrderConfirmIdException("dup", "cid"))
    assert not _is_ambiguous_submit(httpx.ConnectError("refused"))  # never sent
    assert not _is_ambiguous_submit(httpx.ConnectTimeout("t"))
    assert not _is_ambiguous_submit(Exception("Place order failed (HTTP 500): x"))


async def _submit(exc):
    stub = _make_submit_order_stub()
    stub._client.place_order = AsyncMock(side_effect=exc)
    await TradeStationExecutionClient._submit_order(
        stub, _build_submit_command(make_market_order(tags=None))
    )
    return stub


async def test_read_timeout_is_flagged_and_still_rejected():
    stub = await _submit(httpx.ReadTimeout("t"))
    stub.generate_order_rejected.assert_called_once()
    assert any("[SUBMIT-AMBIGUOUS]" in str(c.args[0]) for c in stub._log.error.call_args_list)


async def test_connect_error_is_not_flagged():
    stub = await _submit(httpx.ConnectError("refused"))
    stub.generate_order_rejected.assert_called_once()
    assert not any("[SUBMIT-AMBIGUOUS]" in str(c.args[0]) for c in stub._log.error.call_args_list)
