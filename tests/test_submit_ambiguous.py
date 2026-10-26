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


# --- strategies OPEN_POINTS 1.7: the reject reason carries a stable marker, so a consumer
# can tell "may be live at TradeStation" from a real reject (and never resend the order).
async def test_ambiguous_reject_reason_carries_the_marker():
    from tradestation_nt_community import SUBMIT_AMBIGUOUS_REASON_PREFIX
    stub = await _submit(httpx.ReadTimeout(""))
    reason = stub.generate_order_rejected.call_args.kwargs["reason"]
    assert reason.startswith(SUBMIT_AMBIGUOUS_REASON_PREFIX)
    assert "ReadTimeout" in reason          # str(e) is empty here: the type names it


async def test_non_ambiguous_reject_reason_has_no_marker():
    from tradestation_nt_community import SUBMIT_AMBIGUOUS_REASON_PREFIX
    stub = await _submit(httpx.ConnectError("refused"))
    reason = stub.generate_order_rejected.call_args.kwargs["reason"]
    assert not reason.startswith(SUBMIT_AMBIGUOUS_REASON_PREFIX) and reason == "refused"


async def test_ambiguous_group_reject_reasons_carry_the_marker():
    from tests.test_order_response_errors import _group_stub
    from tradestation_nt_community import SUBMIT_AMBIGUOUS_REASON_PREFIX
    stub, cmd = _group_stub([False, False])
    stub._client.place_order_group = AsyncMock(side_effect=httpx.ReadTimeout("t"))
    await TradeStationExecutionClient._submit_order_group(stub, cmd, "OCO", [{}, {}])
    reasons = [c.kwargs["reason"] for c in stub.generate_order_rejected.call_args_list]
    assert len(reasons) == 2
    assert all(r.startswith(SUBMIT_AMBIGUOUS_REASON_PREFIX) for r in reasons), reasons
