"""HTTP-200 bodies: top-level Errors[], partial groups, replace/cancel Error (LESSONS §207)."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from tests.test_http_client import _mock_resp, http_client  # noqa: F401 (fixture)
from tests.test_late_accept_cancel import _group_stub
from tradestation_nt_community.execution import TradeStationExecutionClient as C
from tradestation_nt_community.http.client import (
    OrderGroupPartialException,
    OrderRejectedException,
)

LEG = {
    "AccountID": "SIM1",
    "Symbol": "GCZ26",
    "Quantity": "1",
    "OrderType": "StopMarket",
    "TradeAction": "Sell",
    "TimeInForce": {"Duration": "GTC"},
    "StopPrice": "1",
}


async def test_place_order_top_level_errors_reject(http_client):  # noqa: F811
    http_client._httpx.post = AsyncMock(
        return_value=_mock_resp(
            200,
            {"Errors": [{"Error": "FAILED", "Message": "Order failed. Reason: contract expired."}]},
        )
    )
    with pytest.raises(OrderRejectedException, match="contract expired"):
        await http_client.place_order("SIM1", "GCZ26", "1", "Market", "Buy")


async def test_group_partial_raises_with_the_landed_ids(http_client):  # noqa: F811
    http_client._httpx.post = AsyncMock(
        return_value=_mock_resp(
            200,
            {
                "Orders": [{"OrderID": "11", "Message": "ok"}],
                "Errors": [{"Error": "FAILED", "Message": "leg 2 rejected"}],
            },
        )
    )
    with pytest.raises(OrderGroupPartialException) as ei:
        await http_client.place_order_group("OCO", [LEG, LEG])
    assert ei.value.landed_order_ids == ["11"]
    assert "leg 2 rejected" in ei.value.ts_message


async def test_group_count_mismatch_without_errors_is_partial(http_client):  # noqa: F811
    http_client._httpx.post = AsyncMock(
        return_value=_mock_resp(200, {"Orders": [{"OrderID": "11"}]})
    )
    with pytest.raises(OrderGroupPartialException):
        await http_client.place_order_group("OCO", [LEG, LEG])


async def test_group_all_failed_is_a_plain_rejection(http_client):  # noqa: F811
    http_client._httpx.post = AsyncMock(
        return_value=_mock_resp(
            200,
            {
                "Errors": [
                    {"Error": "FAILED", "Message": "no margin"},
                    {"Error": "FAILED", "Message": "no margin"},
                ]
            },
        )
    )
    with pytest.raises(OrderRejectedException) as ei:
        await http_client.place_order_group("OCO", [LEG, LEG])
    assert not isinstance(ei.value, OrderGroupPartialException)
    assert "no margin" in ei.value.ts_message


async def test_group_full_success_unchanged(http_client):  # noqa: F811
    body = {"Orders": [{"OrderID": "11"}, {"OrderID": "12"}], "Errors": []}
    http_client._httpx.post = AsyncMock(return_value=_mock_resp(200, body))
    assert await http_client.place_order_group("OCO", [LEG, LEG]) == body


# Replace/cancel 200 bodies are observe-only: no real error body has ever been captured, and a
# wrong raise on cancel would emit cancel_rejected for orders the broker did cancel.
SPEC_PUT_OK = {"Message": "Cancel/Replace order sent.", "OrderID": "123456789"}
SPEC_DELETE_OK = {"Message": "Cancel request sent", "OrderID": "123456789"}


async def test_spec_success_bodies_pass_silently(http_client, caplog):  # noqa: F811
    http_client._httpx.put = AsyncMock(return_value=_mock_resp(200, SPEC_PUT_OK))
    http_client._httpx.delete = AsyncMock(return_value=_mock_resp(200, SPEC_DELETE_OK))
    assert await http_client.replace_order("77", "1", "StopMarket", stop_price="1") == SPEC_PUT_OK
    assert await http_client.cancel_order("77") == SPEC_DELETE_OK
    assert "[ORDER-BODY-ERROR]" not in caplog.text


async def test_replace_body_error_is_logged_not_raised(http_client, caplog):  # noqa: F811
    body = {"Error": "FAILED", "Message": "Invalid stop price"}
    http_client._httpx.put = AsyncMock(return_value=_mock_resp(200, body))
    assert await http_client.replace_order("77", "1", "StopMarket", stop_price="1") == body
    assert "[ORDER-BODY-ERROR]" in caplog.text and "Invalid stop price" in caplog.text


async def test_cancel_body_error_is_logged_not_raised(http_client, caplog):  # noqa: F811
    body = {"Error": "FAILED", "Message": "Not an open order."}
    http_client._httpx.delete = AsyncMock(return_value=_mock_resp(200, body))
    assert await http_client.cancel_order("77") == body
    assert "[ORDER-BODY-ERROR]" in caplog.text and "Not an open order" in caplog.text


async def test_group_partial_cancels_landed_legs_and_rejects_every_leg():
    stub, cmd = _group_stub([False, False])
    stub._client.place_order_group = AsyncMock(
        side_effect=OrderGroupPartialException("leg 2 rejected", ["90"])
    )
    await C._submit_order_group(stub, cmd, "OCO", [{}, {}])
    stub._client.cancel_order.assert_awaited_once_with("90")
    assert stub.generate_order_rejected.call_count == 2
    stub.generate_order_accepted.assert_not_called()
    assert any("[GROUP-PARTIAL]" in str(c.args[0]) for c in stub._log.error.call_args_list)


async def test_replace_sends_only_spec_fields(http_client):  # noqa: F811
    http_client._httpx.put = AsyncMock(return_value=_mock_resp(200, {"OrderID": "77"}))
    await http_client.replace_order("77", "1", "StopMarket", stop_price="2000.5")
    assert http_client._httpx.put.call_args.kwargs["json"] == {
        "Quantity": "1",
        "StopPrice": "2000.5",
    }
