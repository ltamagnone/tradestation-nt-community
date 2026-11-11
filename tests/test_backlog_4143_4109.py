"""OPEN_POINTS 4.143 (replace answered 200 + Error body) and 4.109 (post-200 submit failures
are ambiguous, tolerant rate-limit headers)."""
from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from tests.test_execution import (
    _make_check_statuses_mock,
    _make_exec_mock,
    _make_modify_command_with_trigger,
)
from tests.test_http_client import _mock_resp, http_client  # noqa: F401 (fixture)
from tests.test_kit import make_market_order
from tests.test_late_accept_cancel import _group_stub
from tests.test_order_attribution import _build_submit_command, _make_submit_order_stub
from tradestation_nt_community import SUBMIT_AMBIGUOUS_REASON_PREFIX
from tradestation_nt_community.execution import TradeStationExecutionClient as C
from tradestation_nt_community.execution import _is_ambiguous_submit
from tradestation_nt_community.http.client import (
    OrderRejectedException,
    OrderResponseAmbiguousException,
    ReplaceBodyErrorException,
)
from nautilus_trader.model.objects import Price

LEG = {"AccountID": "SIM1", "Symbol": "GCZ26", "Quantity": "1", "OrderType": "StopMarket",
       "TradeAction": "Sell", "TimeInForce": {"Duration": "GTC"}, "StopPrice": "1"}


# ------------------------------------------------------------------ 4.143 replace body

async def test_modify_body_error_emits_nothing_and_records_the_amend(monkeypatch):
    import tradestation_nt_community.execution as ex
    monkeypatch.setattr(ex, "_mono", lambda: 0.0)
    cmd = _make_modify_command_with_trigger(trigger_price=21050.0)
    m = _make_exec_mock(cmd, ts_order_id="TS-STOP")
    m._cache.order.return_value.trigger_price = Price(21000.0, 2)
    m._client.replace_order = AsyncMock(
        side_effect=ReplaceBodyErrorException("TS-STOP", ["HTTP 400 Invalid stop price"]))
    await C._modify_order(m, cmd)
    # old code: OrderUpdated with the new trigger although TS kept the old one
    m.generate_order_updated.assert_not_called()
    # never an immediate reject either, even though the text holds "HTTP 4"
    m.generate_order_modify_rejected.assert_not_called()
    coid = cmd.client_order_id
    assert m._pending_modify_trigger_price[coid] == (Price(21050.0, 2), None)
    assert m._pending_amend_meta[coid].prev == (Price(21000.0, 2), None)
    assert not m._modify_inflight


async def _modify_then_poll(monkeypatch, broker_stop: str):
    import tradestation_nt_community.execution as ex
    clock = [0.0]
    monkeypatch.setattr(ex, "_mono", lambda: clock[0])
    cmd = _make_modify_command_with_trigger(trigger_price=21050.0)
    m = _make_exec_mock(cmd, ts_order_id="TS-STOP")
    m._cache.order.return_value.trigger_price = Price(21000.0, 2)
    m._client.replace_order = AsyncMock(
        side_effect=ReplaceBodyErrorException("TS-STOP", ["Invalid stop price"]))
    await C._modify_order(m, cmd)
    p = _make_check_statuses_mock(
        ts_orders=[{"OrderID": "TS-STOP", "Status": "OPN", "StopPrice": broker_stop}])
    p._pending_modify_trigger_price = m._pending_modify_trigger_price
    p._pending_amend_meta = m._pending_amend_meta
    for t in (25.0, 31.0):
        clock[0] = t
        await C._check_order_statuses(p)
    return cmd, p


async def test_body_error_broker_kept_old_price_is_modify_rejected(monkeypatch):
    cmd, p = await _modify_then_poll(monkeypatch, "21000.00")
    p.generate_order_modify_rejected.assert_called_once()
    assert p.generate_order_modify_rejected.call_args.kwargs["reason"].startswith(
        "modify recovery:")
    p.generate_order_updated.assert_not_called()
    assert cmd.client_order_id not in p._pending_modify_trigger_price


async def test_body_error_broker_has_new_price_is_updated(monkeypatch):
    cmd, p = await _modify_then_poll(monkeypatch, "21050.00")
    p.generate_order_updated.assert_called_once()
    assert p.generate_order_updated.call_args.kwargs["trigger_price"] == Price(21050.0, 2)
    p.generate_order_modify_rejected.assert_not_called()


async def test_modify_plain_4xx_still_rejected_at_once():
    cmd = _make_modify_command_with_trigger()
    m = _make_exec_mock(cmd)
    m._client.replace_order = AsyncMock(
        side_effect=Exception("Replace order failed (HTTP 400): Invalid stop price"))
    await C._modify_order(m, cmd)
    m.generate_order_modify_rejected.assert_called_once()


# ------------------------------------------------------------- 4.109 post-200 = ambiguous

class _BadJson:
    status_code = 200
    text = "<html>oops</html>"
    headers: dict = {}

    def json(self):
        raise json.JSONDecodeError("Expecting value", "<html>", 0)


@pytest.mark.parametrize("resp", [_BadJson(), _mock_resp(200, ["not", "a", "dict"])])
async def test_place_order_unreadable_200_is_ambiguous(http_client, resp):  # noqa: F811
    http_client._httpx.post = AsyncMock(return_value=resp)
    with pytest.raises(OrderResponseAmbiguousException):
        await http_client.place_order("SIM1", "GCZ26", "1", "Market", "Buy")


async def test_place_order_null_json_is_ambiguous(http_client):  # noqa: F811
    resp = _mock_resp(200, {})
    resp.json.return_value = None
    http_client._httpx.post = AsyncMock(return_value=resp)
    with pytest.raises(OrderResponseAmbiguousException):
        await http_client.place_order("SIM1", "GCZ26", "1", "Market", "Buy")


async def test_place_order_group_unreadable_200_is_ambiguous(http_client):  # noqa: F811
    http_client._httpx.post = AsyncMock(return_value=_BadJson())
    with pytest.raises(OrderResponseAmbiguousException):
        await http_client.place_order_group("OCO", [LEG, LEG])


async def test_group_no_leg_and_no_error_is_ambiguous(http_client):  # noqa: F811
    http_client._httpx.post = AsyncMock(return_value=_mock_resp(200, {"Orders": []}))
    with pytest.raises(OrderResponseAmbiguousException):
        await http_client.place_order_group("OCO", [LEG, LEG])


async def test_group_all_failed_with_errors_stays_a_plain_reject(http_client):  # noqa: F811
    body = {"Errors": [{"Error": "FAILED", "Message": "no margin"}]}
    http_client._httpx.post = AsyncMock(return_value=_mock_resp(200, body))
    with pytest.raises(OrderRejectedException):
        await http_client.place_order_group("OCO", [LEG, LEG])


async def test_place_order_5xx_stays_plain(http_client):  # noqa: F811
    http_client._httpx.post = AsyncMock(return_value=_mock_resp(503, {"x": 1}))
    with pytest.raises(Exception) as ei:
        await http_client.place_order("SIM1", "GCZ26", "1", "Market", "Buy")
    assert not _is_ambiguous_submit(ei.value)


@pytest.mark.parametrize("response", [{"Message": "ok"}, {"Orders": []}, {"Orders": "x"}])
async def test_submit_without_order_id_is_rejected_as_ambiguous(response):
    stub = _make_submit_order_stub()
    stub._client.place_order = AsyncMock(return_value=response)
    await C._submit_order(stub, _build_submit_command(make_market_order(tags=None)))
    reason = stub.generate_order_rejected.call_args.kwargs["reason"]
    assert reason.startswith(SUBMIT_AMBIGUOUS_REASON_PREFIX), reason
    stub.generate_order_accepted.assert_not_called()
    assert any("[SUBMIT-AMBIGUOUS]" in str(c.args[0]) for c in stub._log.error.call_args_list)


async def test_submit_unreadable_answer_reason_is_ambiguous():
    stub = _make_submit_order_stub()
    stub._client.place_order = AsyncMock(
        side_effect=OrderResponseAmbiguousException("unreadable"))
    await C._submit_order(stub, _build_submit_command(make_market_order(tags=None)))
    assert stub.generate_order_rejected.call_args.kwargs["reason"].startswith(
        SUBMIT_AMBIGUOUS_REASON_PREFIX)


async def test_submit_local_conversion_error_stays_plain():
    stub = _make_submit_order_stub()
    stub._convert_order_to_ts_format = MagicMock(side_effect=ValueError("bad order"))
    await C._submit_order(stub, _build_submit_command(make_market_order(tags=None)))
    stub._client.place_order.assert_not_awaited()
    assert stub.generate_order_rejected.call_args.kwargs["reason"] == "bad order"


async def test_group_unreadable_answer_rejects_every_leg_as_ambiguous():
    stub, cmd = _group_stub([False, False])
    stub._client.place_order_group = AsyncMock(
        side_effect=OrderResponseAmbiguousException("unreadable"))
    await C._submit_order_group(stub, cmd, "OCO", [{}, {}])
    reasons = [c.kwargs["reason"] for c in stub.generate_order_rejected.call_args_list]
    assert len(reasons) == 2 and all(
        r.startswith(SUBMIT_AMBIGUOUS_REASON_PREFIX) for r in reasons)


# ------------------------------------------------------ 4.109 tolerant rate-limit headers

async def test_bad_ratelimit_headers_never_raise(http_client, monkeypatch):  # noqa: F811
    slept = []

    async def fake_sleep(s):
        slept.append(s)
    monkeypatch.setattr("asyncio.sleep", fake_sleep)
    resp = _mock_resp(200, {"Orders": [{"OrderID": "9"}]})
    resp.headers = {"X-RateLimit-Remaining": "n/a", "X-RateLimit-Reset": "soon"}
    http_client._httpx.post = AsyncMock(return_value=resp)
    out = await http_client.place_order("SIM1", "GCZ26", "1", "Market", "Buy")
    assert out["Orders"][0]["OrderID"] == "9" and slept == []
    # remaining parses but reset does not: a short sleep, no raise
    resp.headers = {"X-RateLimit-Remaining": "2", "X-RateLimit-Reset": "garbage"}
    await http_client.place_order("SIM1", "GCZ26", "1", "Market", "Buy")
    assert len(slept) == 1 and slept[0] <= http_client._retry_delay_max_s


async def test_proactive_ratelimit_sleep_is_capped(http_client, monkeypatch):  # noqa: F811
    import time
    slept = []

    async def fake_sleep(s):
        slept.append(s)
    monkeypatch.setattr("asyncio.sleep", fake_sleep)
    resp = _mock_resp(200, {"Orders": []})
    resp.headers = {"X-RateLimit-Remaining": "1",
                    "X-RateLimit-Reset": str(int(time.time()) + 3600)}
    http_client._httpx.get = AsyncMock(return_value=resp)
    await http_client._request("GET", "https://x/y")
    assert slept == [http_client._retry_delay_max_s]


async def test_429_http_date_retry_after(http_client, monkeypatch):  # noqa: F811
    slept = []

    async def fake_sleep(s):
        slept.append(s)
    monkeypatch.setattr("asyncio.sleep", fake_sleep)
    r429 = _mock_resp(429, {})
    r429.headers = {"Retry-After": "Wed, 21 Oct 2026 07:28:00 GMT"}
    ok = _mock_resp(200, {"ok": 1})
    http_client._httpx.get = AsyncMock(side_effect=[r429, ok])
    assert (await http_client._request("GET", "https://x/y")) is ok
    assert slept == [http_client._retry_delay_initial_s]
