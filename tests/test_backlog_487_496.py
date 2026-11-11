"""OPEN_POINTS 4.87 (reads refresh the token before an order write would) and 4.96
(last X-RateLimit-* stashed by _request, printed by [STATUS-POLL])."""
from __future__ import annotations

import time
from unittest.mock import AsyncMock, MagicMock

import pytest

from tests.test_auth_401_4107 import _auth_ok, _client
from tests.test_http_client import _mock_resp
from tradestation_nt_community.execution import TradeStationExecutionClient as C


# ------------------------------------------------------------------------------ 4.87

async def test_read_refreshes_with_six_minutes_left():
    c = _client(expires_in_s=6 * 60)
    c._httpx.post = AsyncMock(return_value=_auth_ok("new"))
    c._httpx.get = AsyncMock(return_value=_mock_resp(200, {}))
    await c._request("GET", "https://x/orders")
    c._httpx.post.assert_awaited_once()          # old code: no refresh above 5 min
    assert c._httpx.get.call_args.kwargs["headers"]["Authorization"] == "Bearer new"


async def test_keepalive_refreshes_with_six_minutes_left():
    c = _client(expires_in_s=6 * 60)
    c._httpx.post = AsyncMock(return_value=_auth_ok("new"))
    await c._ensure_authenticated()              # the 10-min keepalive's call
    c._httpx.post.assert_awaited_once()


async def test_order_write_with_six_minutes_left_does_not_refresh():
    c = _client(expires_in_s=6 * 60)
    c._httpx.post = AsyncMock(return_value=_mock_resp(200, {"Orders": [{"OrderID": "1"}]}))
    await c._request("POST", "https://x/orderexecution/orders", json={})
    # only the order POST, no auth POST in front of it
    assert c._httpx.post.await_count == 1
    assert "orderexecution" in c._httpx.post.call_args.args[0]


async def test_order_write_inside_five_minutes_still_refreshes():
    c = _client(expires_in_s=4 * 60)
    c._httpx.post = AsyncMock(side_effect=[_auth_ok("new"),
                                           _mock_resp(200, {"Orders": [{"OrderID": "1"}]})])
    await c._request("POST", "https://x/orderexecution/orders", json={})
    assert c._httpx.post.await_count == 2


async def test_read_outside_seven_minutes_does_nothing():
    c = _client(expires_in_s=8 * 60)
    c._httpx.post = AsyncMock()
    await c._ensure_authenticated()
    c._httpx.post.assert_not_awaited()


# ------------------------------------------------------------------------------ 4.96

async def test_request_stashes_last_ratelimit_headers():
    c = _client()
    resp = _mock_resp(200, {})
    resp.headers = {"X-RateLimit-Remaining": "247", "X-RateLimit-Limit": "250",
                    "X-RateLimit-Reset": "1794413953"}
    c._httpx.get = AsyncMock(return_value=resp)
    assert c.last_rate_limit is None
    await c._request("GET", "https://x/orders")
    rl = c.last_rate_limit
    assert rl[:3] == ("247", "250", "1794413953") and rl[3] <= time.monotonic()
    # an answer without the headers keeps the previous values
    c._httpx.get = AsyncMock(return_value=_mock_resp(200, {}))
    await c._request("GET", "https://x/orders")
    assert c.last_rate_limit[:3] == ("247", "250", "1794413953")


def _poll_stub(rl):
    stub = MagicMock()
    stub._last_status_poll_log_s = -1e9
    stub._STATUS_POLL_LOG_EVERY_S = 600.0
    stub._status_safety_poll_secs = 30.0
    stub._client = MagicMock()
    stub._client.last_rate_limit = rl
    return stub


def test_status_poll_line_carries_ratelimit():
    stub = _poll_stub(("247", "250", "1794413953", time.monotonic() - 3))
    C._log_status_poll(stub, 12, 85.0)
    line = stub._log.info.call_args.args[0]
    assert line.startswith("[STATUS-POLL] orders=12 read_ms=85 every=30s ")
    assert ("ratelimit_remaining=247 ratelimit_limit=250 ratelimit_reset=1794413953 "
            "ratelimit_age_s=3") in line


def test_status_poll_line_without_ratelimit():
    stub = _poll_stub(None)
    C._log_status_poll(stub, 3, 10.0)
    assert stub._log.info.call_args.args[0] == (
        "[STATUS-POLL] orders=3 read_ms=10 every=30s ratelimit=n/a")
