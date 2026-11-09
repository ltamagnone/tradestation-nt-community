"""Adapter 401 handling (OPEN_POINTS 4.107 + 4.108).

- ``force_refresh``: one refresh for N concurrent 401 callers (30 s window).
- ``_ensure_authenticated``: a failed proactive refresh keeps a still-valid token;
  a failure is shared for 5 s.
- ``_request``: a 401 is refreshed and resent once (any method); a second 401 is returned.
- SSE: a 401 always sleeps before reconnecting (never a zero-delay loop).
"""
import asyncio
import time
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from tradestation_nt_community.http.client import TradeStationHttpClient
from tradestation_nt_community.streaming.client import TradeStationStreamClient
from tests.test_http_client import _mock_resp


def _client(expires_in_s: float = 3600.0) -> TradeStationHttpClient:
    c = TradeStationHttpClient(
        client_id="c", client_secret="s", refresh_token="r", use_sandbox=True,
    )
    c._access_token = "old"
    c.token_expiry = datetime.now(tz=timezone.utc) + timedelta(seconds=expires_in_s)
    return c


def _auth_ok(token: str = "new") -> MagicMock:
    return _mock_resp(200, {"access_token": token, "expires_in": 1200})


# --------------------------------------------------------------------------- force_refresh

@pytest.mark.asyncio
async def test_concurrent_force_refresh_makes_one_auth_post():
    c = _client()

    async def slow_post(*a, **kw):
        await asyncio.sleep(0.01)
        return _auth_ok()

    c._httpx.post = AsyncMock(side_effect=slow_post)
    await asyncio.gather(*[c.force_refresh() for _ in range(8)])
    assert c._httpx.post.await_count == 1
    assert c.access_token == "new"


@pytest.mark.asyncio
async def test_force_refresh_after_window_refreshes_again():
    c = _client()
    c._httpx.post = AsyncMock(side_effect=[_auth_ok("t1"), _auth_ok("t2")])
    await c.force_refresh()
    c._last_refresh_ok_mono = time.monotonic() - 31.0
    await c.force_refresh()
    assert c._httpx.post.await_count == 2
    assert c.access_token == "t2"


@pytest.mark.asyncio
async def test_force_refresh_failure_raises_and_is_shared_for_5s():
    c = _client()
    c._httpx.post = AsyncMock(return_value=_mock_resp(401, None))
    with pytest.raises(Exception, match="authentication failed"):
        await c.force_refresh()
    # a second caller within 5 s gets the same error, no new POST
    with pytest.raises(Exception, match="authentication failed"):
        await c.force_refresh()
    assert c._httpx.post.await_count == 1
    assert c.access_token == "old"  # nothing to keep after a rejected token, but untouched
    # after 5 s a new attempt is made
    c._last_refresh_fail_mono = time.monotonic() - 6.0
    c._httpx.post = AsyncMock(return_value=_auth_ok())
    await c.force_refresh()
    assert c._httpx.post.await_count == 1
    assert c.access_token == "new"


# ------------------------------------------------------------------ _ensure_authenticated

@pytest.mark.asyncio
async def test_proactive_refresh_failure_keeps_valid_token(caplog):
    c = _client(expires_in_s=120.0)  # inside the 5-min window, still valid
    expiry = c.token_expiry
    c._httpx.post = AsyncMock(return_value=_mock_resp(401, None))
    with caplog.at_level("WARNING"):
        await c._ensure_authenticated()  # must not raise
    assert c.access_token == "old"
    assert c.token_expiry == expiry
    assert any("[AUTH] refresh failed, keeping the current token" in r.getMessage()
               for r in caplog.records)


@pytest.mark.asyncio
async def test_proactive_refresh_failure_expired_token_raises():
    c = _client(expires_in_s=-10.0)
    c._httpx.post = AsyncMock(return_value=_mock_resp(401, None))
    with pytest.raises(Exception, match="authentication failed"):
        await c._ensure_authenticated()


@pytest.mark.asyncio
async def test_second_caller_within_5s_makes_no_new_auth_post():
    c = _client(expires_in_s=120.0)
    c._httpx.post = AsyncMock(return_value=_mock_resp(401, None))
    await asyncio.gather(*[c._ensure_authenticated() for _ in range(5)])
    assert c._httpx.post.await_count == 1
    # also for the must-raise case
    c2 = _client(expires_in_s=-10.0)
    c2._httpx.post = AsyncMock(return_value=_mock_resp(401, None))
    res = await asyncio.gather(*[c2._ensure_authenticated() for _ in range(4)],
                               return_exceptions=True)
    assert all(isinstance(r, Exception) for r in res)
    assert c2._httpx.post.await_count == 1


@pytest.mark.asyncio
async def test_valid_token_outside_window_does_nothing():
    c = _client(expires_in_s=3600.0)
    c._httpx.post = AsyncMock()
    await c._ensure_authenticated()
    c._httpx.post.assert_not_awaited()


# ---------------------------------------------------------------------------- _request

@pytest.mark.asyncio
async def test_request_401_refreshes_once_and_resends():
    c = _client()
    c._httpx.post = AsyncMock(return_value=_auth_ok("fresh"))
    c._httpx.get = AsyncMock(side_effect=[_mock_resp(401, None), _mock_resp(200, {"ok": 1})])
    resp = await c._request("GET", "https://x/y")
    assert resp.status_code == 200
    assert c._httpx.post.await_count == 1
    assert c._httpx.get.await_count == 2
    assert c._httpx.get.await_args_list[0].kwargs["headers"]["Authorization"] == "Bearer old"
    assert c._httpx.get.await_args_list[1].kwargs["headers"]["Authorization"] == "Bearer fresh"


@pytest.mark.asyncio
async def test_request_two_401_returns_401_one_refresh():
    c = _client()
    c._httpx.post = AsyncMock(return_value=_auth_ok("fresh"))
    c._httpx.get = AsyncMock(return_value=_mock_resp(401, None))
    resp = await c._request("GET", "https://x/y")
    assert resp.status_code == 401
    assert c._httpx.post.await_count == 1
    assert c._httpx.get.await_count == 2


@pytest.mark.asyncio
async def test_request_401_refresh_failure_returns_the_401():
    c = _client()
    c._httpx.post = AsyncMock(return_value=_mock_resp(401, None))
    c._httpx.get = AsyncMock(return_value=_mock_resp(401, None))
    resp = await c._request("GET", "https://x/y")
    assert resp.status_code == 401
    assert c._httpx.get.await_count == 1  # no resend with a token that was just rejected


@pytest.mark.asyncio
async def test_post_order_401_resent_once_with_same_body():
    c = _client()
    c._httpx.post = AsyncMock(side_effect=[
        _mock_resp(401, None),      # the order POST, rejected before processing
        _auth_ok("fresh"),          # the auth POST
        _mock_resp(200, {"Orders": [{"OrderID": "1"}]}),  # the resend
    ])
    body = {"Symbol": "ESZ26", "Quantity": "1", "OrderConfirmID": "abc"}
    resp = await c._request("POST", "https://x/orderexecution/orders", json=body)
    assert resp.status_code == 200
    calls = c._httpx.post.await_args_list
    assert calls[0].kwargs["json"] == body and calls[2].kwargs["json"] == body
    assert calls[2].kwargs["headers"]["Authorization"] == "Bearer fresh"


@pytest.mark.asyncio
async def test_post_non_401_error_is_never_resent():
    c = _client()
    c._httpx.post = AsyncMock(return_value=_mock_resp(500, None))
    resp = await c._request("POST", "https://x/orders", json={"a": 1})
    assert resp.status_code == 500
    assert c._httpx.post.await_count == 1


@pytest.mark.asyncio
async def test_request_200_triggers_nothing_new():
    c = _client()
    c._httpx.post = AsyncMock()
    c._httpx.get = AsyncMock(return_value=_mock_resp(200, {}))
    await c._request("GET", "https://x/y")
    c._httpx.post.assert_not_awaited()


# -------------------------------------------------------------------------------- SSE

def _stream_mod_client(responses):
    idx = [-1]

    class _Resp:
        def __init__(self, i):
            self.status_code = responses[i][0]
            self._c = responses[i][1]

        async def __aenter__(self): return self
        async def __aexit__(self, *a): pass
        async def aread(self): return self._c if isinstance(self._c, bytes) else b""
        async def aiter_lines(self):
            for x in self._c:
                yield x

    class _C:
        def __init__(self, **kw): pass
        async def __aenter__(self): return self
        async def __aexit__(self, *a): pass
        def stream(self, *a, **kw):
            idx[0] += 1
            return _Resp(idx[0])

    return _C


async def _run(monkeypatch, responses, on_auth_error):
    from tradestation_nt_community.streaming import client as mod
    sc = TradeStationStreamClient(
        access_token_provider=lambda: "tok",
        base_url="https://mock.ts.com/v3",
        reconnect_delay_secs=0.01,
        on_auth_error=on_auth_error,
    )
    sleeps = []

    async def fake_sleep(s):
        sleeps.append(s)

    monkeypatch.setattr("asyncio.sleep", fake_sleep)
    monkeypatch.setattr(mod.httpx, "AsyncClient", _stream_mod_client(responses))
    async for _ in sc._stream("https://mock.ts.com/stream"):
        break
    return sleeps


@pytest.mark.asyncio
async def test_sse_401_forces_refresh_then_sleeps(monkeypatch):
    cb = AsyncMock()
    sleeps = await _run(monkeypatch, [(401, b"x"), (200, ['{"Symbol":"A"}'])], cb)
    cb.assert_awaited_once()
    assert sleeps == [pytest.approx(0.01)]


@pytest.mark.asyncio
async def test_sse_401_refresh_failure_also_sleeps(monkeypatch):
    cb = AsyncMock(side_effect=RuntimeError("down"))
    sleeps = await _run(monkeypatch, [(401, b"x"), (200, ['{"Symbol":"A"}'])], cb)
    cb.assert_awaited_once()
    assert sleeps == [pytest.approx(0.01)]


@pytest.mark.asyncio
async def test_sse_repeated_401_back_off(monkeypatch):
    cb = AsyncMock()
    sleeps = await _run(
        monkeypatch,
        [(401, b"x"), (401, b"x"), (401, b"x"), (200, ['{"Symbol":"A"}'])], cb)
    assert cb.await_count == 3
    assert sleeps == [pytest.approx(0.01), pytest.approx(0.02), pytest.approx(0.04)]


def test_wiring_uses_force_refresh():
    import inspect
    from tradestation_nt_community import data, execution
    for mod in (data, execution):
        src = inspect.getsource(mod)
        assert "on_auth_error=self._client.force_refresh" in src
        assert "on_auth_error=self._client._ensure_authenticated" not in src


@pytest.mark.asyncio
async def test_request_401_after_token_already_changed_resends_without_refresh():
    """Review M4: a 401 for a request sent with a token that another caller has since
    replaced is resent with the new token, without a further auth POST."""
    c = _client()
    c._httpx.post = AsyncMock(return_value=_auth_ok("unused"))

    async def _get(url, **kw):
        if c._httpx.get.await_count == 1:
            c._access_token = "rotated-by-other"   # another caller refreshed meanwhile
            return _mock_resp(401, None)
        return _mock_resp(200, {"ok": 1})

    c._httpx.get = AsyncMock(side_effect=_get)
    resp = await c._request("GET", "https://x/y")
    assert resp.status_code == 200
    assert c._httpx.post.await_count == 0
    assert c._httpx.get.await_args_list[1].kwargs["headers"]["Authorization"] == \
        "Bearer rotated-by-other"


@pytest.mark.asyncio
async def test_request_429_then_401_then_200(monkeypatch):
    """Review M6: a 429 counts as an attempt, the 401 resend does not."""
    monkeypatch.setattr(asyncio, "sleep", AsyncMock())
    c = _client()
    c._max_retries = 2
    c._httpx.post = AsyncMock(return_value=_auth_ok("fresh"))
    c._httpx.get = AsyncMock(side_effect=[
        _mock_resp(429, None), _mock_resp(401, None), _mock_resp(200, {"ok": 1})])
    resp = await c._request("GET", "https://x/y")
    assert resp.status_code == 200
    assert c._httpx.post.await_count == 1
    assert c._httpx.get.await_count == 3
