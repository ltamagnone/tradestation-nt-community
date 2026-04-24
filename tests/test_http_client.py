"""
Tests for TradeStationHttpClient (async, Phase 6).

All tests are async (``@pytest.mark.asyncio``).  The underlying httpx client
is mocked by replacing ``http_client._httpx.get`` / ``.post`` / etc. with
``AsyncMock`` instances — no network calls are made.
"""
import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from tradestation_nt_community.common.enums import TradeStationBarUnit
from tradestation_nt_community.http.client import (
    DuplicateOrderConfirmIdException,
    OrderRejectedException,
    TradeStationHttpClient,
)


RESOURCES_DIR = Path(__file__).parent / "resources"


def _load(filename: str) -> dict:
    return json.loads((RESOURCES_DIR / filename).read_text())


def _mock_resp(status: int = 200, data: dict | list | None = None) -> MagicMock:
    """Return a MagicMock that looks like an httpx Response."""
    resp = MagicMock()
    resp.status_code = status
    resp.text = str(data or "")
    resp.json.return_value = data if data is not None else {}
    resp.headers = {}  # _request() reads X-RateLimit-Remaining / Retry-After
    return resp


@pytest.fixture
def http_client():
    """
    Return a TradeStationHttpClient with a pre-set access token so
    tests never trigger real OAuth.  The _httpx attribute is the real
    httpx.AsyncClient; individual tests replace its methods with AsyncMock.
    """
    client = TradeStationHttpClient(
        client_id="test_client_id",
        client_secret="test_client_secret",
        refresh_token="test_refresh_token",
        use_sandbox=True,
    )
    # Pre-authenticate so _ensure_authenticated is a no-op
    from datetime import datetime, timedelta, timezone
    client._access_token = "test_access_token"
    client.token_expiry = datetime.now(tz=timezone.utc) + timedelta(hours=1)
    return client



def test_client_initialization_with_sandbox():
    """Sandbox URL contains 'sim-api'."""
    client = TradeStationHttpClient(
        client_id="c", client_secret="s", refresh_token="r", use_sandbox=True,
    )
    assert "sim-api" in client.base_url


def test_client_initialization_with_production():
    """Production URL does not contain 'sim-api'."""
    client = TradeStationHttpClient(
        client_id="c", client_secret="s", refresh_token="r", use_sandbox=False,
    )
    assert "api.tradestation.com" in client.base_url
    assert "sim-api" not in client.base_url


def test_base_url_rejects_non_tradestation_host():
    """Custom base_url to unknown host is rejected by default."""
    with pytest.raises(ValueError, match="base_url must be HTTPS"):
        TradeStationHttpClient(
            client_id="c", client_secret="s", refresh_token="r",
            base_url="https://evil.example.com/v3",
        )


def test_base_url_rejects_http_scheme():
    """Plain HTTP is rejected even for a valid TradeStation host."""
    with pytest.raises(ValueError, match="base_url must be HTTPS"):
        TradeStationHttpClient(
            client_id="c", client_secret="s", refresh_token="r",
            base_url="http://api.tradestation.com/v3",
        )


def test_base_url_accepts_tradestation_hosts():
    """Known TradeStation HTTPS hosts are accepted."""
    client = TradeStationHttpClient(
        client_id="c", client_secret="s", refresh_token="r",
        base_url="https://api.tradestation.com/v3",
    )
    assert client.base_url == "https://api.tradestation.com/v3"

    client2 = TradeStationHttpClient(
        client_id="c", client_secret="s", refresh_token="r",
        base_url="https://sim-api.tradestation.com/v3",
    )
    assert client2.base_url == "https://sim-api.tradestation.com/v3"


def test_base_url_allow_custom_bypasses_validation():
    """allow_custom_base_url=True skips hostname check."""
    client = TradeStationHttpClient(
        client_id="c", client_secret="s", refresh_token="r",
        base_url="http://localhost:8080/mock",
        allow_custom_base_url=True,
    )
    assert client.base_url == "http://localhost:8080/mock"


@pytest.mark.asyncio
async def test_get_symbol_details_returns_parsed_data(http_client):
    """get_symbol_details unwraps the Symbols array."""
    sample = _load("symbol_detail_future.json")
    http_client._httpx.get = AsyncMock(
        return_value=_mock_resp(200, {"Symbols": [sample], "Errors": []})
    )
    result = await http_client.get_symbol_details("GCG25")
    assert result["Symbol"] == sample["Symbol"]
    assert result["AssetType"] == "FUTURE"


@pytest.mark.asyncio
async def test_get_symbol_details_handles_empty_response(http_client):
    """Empty Symbols array returns {}."""
    http_client._httpx.get = AsyncMock(
        return_value=_mock_resp(200, {"Symbols": [], "Errors": ["No match"]})
    )
    result = await http_client.get_symbol_details("INVALID")
    assert result == {}



@pytest.mark.asyncio
async def test_get_bars_returns_bar_data(http_client):
    """get_bars returns the Bars list from the response."""
    bars_data = _load("bars_response.json")
    http_client._httpx.get = AsyncMock(return_value=_mock_resp(200, bars_data))
    result = await http_client.get_bars(
        symbol="GCG25",
        interval="1",
        unit=TradeStationBarUnit.MINUTE,
        barsback=3,
    )
    assert len(result) == 3
    assert result[0]["Open"] == 2050.5


@pytest.mark.asyncio
async def test_get_bars_with_date_range(http_client):
    """get_bars passes firstdate/lastdate when barsback is not given."""
    captured = {}
    bars_data = _load("bars_response.json")

    async def mock_get(url, headers=None, params=None, **kw):
        captured["params"] = params
        return _mock_resp(200, bars_data)

    http_client._httpx.get = mock_get
    await http_client.get_bars(
        symbol="GCG25",
        interval="1",
        unit=TradeStationBarUnit.MINUTE,
        first_date="01-01-2025",
        last_date="01-31-2025",
    )
    assert "firstdate" in captured["params"]
    assert "lastdate" in captured["params"]



@pytest.mark.asyncio
async def test_get_quotes_returns_parsed_data(http_client):
    """get_quotes returns quote list from Quotes key."""
    quote = _load("quote_response.json")
    http_client._httpx.get = AsyncMock(return_value=_mock_resp(200, {"Quotes": [quote]}))
    result = await http_client.get_quotes("GCG25")
    assert len(result) == 1
    assert result[0]["Bid"] == pytest.approx(2050.5, rel=1e-4)
    assert result[0]["Ask"] == pytest.approx(2050.7, rel=1e-4)
    assert result[0]["Last"] == pytest.approx(2050.6, rel=1e-4)


@pytest.mark.asyncio
async def test_get_quotes_uses_correct_url(http_client):
    """get_quotes builds the correct URL with the symbol."""
    captured = {}

    async def mock_get(url, headers=None, **kw):
        captured["url"] = url
        return _mock_resp(200, {"Quotes": []})

    http_client._httpx.get = mock_get
    await http_client.get_quotes("GCJ26,ESM26")
    assert "GCJ26,ESM26" in captured["url"]
    assert "quotes" in captured["url"].lower()


@pytest.mark.asyncio
async def test_get_quotes_raises_on_error(http_client):
    """get_quotes raises on non-200 status."""
    http_client._httpx.get = AsyncMock(return_value=_mock_resp(403, None))
    with pytest.raises(Exception, match="Get quotes failed"):
        await http_client.get_quotes("GCJ26")



@pytest.mark.asyncio
async def test_http_error_handling_401(http_client):
    """401 response raises an exception."""
    http_client._httpx.get = AsyncMock(return_value=_mock_resp(401, None))
    with pytest.raises(Exception):
        await http_client.get_symbol_details("GCG25")


@pytest.mark.asyncio
async def test_http_error_handling_404(http_client):
    """404 response raises an exception."""
    http_client._httpx.get = AsyncMock(return_value=_mock_resp(404, None))
    with pytest.raises(Exception):
        await http_client.get_bars("FAKE", "1", TradeStationBarUnit.MINUTE)


@pytest.mark.asyncio
async def test_http_error_handling_500(http_client):
    """500 response raises an exception."""
    http_client._httpx.get = AsyncMock(return_value=_mock_resp(500, None))
    with pytest.raises(Exception):
        await http_client.get_symbol_details("GCG25")



@pytest.mark.asyncio
async def test_authentication_headers_included(http_client):
    """_get_headers returns an Authorization Bearer header."""
    headers = await http_client._get_headers()
    assert "Authorization" in headers
    assert "Bearer" in headers["Authorization"]



@pytest.mark.asyncio
async def test_place_order_group_returns_response(http_client):
    """place_order_group returns the parsed JSON response."""
    response_data = {
        "OrderGroupId": "GRP-001",
        "Type": "OCO",
        "Orders": [
            {"OrderID": "TS-010", "Status": "OPN"},
            {"OrderID": "TS-011", "Status": "OPN"},
        ],
    }
    http_client._httpx.post = AsyncMock(return_value=_mock_resp(200, response_data))

    orders = [
        {"AccountID": "SIM001", "Symbol": "GCJ26", "Quantity": "1",
         "OrderType": "StopMarket", "TradeAction": "Sell",
         "TimeInForce": {"Duration": "GTC"}, "StopPrice": "3300.0"},
        {"AccountID": "SIM001", "Symbol": "GCJ26", "Quantity": "1",
         "OrderType": "Limit", "TradeAction": "Sell",
         "TimeInForce": {"Duration": "GTC"}, "LimitPrice": "3500.0"},
    ]
    result = await http_client.place_order_group(group_type="OCO", orders=orders)

    assert result["OrderGroupId"] == "GRP-001"
    assert len(result["Orders"]) == 2


@pytest.mark.asyncio
async def test_place_order_group_uses_correct_url(http_client):
    """place_order_group posts to /orderexecution/ordergroups."""
    captured = {}

    async def mock_post(url, headers=None, json=None, **kw):
        captured["url"] = url
        captured["payload"] = json
        return _mock_resp(200, {"OrderGroupId": "G-1", "Orders": []})

    http_client._httpx.post = mock_post
    await http_client.place_order_group("OCO", [])

    assert "ordergroups" in captured["url"]
    assert captured["payload"]["Type"] == "OCO"


@pytest.mark.asyncio
async def test_place_order_group_raises_on_error(http_client):
    """place_order_group raises on non-200 status."""
    http_client._httpx.post = AsyncMock(return_value=_mock_resp(400, None))
    with pytest.raises(Exception, match="Place order group failed"):
        await http_client.place_order_group("OCO", [])


# =============================================================================
# Refresh-token rotation
# =============================================================================

class TestRefreshTokenRotation:
    """_refresh_access_token updates self._refresh_token when provider rotates it."""

    @pytest.mark.asyncio
    async def test_rotated_token_is_stored(self):
        """If the auth response contains refresh_token, it is saved in memory."""
        client = TradeStationHttpClient(
            client_id="c", client_secret="s", refresh_token="old_refresh",
            use_sandbox=True,
        )
        client._httpx.post = AsyncMock(return_value=_mock_resp(200, {
            "access_token": "new_access",
            "expires_in": 1200,
            "refresh_token": "new_refresh",
        }))
        await client._refresh_access_token()
        assert client._refresh_token == "new_refresh"
        assert client._access_token == "new_access"

    @pytest.mark.asyncio
    async def test_no_rotation_leaves_token_unchanged(self):
        """If the auth response omits refresh_token, the stored token is unchanged."""
        client = TradeStationHttpClient(
            client_id="c", client_secret="s", refresh_token="original",
            use_sandbox=True,
        )
        client._httpx.post = AsyncMock(return_value=_mock_resp(200, {
            "access_token": "new_access",
            "expires_in": 1200,
        }))
        await client._refresh_access_token()
        assert client._refresh_token == "original"

    @pytest.mark.asyncio
    async def test_rotation_callback_is_invoked(self):
        """set_token_rotation_callback fires with the new token on rotation."""
        client = TradeStationHttpClient(
            client_id="c", client_secret="s", refresh_token="old",
            use_sandbox=True,
        )
        received = []
        client.set_token_rotation_callback(received.append)
        client._httpx.post = AsyncMock(return_value=_mock_resp(200, {
            "access_token": "a", "expires_in": 1200, "refresh_token": "rotated",
        }))
        await client._refresh_access_token()
        assert received == ["rotated"]

    @pytest.mark.asyncio
    async def test_callback_not_called_without_rotation(self):
        """Callback is not invoked when the auth response has no refresh_token."""
        client = TradeStationHttpClient(
            client_id="c", client_secret="s", refresh_token="stable",
            use_sandbox=True,
        )
        received = []
        client.set_token_rotation_callback(received.append)
        client._httpx.post = AsyncMock(return_value=_mock_resp(200, {
            "access_token": "a", "expires_in": 1200,
        }))
        await client._refresh_access_token()
        assert received == []

    @pytest.mark.asyncio
    async def test_callback_exception_does_not_raise(self):
        """A failing callback is swallowed — auth still succeeds."""
        client = TradeStationHttpClient(
            client_id="c", client_secret="s", refresh_token="old",
            use_sandbox=True,
        )
        client.set_token_rotation_callback(lambda t: (_ for _ in ()).throw(RuntimeError("disk full")))
        client._httpx.post = AsyncMock(return_value=_mock_resp(200, {
            "access_token": "a", "expires_in": 1200, "refresh_token": "new",
        }))
        await client._refresh_access_token()  # must not raise
        assert client._access_token == "a"

    @pytest.mark.asyncio
    async def test_auth_retries_on_5xx_then_succeeds(self, monkeypatch):
        """5xx from auth endpoint is retried; success on second attempt."""
        async def mock_sleep(secs): pass
        monkeypatch.setattr("asyncio.sleep", mock_sleep)

        client = TradeStationHttpClient(
            client_id="c", client_secret="s", refresh_token="r", use_sandbox=True,
        )
        server_err = _mock_resp(503, None)
        success = _mock_resp(200, {"access_token": "tok", "expires_in": 1200})
        client._httpx.post = AsyncMock(side_effect=[server_err, success])
        await client._refresh_access_token()
        assert client._access_token == "tok"

    @pytest.mark.asyncio
    async def test_auth_does_not_retry_on_401(self, monkeypatch):
        """401 from auth endpoint is not retried — credential problem, not transient."""
        sleep_calls = []
        async def mock_sleep(secs): sleep_calls.append(secs)
        monkeypatch.setattr("asyncio.sleep", mock_sleep)

        client = TradeStationHttpClient(
            client_id="c", client_secret="s", refresh_token="dead", use_sandbox=True,
        )
        client._httpx.post = AsyncMock(return_value=_mock_resp(401, None))
        with pytest.raises(Exception, match="authentication failed"):
            await client._refresh_access_token()
        assert sleep_calls == [], "401 must not trigger retry sleep"
        assert client._httpx.post.call_count == 1


# =============================================================================
# Rate-limit handling via _request()
# =============================================================================

class TestRateLimitHandling:
    """_request() handles HTTP 429 with Retry-After sleep and retries."""

    @pytest.mark.asyncio
    async def test_429_retries_after_retry_after_header(self, http_client, monkeypatch):
        """429 with Retry-After causes one sleep then a successful retry."""
        sleep_calls = []

        async def mock_sleep(secs):
            sleep_calls.append(secs)

        monkeypatch.setattr("asyncio.sleep", mock_sleep)

        rate_limited = _mock_resp(429, None)
        rate_limited.headers = {"Retry-After": "2.5"}
        success = _mock_resp(200, {"Quotes": [{"Bid": 3350.0}]})

        http_client._httpx.get = AsyncMock(side_effect=[rate_limited, success])
        result = await http_client.get_quotes("GCJ26")

        assert result[0]["Bid"] == 3350.0
        assert len(sleep_calls) == 1
        assert sleep_calls[0] == pytest.approx(2.5)

    @pytest.mark.asyncio
    async def test_429_uses_initial_delay_when_no_retry_after(self, http_client, monkeypatch):
        """429 without Retry-After falls back to retry_delay_initial_s."""
        sleep_calls = []

        async def mock_sleep(secs):
            sleep_calls.append(secs)

        monkeypatch.setattr("asyncio.sleep", mock_sleep)

        rate_limited = _mock_resp(429, None)
        rate_limited.headers = {}
        success = _mock_resp(200, {"Quotes": []})

        http_client._httpx.get = AsyncMock(side_effect=[rate_limited, success])
        await http_client.get_quotes("GCJ26")

        assert sleep_calls[0] == pytest.approx(http_client._retry_delay_initial_s)

    @pytest.mark.asyncio
    async def test_429_exhausts_retries_and_raises(self, http_client, monkeypatch):
        """Persistent 429 across all retries still raises the downstream error."""
        async def mock_sleep(secs): pass
        monkeypatch.setattr("asyncio.sleep", mock_sleep)

        rate_limited = _mock_resp(429, None)
        rate_limited.headers = {"Retry-After": "0.01"}

        http_client._max_retries = 2
        http_client._httpx.get = AsyncMock(return_value=rate_limited)

        with pytest.raises(Exception, match="Get quotes failed"):
            await http_client.get_quotes("GCJ26")

    @pytest.mark.asyncio
    async def test_retry_after_capped_at_max_delay(self, http_client, monkeypatch):
        """Retry-After larger than retry_delay_max_s is capped."""
        sleep_calls = []

        async def mock_sleep(secs):
            sleep_calls.append(secs)

        monkeypatch.setattr("asyncio.sleep", mock_sleep)

        rate_limited = _mock_resp(429, None)
        rate_limited.headers = {"Retry-After": "99999"}
        success = _mock_resp(200, {"Quotes": []})

        http_client._httpx.get = AsyncMock(side_effect=[rate_limited, success])
        await http_client.get_quotes("GCJ26")

        assert sleep_calls[0] <= http_client._retry_delay_max_s

    @pytest.mark.asyncio
    async def test_near_exhaustion_triggers_proactive_sleep(self, http_client, monkeypatch):
        """X-RateLimit-Remaining < 5 causes a proactive sleep before returning."""
        import time as time_mod
        sleep_calls = []

        async def mock_sleep(secs):
            sleep_calls.append(secs)

        monkeypatch.setattr("asyncio.sleep", mock_sleep)
        reset_epoch = int(time_mod.time()) + 2
        low_remaining = _mock_resp(200, {"Quotes": [{"Bid": 3350.0}]})
        low_remaining.headers = {"X-RateLimit-Remaining": "3", "X-RateLimit-Reset": str(reset_epoch)}

        http_client._httpx.get = AsyncMock(return_value=low_remaining)
        result = await http_client.get_quotes("GCJ26")

        assert result[0]["Bid"] == 3350.0
        assert len(sleep_calls) == 1
        assert sleep_calls[0] > 0

    @pytest.mark.asyncio
    async def test_normal_response_skips_sleep(self, http_client, monkeypatch):
        """200 with healthy rate-limit headers causes no sleep."""
        sleep_calls = []

        async def mock_sleep(secs):
            sleep_calls.append(secs)

        monkeypatch.setattr("asyncio.sleep", mock_sleep)

        ok = _mock_resp(200, {"Quotes": [{"Bid": 3350.0}]})
        ok.headers = {"X-RateLimit-Remaining": "95", "X-RateLimit-Reset": "0"}

        http_client._httpx.get = AsyncMock(return_value=ok)
        await http_client.get_quotes("GCJ26")

        assert sleep_calls == []


# =============================================================================
# place_order() — OrderConfirmId injection and dedup protocol (Group A)
# =============================================================================

def _dedup_ack_body(confirm_id: str | None = None) -> dict:
    """Return a TS duplicate-acknowledge response body."""
    msg = "Order not placed because a unique orderconfirmid already exists."
    return {"Orders": [{"Error": "FAILED", "Message": msg, "OrderID": ""}]}


class TestPlaceOrderConfirmId:
    """place_order() injects OrderConfirmId and handles all dedup paths."""

    @pytest.mark.asyncio
    async def test_place_order_injects_order_confirm_id_when_not_supplied(self, http_client):
        """POST body contains auto-generated OrderConfirmId — exactly 22 hex chars."""
        captured = {}

        async def mock_post(url, headers=None, json=None, **kw):
            captured["body"] = json
            return _mock_resp(200, {"Orders": [{"OrderID": "111", "Message": "Sent order"}]})

        http_client._httpx.post = mock_post
        await http_client.place_order(
            account_id="SIM001", symbol="GCJ26", quantity="1",
            order_type="Market", trade_action="Buy",
        )

        body = captured["body"]
        assert "OrderConfirmId" in body
        cid = body["OrderConfirmId"]
        assert len(cid) == 22
        assert all(c in "0123456789abcdef" for c in cid)

    @pytest.mark.asyncio
    async def test_place_order_uses_supplied_order_confirm_id(self, http_client):
        """Caller-supplied order_confirm_id is placed in POST body verbatim."""
        custom_id = "my-custom-id-abc12345"
        captured = {}

        async def mock_post(url, headers=None, json=None, **kw):
            captured["body"] = json
            return _mock_resp(200, {"Orders": [{"OrderID": "222", "Message": "Sent order"}]})

        http_client._httpx.post = mock_post
        await http_client.place_order(
            account_id="SIM001", symbol="GCJ26", quantity="1",
            order_type="Market", trade_action="Buy",
            order_confirm_id=custom_id,
        )

        assert captured["body"]["OrderConfirmId"] == custom_id

    @pytest.mark.asyncio
    async def test_place_order_normal_success_returns_response_as_is(self, http_client):
        """Normal 200 success response is returned unchanged."""
        expected = {"Orders": [{"Message": "Sent order", "OrderID": "111"}]}
        http_client._httpx.post = AsyncMock(return_value=_mock_resp(200, expected))

        result = await http_client.place_order(
            account_id="SIM001", symbol="GCJ26", quantity="1",
            order_type="Market", trade_action="Buy",
        )

        assert result == expected

    @pytest.mark.asyncio
    async def test_place_order_duplicate_ack_resolves_via_get_orders(self, http_client):
        """Dedup-ack: GET /orders lookup by confirm_id resolves to real OrderID."""
        custom_id = "dedup-confirm-id-abcde"

        dedup_body = _dedup_ack_body()
        get_orders_response = _mock_resp(200, {
            "Orders": [{"OrderID": "real-222", "OrderConfirmId": custom_id}]
        })

        post_resp = _mock_resp(200, dedup_body)
        http_client._httpx.post = AsyncMock(return_value=post_resp)
        http_client._httpx.get = AsyncMock(return_value=get_orders_response)

        result = await http_client.place_order(
            account_id="SIM001", symbol="GCJ26", quantity="1",
            order_type="Market", trade_action="Buy",
            order_confirm_id=custom_id,
        )

        assert "Orders" in result
        assert result["Orders"][0]["OrderID"] == "real-222"
        assert result["Orders"][0]["Message"].startswith("Dedup acknowledged")

    @pytest.mark.asyncio
    async def test_place_order_duplicate_ack_original_not_found_raises(self, http_client):
        """Dedup-ack but GET /orders returns empty list → DuplicateOrderConfirmIdException."""
        dedup_body = _dedup_ack_body()
        post_resp = _mock_resp(200, dedup_body)
        get_orders_resp = _mock_resp(200, {"Orders": []})

        http_client._httpx.post = AsyncMock(return_value=post_resp)
        http_client._httpx.get = AsyncMock(return_value=get_orders_resp)

        with pytest.raises(DuplicateOrderConfirmIdException):
            await http_client.place_order(
                account_id="SIM001", symbol="GCJ26", quantity="1",
                order_type="Market", trade_action="Buy",
            )

    @pytest.mark.asyncio
    async def test_place_order_non_dedup_body_error_raises_OrderRejectedException(self, http_client):
        """Non-dedup FAILED body raises OrderRejectedException; GET /orders not called."""
        rejection_body = {
            "Orders": [{"Error": "FAILED", "Message": "Insufficient buying power", "OrderID": "reject-999"}]
        }
        post_resp = _mock_resp(200, rejection_body)
        get_mock = AsyncMock()

        http_client._httpx.post = AsyncMock(return_value=post_resp)
        http_client._httpx.get = get_mock

        with pytest.raises(OrderRejectedException) as exc_info:
            await http_client.place_order(
                account_id="SIM001", symbol="GCJ26", quantity="1",
                order_type="Market", trade_action="Buy",
            )

        assert "Insufficient buying power" in exc_info.value.ts_message
        get_mock.assert_not_called()

    @pytest.mark.asyncio
    async def test_place_order_http_5xx_still_raises(self, http_client):
        """HTTP 500 raises a generic exception (pre-existing behavior)."""
        http_client._httpx.post = AsyncMock(return_value=_mock_resp(500, None))

        with pytest.raises(Exception, match="Place order failed"):
            await http_client.place_order(
                account_id="SIM001", symbol="GCJ26", quantity="1",
                order_type="Market", trade_action="Buy",
            )


# =============================================================================
# Exception shape tests (Group C)
# =============================================================================

class TestExceptionShape:
    """DuplicateOrderConfirmIdException and OrderRejectedException expose correct attrs."""

    def test_DuplicateOrderConfirmIdException_exposes_confirm_id_and_message(self):
        """.confirm_id and .message are accessible on the exception."""
        exc = DuplicateOrderConfirmIdException(
            message="TS duplicate message", confirm_id="abc123"
        )
        assert exc.confirm_id == "abc123"
        assert exc.message == "TS duplicate message"

    def test_OrderRejectedException_exposes_ts_message(self):
        """.ts_message is accessible on the exception."""
        exc = OrderRejectedException("Insufficient buying power")
        assert exc.ts_message == "Insufficient buying power"
