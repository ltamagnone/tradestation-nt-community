# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# -------------------------------------------------------------------------------------------------
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

from nautilus_tradestation.common.enums import TradeStationBarUnit
from nautilus_tradestation.http.client import TradeStationHttpClient


RESOURCES_DIR = Path(__file__).parent / "resources"


def _load(filename: str) -> dict:
    return json.loads((RESOURCES_DIR / filename).read_text())


def _mock_resp(status: int = 200, data: dict | list | None = None) -> MagicMock:
    """Return a MagicMock that looks like an httpx Response."""
    resp = MagicMock()
    resp.status_code = status
    resp.text = str(data or "")
    resp.json.return_value = data if data is not None else {}
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
    from datetime import datetime, timedelta
    client.access_token = "test_access_token"
    client.token_expiry = datetime.utcnow() + timedelta(hours=1)
    return client


# ---------------------------------------------------------------------------
# Initialisation
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# get_symbol_details
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# get_bars
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# get_quotes
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Authentication headers
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_authentication_headers_included(http_client):
    """_get_headers returns an Authorization Bearer header."""
    headers = await http_client._get_headers()
    assert "Authorization" in headers
    assert "Bearer" in headers["Authorization"]


# ---------------------------------------------------------------------------
# place_order_group (Phase 8)
# ---------------------------------------------------------------------------

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
