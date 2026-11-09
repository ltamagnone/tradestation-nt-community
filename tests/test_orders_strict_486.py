"""Strict /orders paging and /historicalorders reads (heal-async Task 1)."""
from __future__ import annotations

import asyncio
import logging
from unittest.mock import AsyncMock, MagicMock

import pytest

from tradestation_nt_community.http.client import BrokerReadError, TradeStationHttpClient

LOGGER = "tradestation_nt_community.http.client"


def _resp(data) -> MagicMock:
    r = MagicMock()
    r.status_code = 200
    r.text = str(data)
    r.json.return_value = data
    r.headers = {}
    return r


@pytest.fixture
def client():
    from datetime import datetime, timedelta, timezone
    c = TradeStationHttpClient(client_id="i", client_secret="s", refresh_token="r",
                               use_sandbox=True)
    c._access_token = "t"
    c.token_expiry = datetime.now(tz=timezone.utc) + timedelta(hours=1)
    return c


def _endless(client):
    n = {"i": 0}

    async def _get(url, **kw):
        n["i"] += 1
        return _resp({"Orders": [{"OrderID": str(n["i"])}], "NextToken": f"t{n['i']}"})

    client._httpx.get = AsyncMock(side_effect=_get)
    return n


def test_get_orders_strict_raises_at_page_cap(client):
    n = _endless(client)
    with pytest.raises(BrokerReadError):
        asyncio.run(client.get_orders("A1", strict=True))
    assert n["i"] == 20


def test_get_orders_default_still_truncates(client, caplog):
    _endless(client)
    with caplog.at_level(logging.ERROR, logger=LOGGER):
        out = asyncio.run(client.get_orders("A1"))
    assert len(out) == 20
    assert "[ORDERS-PAGE-CAP]" in caplog.text


def test_get_historical_orders_pages_and_since(client):
    client._httpx.get = AsyncMock(side_effect=[
        _resp({"Orders": [{"OrderID": "1"}], "NextToken": "tok"}),
        _resp({"Orders": [{"OrderID": "2"}]}),
    ])
    out = asyncio.run(client.get_historical_orders("A1", "11-07-2026"))
    assert [o["OrderID"] for o in out] == ["1", "2"]
    c1, c2 = client._httpx.get.call_args_list
    assert c1.args[0].endswith("/brokerage/accounts/A1/historicalorders")
    assert c1.kwargs["params"]["since"] == "11-07-2026"
    assert "nextToken" not in c1.kwargs["params"]
    assert c2.kwargs["params"]["nextToken"] == "tok"
    assert c2.kwargs["params"]["since"] == "11-07-2026"


def test_get_historical_orders_cap_raises(client):
    _endless(client)
    with pytest.raises(BrokerReadError):
        asyncio.run(client.get_historical_orders("A1", "11-07-2026"))


def test_get_historical_orders_errors_body_raises(client, monkeypatch):
    monkeypatch.setattr(TradeStationHttpClient, "_READ_RETRY_DELAYS", (0.0, 0.0))
    client._httpx.get = AsyncMock(return_value=_resp(
        {"Errors": [{"AccountID": "A1", "Error": "ServiceUnavailable"}]}))
    with pytest.raises(BrokerReadError):
        asyncio.run(client.get_historical_orders("A1", "11-07-2026"))
