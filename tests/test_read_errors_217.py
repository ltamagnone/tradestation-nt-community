"""§217: a TradeStation read that fails must never look like "flat / no orders".

Incident 2026-10-19 15:35:08 CEST: ``GET /orders?status=Open`` answered HTTP 200 with
``{"Errors":[{"AccountID":"SIM2736556F","Error":"ServiceUnavailable"}]}`` and no ``Orders``
key. The adapter returned ``[]`` and the invariant check reported all 9 positions NAKED.

Rule: every read retries briefly (3 attempts, sleeps 0.5 s then 1.0 s) and then raises
``BrokerReadError``. A genuine empty reply (``{"Orders": []}``) stays ``[]``. Errors scoped to one
order (``get_orders_by_id`` NotFound) or one symbol (``get_quotes``) are data, not a failure.

Sync tests driving coroutines with ``asyncio.run`` so they run with or without pytest-asyncio.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock, MagicMock

import pytest

import tradestation_nt_community
from tradestation_nt_community.common.enums import TradeStationBarUnit
from tradestation_nt_community.execution import TradeStationExecutionClient as C
from tradestation_nt_community.http.client import BrokerReadError, TradeStationHttpClient

LOGGER = "tradestation_nt_community.http.client"
INCIDENT = {"Errors": [{"AccountID": "SIM2736556F", "Error": "ServiceUnavailable"}]}
O1 = {"OrderID": "1", "Status": "OPN"}


_JSON_NULL = object()  # a body that parses to JSON ``null``


def _resp(status: int = 200, data=None, *, bad_json: bool = False) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status
    resp.text = "null" if data is _JSON_NULL else str(data or "")
    if bad_json:
        resp.json.side_effect = ValueError("Expecting value")
    elif data is _JSON_NULL:
        resp.json.return_value = None
    else:
        resp.json.return_value = data if data is not None else {}
    resp.headers = {}
    return resp


@pytest.fixture
def client(monkeypatch):
    c = TradeStationHttpClient(client_id="i", client_secret="s", refresh_token="r",
                               use_sandbox=True)
    c._access_token = "tok"
    c.token_expiry = datetime.now(tz=timezone.utc) + timedelta(hours=1)
    c.sleeps = AsyncMock()
    monkeypatch.setattr("asyncio.sleep", c.sleeps)
    return c


def _get(client, *responses):
    client._httpx.get = AsyncMock(side_effect=list(responses))
    return client._httpx.get


def _warnings(caplog):
    return [r for r in caplog.records if r.name == LOGGER and r.levelno == logging.WARNING]


def _errors(caplog):
    return [r for r in caplog.records if r.name == LOGGER and r.levelno == logging.ERROR]


# ----------------------------------------------------------------------------- the type
def test_broker_read_error_exported_with_endpoint_and_errors():
    assert tradestation_nt_community.BrokerReadError is BrokerReadError
    assert "BrokerReadError" in tradestation_nt_community.__all__
    e = BrokerReadError("GET /orders", [{"Error": "ServiceUnavailable"}])
    assert isinstance(e, Exception)
    assert e.endpoint == "GET /orders"
    assert e.errors == [{"Error": "ServiceUnavailable"}]
    assert "ServiceUnavailable" in str(e)


# ----------------------------------------------------------------------------- get_orders
def test_incident_body_three_times_raises_after_three_attempts(client, caplog):
    caplog.set_level(logging.WARNING, logger=LOGGER)
    get = _get(client, _resp(200, INCIDENT), _resp(200, INCIDENT), _resp(200, INCIDENT))
    with pytest.raises(BrokerReadError) as ei:
        asyncio.run(client.get_orders("SIM2736556F", status="Open"))
    assert get.await_count == 3
    assert ei.value.errors == INCIDENT["Errors"]
    assert "orders" in ei.value.endpoint
    assert [c.args[0] for c in client.sleeps.await_args_list] == [0.5, 1.0]
    assert len(_warnings(caplog)) == 3  # one per failed attempt, with the TS Error text
    assert all("ServiceUnavailable" in r.getMessage() for r in _warnings(caplog))
    assert len(_errors(caplog)) == 1  # exhausted
    assert "ServiceUnavailable" in _errors(caplog)[0].getMessage()


def test_transient_error_then_good_returns_data_with_one_warning(client, caplog):
    caplog.set_level(logging.WARNING, logger=LOGGER)
    get = _get(client, _resp(200, INCIDENT), _resp(200, {"Orders": [O1]}))
    assert asyncio.run(client.get_orders("SIM1", status="Open")) == [O1]
    assert get.await_count == 2
    assert [c.args[0] for c in client.sleeps.await_args_list] == [0.5]
    assert len(_warnings(caplog)) == 1
    assert _errors(caplog) == []
    # the retry re-sends the same request
    assert get.await_args_list[0].kwargs["params"] == get.await_args_list[1].kwargs["params"]


def test_mid_page_error_raises(client):
    """Review Focus 2: page 1 fine, page 2 Errors without NextToken -> raise, not truncate."""
    page2_bad = {"Errors": [{"Error": "ServiceUnavailable"}]}
    get = _get(client, _resp(200, {"Orders": [O1], "NextToken": "t"}),
               _resp(200, page2_bad), _resp(200, page2_bad), _resp(200, page2_bad))
    with pytest.raises(BrokerReadError):
        asyncio.run(client.get_orders("SIM1"))
    assert get.await_count == 4  # page 1 once, page 2 three times (retried, not restarted)
    for call in get.await_args_list[1:]:
        assert call.kwargs["params"]["nextToken"] == "t"


def test_mid_page_transient_error_retries_that_page_only(client):
    get = _get(client, _resp(200, {"Orders": [O1], "NextToken": "t"}),
               _resp(200, INCIDENT), _resp(200, {"Orders": [{"OrderID": "2"}]}))
    orders = asyncio.run(client.get_orders("SIM1"))
    assert [o["OrderID"] for o in orders] == ["1", "2"]
    assert get.await_count == 3


def test_account_error_next_to_partial_orders_raises(client):
    """TS may send partial data with an account error: partial is not truth."""
    body = {"Orders": [O1], "NextToken": "t", "Errors": [{"AccountID": "A", "Error": "Failed"}]}
    get = _get(client, _resp(200, body), _resp(200, body), _resp(200, body))
    with pytest.raises(BrokerReadError):
        asyncio.run(client.get_orders("SIM1"))
    assert get.await_count == 3  # never followed the NextToken of a failed page


def test_genuine_empty_stays_empty(client, caplog):
    """Review Focus 4: {"Orders": []} / {"Positions": []} with no Errors is a real empty."""
    caplog.set_level(logging.WARNING, logger=LOGGER)
    get = _get(client, _resp(200, {"Orders": []}))
    assert asyncio.run(client.get_orders("SIM1", status="Open")) == []
    assert get.await_count == 1
    get = _get(client, _resp(200, {"Orders": [], "Errors": []}))
    assert asyncio.run(client.get_orders("SIM1")) == []
    get = _get(client, _resp(200, {"Positions": []}))
    assert asyncio.run(client.get_positions("SIM1")) == []
    assert get.await_count == 1
    client.sleeps.assert_not_awaited()
    assert _warnings(caplog) == [] and _errors(caplog) == []


@pytest.mark.parametrize("body", [{}, {"NextToken": None}, {"Orders": None}, [O1], _JSON_NULL],
                         ids=["empty", "token-only", "null-container", "list", "json-null"])
def test_missing_container_raises(client, body):
    get = _get(client, *(_resp(200, body) for _ in range(3)))
    with pytest.raises(BrokerReadError):
        asyncio.run(client.get_orders("SIM1"))
    assert get.await_count == 3


def test_invalid_json_is_a_failed_read(client):
    get = _get(client, *(_resp(200, bad_json=True) for _ in range(3)))
    with pytest.raises(BrokerReadError):
        asyncio.run(client.get_orders("SIM1"))
    assert get.await_count == 3


# ----------------------------------------------------------------------------- positions / balances
def test_get_positions_incident_body_raises(client):
    get = _get(client, *(_resp(200, INCIDENT) for _ in range(3)))
    with pytest.raises(BrokerReadError):
        asyncio.run(client.get_positions("SIM1"))
    assert get.await_count == 3


def test_get_positions_partial_with_account_error_raises(client):
    body = {"Positions": [{"Symbol": "GCZ26", "Quantity": "1"}],
            "Errors": [{"AccountID": "SIM2", "Error": "ServiceUnavailable"}]}
    _get(client, *(_resp(200, body) for _ in range(3)))
    with pytest.raises(BrokerReadError):
        asyncio.run(client.get_positions("SIM1,SIM2"))


def test_get_positions_good_body_returns_rows(client):
    rows = [{"Symbol": "GCZ26", "Quantity": "1"}]
    _get(client, _resp(200, {"Positions": rows}))
    assert asyncio.run(client.get_positions("SIM1")) == rows


def test_get_balances_incident_body_raises(client):
    get = _get(client, *(_resp(200, INCIDENT) for _ in range(3)))
    with pytest.raises(BrokerReadError):
        asyncio.run(client.get_balances("SIM1"))
    assert get.await_count == 3


def test_get_balances_good_body_returns_first(client):
    _get(client, _resp(200, {"Balances": [{"CashBalance": "1"}]}))
    assert asyncio.run(client.get_balances("SIM1")) == {"CashBalance": "1"}


# ----------------------------------------------------------------------------- get_orders_by_id
def test_order_scoped_error_does_not_raise(client):
    """Review Focus 1: a NotFound scoped to one OrderID inside a good body is data."""
    body = {"Orders": [O1], "Errors": [{"OrderID": "999", "Error": "NotFound"}]}
    get = _get(client, _resp(200, body))
    assert asyncio.run(client.get_orders_by_id("SIM1", ["1", "999"])) == [O1]
    assert get.await_count == 1
    client.sleeps.assert_not_awaited()


def test_orders_by_id_unscoped_error_raises(client):
    body = {"Errors": [{"Error": "ServiceUnavailable"}]}
    get = _get(client, *(_resp(200, body) for _ in range(3)))
    with pytest.raises(BrokerReadError):
        asyncio.run(client.get_orders_by_id("SIM1", ["1"]))
    assert get.await_count == 3


def test_orders_by_id_non_notfound_error_on_an_id_raises(client):
    """M10: a non-NotFound error on one id means its state is unknown, not that it is absent."""
    body = {"Orders": [O1], "Errors": [{"OrderID": "2", "Error": "Failed", "Message": "busy"}]}
    get = _get(client, *(_resp(200, body) for _ in range(3)))
    with pytest.raises(BrokerReadError, match="busy"):
        asyncio.run(client.get_orders_by_id("SIM1", ["1", "2"]))
    assert get.await_count == 3


def test_orders_by_id_notfound_with_empty_order_id_raises(client):
    body = {"Orders": [O1], "Errors": [{"OrderID": "", "Error": "NotFound"}]}
    _get(client, *(_resp(200, body) for _ in range(3)))
    with pytest.raises(BrokerReadError):
        asyncio.run(client.get_orders_by_id("SIM1", ["1"]))


@pytest.mark.parametrize("method,container", [("get_orders", "Orders"),
                                              ("get_positions", "Positions"),
                                              ("get_balances", "Balances")])
@pytest.mark.parametrize("entry", [{"Symbol": "XYZ", "Error": "InvalidSymbol"},
                                   {"OrderID": "7", "Error": "NotFound"}],
                         ids=["symbol-scoped", "orderid-scoped"])
def test_list_read_with_any_scoped_error_raises(client, method, container, entry):
    """Final ruling: on a LIST read any Errors entry fails it - a scoped error there means
    that item is missing from the list, so the list is not the truth."""
    body = {container: [{"Symbol": "GCZ26", "OrderID": "1"}], "Errors": [entry]}
    get = _get(client, *(_resp(200, body) for _ in range(3)))
    with pytest.raises(BrokerReadError):
        asyncio.run(getattr(client, method)("SIM1"))
    assert get.await_count == 3


def test_empty_scope_value_is_not_scoped(client):
    body = {"Orders": [O1], "Errors": [{"OrderID": "", "Error": "ServiceUnavailable"}]}
    _get(client, *(_resp(200, body) for _ in range(3)))
    with pytest.raises(BrokerReadError):
        asyncio.run(client.get_orders("SIM1"))


def test_orders_by_id_404_still_means_none_found(client):
    get = _get(client, _resp(404, {"Error": "NotFound"}))
    assert asyncio.run(client.get_orders_by_id("SIM1", ["9"])) == []
    assert get.await_count == 1


# ----------------------------------------------------------------------------- market data
def test_quotes_symbol_scoped_error_returns_good_quote(client):
    q = {"Symbol": "GCZ26", "Bid": "1", "Ask": "2"}
    body = {"Quotes": [q], "Errors": [{"Symbol": "XXZ26", "Error": "Failed",
                                       "Message": "INVALID SYMBOL"}]}
    get = _get(client, _resp(200, body))
    assert asyncio.run(client.get_quotes("GCZ26,XXZ26")) == [q]
    assert get.await_count == 1


def test_quotes_order_id_scoped_error_is_not_a_quote_scope(client):
    body = {"Quotes": [{"Symbol": "GCZ26"}], "Errors": [{"OrderID": "7", "Error": "Failed"}]}
    _get(client, *(_resp(200, body) for _ in range(3)))
    with pytest.raises(BrokerReadError):
        asyncio.run(client.get_quotes("GCZ26"))


def test_quotes_invalid_symbol_example_returns_good_quote(client):
    q = {"Symbol": "GCZ26", "Bid": "1"}
    _get(client, _resp(200, {"Quotes": [q], "Errors": [{"Symbol": "XYZ",
                                                         "Error": "InvalidSymbol"}]}))
    assert asyncio.run(client.get_quotes("GCZ26,XYZ")) == [q]


def test_quotes_no_container_with_only_scoped_errors_raises_on_purpose(client):
    """LOW 1: no Quotes key is never read as "no quotes", even if every error is per-symbol."""
    body = {"Errors": [{"Symbol": "XYZ", "Error": "InvalidSymbol"}]}
    get = _get(client, *(_resp(200, body) for _ in range(3)))
    with pytest.raises(BrokerReadError):
        asyncio.run(client.get_quotes("XYZ"))
    assert get.await_count == 3


def test_quotes_account_level_error_raises(client):
    _get(client, *(_resp(200, {"Errors": [{"Error": "ServiceUnavailable"}]}) for _ in range(3)))
    with pytest.raises(BrokerReadError):
        asyncio.run(client.get_quotes("GCZ26"))


def test_bars_missing_container_raises_and_good_body_returns(client):
    _get(client, *(_resp(200, {"Errors": [{"Error": "ServiceUnavailable"}]}) for _ in range(3)))
    with pytest.raises(BrokerReadError):
        asyncio.run(client.get_bars("GCZ26", "1", TradeStationBarUnit.MINUTE, barsback=2))
    _get(client, _resp(200, {"Bars": [{"Close": "1"}]}))
    assert asyncio.run(
        client.get_bars("GCZ26", "1", TradeStationBarUnit.MINUTE, barsback=2)) == [{"Close": "1"}]


# ----------------------------------------------------------------------------- HTTP status
def test_http_500_on_a_read_is_retried_then_raises(client, caplog):
    caplog.set_level(logging.WARNING, logger=LOGGER)
    get = _get(client, *(_resp(500, {}) for _ in range(3)))
    with pytest.raises(BrokerReadError, match="HTTP 500"):
        asyncio.run(client.get_positions("SIM1"))
    assert get.await_count == 3
    assert len(_errors(caplog)) == 1


def test_http_503_then_good_returns_data(client):
    get = _get(client, _resp(503, {}), _resp(200, {"Orders": [O1]}))
    assert asyncio.run(client.get_orders("SIM1")) == [O1]
    assert get.await_count == 2


def test_http_4xx_on_a_read_is_not_retried(client):
    get = _get(client, _resp(403, {}))
    with pytest.raises(Exception, match="HTTP 403"):
        asyncio.run(client.get_orders("SIM1"))
    assert get.await_count == 1
    client.sleeps.assert_not_awaited()


# ----------------------------------------------------------------------------- writes
def test_writes_are_never_retried(client):
    client._httpx.post = AsyncMock(return_value=_resp(500, {}))
    with pytest.raises(Exception):
        asyncio.run(client.place_order("SIM1", "GCZ26", "1", "Market", "Buy"))
    assert client._httpx.post.await_count == 1
    client._httpx.put = AsyncMock(return_value=_resp(500, {}))
    with pytest.raises(Exception):
        asyncio.run(client.replace_order("1", "1", "StopMarket", stop_price="1"))
    assert client._httpx.put.await_count == 1
    client._httpx.delete = AsyncMock(return_value=_resp(500, {}))
    with pytest.raises(Exception):
        asyncio.run(client.cancel_order("1"))
    assert client._httpx.delete.await_count == 1
    client.sleeps.assert_not_awaited()


def test_place_order_with_account_error_body_is_not_retried(client):
    client._httpx.post = AsyncMock(return_value=_resp(200, INCIDENT))
    with pytest.raises(Exception):
        asyncio.run(client.place_order("SIM1", "GCZ26", "1", "Market", "Buy"))
    assert client._httpx.post.await_count == 1


# ----------------------------------------------------------------------------- execution.py
def _stub(read_error: Exception):
    s = MagicMock()
    s._account_id = "SIM1"
    s._log = MagicMock()
    s._client.get_orders_by_id = AsyncMock(side_effect=read_error)
    s._client.get_orders = AsyncMock(side_effect=read_error)
    s._client.get_positions = AsyncMock(side_effect=read_error)
    s._client_order_id_to_ts_order_id = {}
    s._is_own_cached_order = lambda c, t=None: False
    return s


def _logged(s, level):
    return " ".join(str(c.args[0]) for c in getattr(s._log, level).call_args_list)


def test_m10_inflight_broker_read_error_is_read_failed_not_absent():
    s = _stub(BrokerReadError("GET /orders/1", [{"OrderID": "1", "Error": "Failed"}]))
    assert asyncio.run(C._inflight_status_report(s, MagicMock(), "O-1", "1")) is None
    text = _logged(s, "error")
    assert "[INFLIGHT-QUERY-READ-FAILED]" in text
    assert "not in the broker order list" not in _logged(s, "warning")


def test_m10_order_status_report_broker_read_error_has_its_own_line():
    s = _stub(BrokerReadError("GET /orders/1", [{"Error": "ServiceUnavailable"}]))
    cmd = NS(instrument_id=MagicMock(), client_order_id="O-1", venue_order_id="1")
    assert asyncio.run(C.generate_order_status_report(s, cmd)) is None
    assert "[ORDER-STATUS-READ-FAILED]" in _logged(s, "error")
    assert "not found at TradeStation" not in _logged(s, "warning")


@pytest.mark.parametrize("method", ["generate_order_status_reports", "generate_fill_reports",
                                    "generate_position_status_reports"])
def test_l4_recon_report_read_failure_logs_error_and_returns_empty(method):
    s = _stub(BrokerReadError("GET /orders", INCIDENT["Errors"]))
    cmd = NS(instrument_id=None, open_only=False, venue_order_id=None, start=None, end=None)
    assert asyncio.run(getattr(C, method)(s, cmd)) == []
    assert "[RECON-REPORT-READ-FAILED]" in _logged(s, "error")


def test_m4_check_order_statuses_read_error_keeps_pending_cancel_verifies():
    """M4: a failed /orders read must not drop the §111 cancel verifications."""
    s = _stub(BrokerReadError("GET /orders", INCIDENT["Errors"]))
    s._ts_order_id_to_client_order_id = {"1": "O-1"}
    s._cancel_verify_pending = {"1": object()}
    s._status_watch = {}
    s._verify_pending_cancels = AsyncMock()
    asyncio.run(C._check_order_statuses(s))
    s._verify_pending_cancels.assert_not_awaited()
    assert s._cancel_verify_pending  # untouched
