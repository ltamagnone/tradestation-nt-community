"""Review fixes for 4.109 (odd 200 shapes after the client) and 4.95 (no resurrection of
pruned ids between prunes)."""
from __future__ import annotations

import json
import time
from unittest.mock import AsyncMock, MagicMock

import pytest

from nautilus_trader.model.identifiers import ClientOrderId

from tests.test_backlog_4143_4109 import LEG
from tests.test_http_client import _mock_resp, http_client  # noqa: F401 (fixture)
from tests.test_kit import make_market_order
from tests.test_late_accept_cancel import _group_stub
from tests.test_order_attribution import _build_submit_command, _make_submit_order_stub
from tests.test_order_map_prune_495 import DAY, _Node, _read, _seed
from tradestation_nt_community import SUBMIT_AMBIGUOUS_REASON_PREFIX
from tradestation_nt_community.execution import TradeStationExecutionClient as C
from tradestation_nt_community.http.client import (
    OrderGroupPartialException,
    OrderResponseAmbiguousException,
)


# ------------------------------------------- 4.109: group answer with an odd leg list

@pytest.mark.parametrize("orders", [
    [{"OrderID": "91"}, "junk", {"OrderID": "92"}],     # extra non-dict entry
    [{"OrderID": "91"}, {}, {"OrderID": "92"}],          # extra entry without an id
    [{"OrderID": 91}, {"OrderID": "92"}],                # a non-string id
])
async def test_group_odd_leg_list_is_partial_not_success(http_client, orders):  # noqa: F811
    """Old code counted only the dict legs with an id: 2 landed == 2 legs -> success, and
    the execution client then paired NT legs with the raw list by position."""
    http_client._httpx.post = AsyncMock(return_value=_mock_resp(200, {"Orders": orders}))
    with pytest.raises(OrderGroupPartialException) as ei:
        await http_client.place_order_group("OCO", [LEG, LEG])
    assert sorted(ei.value.landed_order_ids) == ["91", "92"]   # caller cancels both


async def test_group_non_dict_first_entry_is_ambiguous(http_client):  # noqa: F811
    body = {"Orders": ["junk", {"OrderID": "91"}, {"OrderID": "92"}]}
    http_client._httpx.post = AsyncMock(return_value=_mock_resp(200, body))
    with pytest.raises(OrderResponseAmbiguousException):
        await http_client.place_order_group("OCO", [LEG, LEG])


async def test_group_normal_answer_still_success(http_client):  # noqa: F811
    body = {"Orders": [{"OrderID": "91"}, {"OrderID": "92"}]}
    http_client._httpx.post = AsyncMock(return_value=_mock_resp(200, body))
    assert await http_client.place_order_group("OCO", [LEG, LEG]) == body


@pytest.mark.parametrize("orders", [
    ["junk", {"OrderID": "91"}],
    [{"OrderID": 90}, {"OrderID": "91"}],
    [{"OrderID": "90"}],                                 # fewer entries than legs
])
async def test_group_odd_answer_in_exec_client_is_ambiguous(orders):
    stub, cmd = _group_stub([False, False])
    stub._client.place_order_group = AsyncMock(return_value={"Orders": orders})
    await C._submit_order_group(stub, cmd, "OCO", [{}, {}])
    stub.generate_order_accepted.assert_not_called()
    reasons = [c.kwargs["reason"] for c in stub.generate_order_rejected.call_args_list]
    assert len(reasons) == 2 and all(
        r.startswith(SUBMIT_AMBIGUOUS_REASON_PREFIX) for r in reasons), reasons
    assert stub._ts_order_id_to_client_order_id == {}       # nothing half-registered


@pytest.mark.parametrize("response", [{"OrderID": 123}, {"Orders": [{"OrderID": ["x"]}]}])
async def test_submit_non_string_order_id_is_ambiguous(response):
    stub = _make_submit_order_stub()
    stub._client.place_order = AsyncMock(return_value=response)
    await C._submit_order(stub, _build_submit_command(make_market_order(tags=None)))
    reason = stub.generate_order_rejected.call_args.kwargs["reason"]
    assert reason.startswith(SUBMIT_AMBIGUOUS_REASON_PREFIX), reason
    stub.generate_order_accepted.assert_not_called()


# ------------------------------------------- 4.95: no resurrection between prunes

def test_pruned_old_id_is_not_written_back_between_prunes(tmp_path):
    """A prunes OLD; B (no prune due) holds OLD in memory: B's save must not put it back."""
    p = tmp_path / "ts_order_map.json"
    now = time.time()
    _seed(p, now)
    b = _Node(p, group="rolling")
    b._ts_order_id_to_client_order_id["OLD"] = ClientOrderId("O-old")
    b._ts_id_submitted_ts["OLD"] = now - 9 * DAY
    b._ts_order_id_to_client_order_id["T9"] = ClientOrderId("O-9")
    _Node(p, keep_cb=lambda: [], poll_ids=set())._persist_order_map()
    assert "OLD" not in _read(p)
    b._persist_order_map()
    out = _read(p)
    assert "OLD" not in out                                   # old code: back with old ts
    assert out["T9"]["client_order_id"] == "O-9"


def test_own_open_old_id_is_written_back(tmp_path):
    p = tmp_path / "ts_order_map.json"
    now = time.time()
    p.write_text(json.dumps({}))
    b = _Node(p, group="rolling")
    b._ts_order_id_to_client_order_id["OLD"] = ClientOrderId("O-old")
    b._ts_id_submitted_ts["OLD"] = now - 9 * DAY
    b._cache.order.side_effect = lambda coid: MagicMock(is_closed=False)
    b._persist_order_map()
    assert _read(p)["OLD"]["submitted_ts"] == now - 9 * DAY


def test_unreadable_file_writes_every_in_memory_id(tmp_path):
    """Safe side: when the file cannot be read, nothing in memory is skipped."""
    p = tmp_path / "ts_order_map.json"
    now = time.time()
    p.write_text("{not json")
    b = _Node(p, group="rolling")
    b._ts_order_id_to_client_order_id["OLD"] = ClientOrderId("O-old")
    b._ts_id_submitted_ts["OLD"] = now - 9 * DAY
    b._persist_order_map()
    assert "OLD" in _read(p)
