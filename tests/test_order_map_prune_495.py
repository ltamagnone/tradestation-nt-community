"""OPEN_POINTS 4.95: _persist_order_map prunes the shared ts_order_map.json on save."""
from __future__ import annotations

import json
import time
from unittest.mock import MagicMock

from nautilus_trader.model.identifiers import ClientOrderId

from tradestation_nt_community.execution import TradeStationExecutionClient as C

DAY = 86400.0


class _Node:
    """The real map methods on a minimal client (one per node; both share one file)."""

    def __init__(self, path, group="stable", keep_cb=None, poll_ids=None):
        self._order_map_path = str(path)
        self._ts_order_id_to_client_order_id = {}
        self._client_order_id_to_ts_order_id = {}
        self._ts_id_to_group = {}
        self._node_group = group
        self._previous_alive = {}
        self._alive_loaded = False
        self._log = MagicMock()
        self._cache = MagicMock()
        self._cache.order.return_value = None
        self._ts_id_submitted_ts = {}
        self._order_map_keep_ids_callback = keep_cb
        self._last_poll_ts_ids = poll_ids
        self._last_order_map_prune_s = 0.0

    def __getattr__(self, name):          # _order_map_keep_ids, _prune_order_map, ...
        return getattr(C, name).__get__(self)


def _seed(path, now):
    data = {
        "_meta": {"alive": {"stable": now - 60}},
        "OLD": {"client_order_id": "O-old", "submitted_ts": now - 9 * DAY},
        "OLD-GRP": {"client_order_id": "O-oldg", "submitted_ts": now - 9 * DAY,
                    "group": "rolling"},
        "OLD-WORKING": {"client_order_id": "O-w", "submitted_ts": now - 30 * DAY,
                        "group": "rolling"},
        "OLD-POR": {"client_order_id": "O-p", "submitted_ts": now - 20 * DAY},
        "NEW": {"client_order_id": "O-new", "submitted_ts": now - 1 * DAY, "group": "stable"},
        "NO-TS": {"client_order_id": "O-nots"},
    }
    path.write_text(json.dumps(data))
    return data


def _read(path):
    return json.loads(path.read_text())


def test_prune_on_save_drops_old_keeps_working_por_meta(tmp_path):
    p = tmp_path / "ts_order_map.json"
    _seed(p, time.time())
    n = _Node(p, keep_cb=lambda: ["OLD-POR"], poll_ids={"OLD-WORKING", "NEW"})
    n._ts_order_id_to_client_order_id["T9"] = ClientOrderId("O-9")
    n._persist_order_map()
    out = _read(p)
    assert "OLD" not in out and "OLD-GRP" not in out          # old code kept both forever
    assert out["OLD-WORKING"]["group"] == "rolling"            # kept with its group tag
    assert "OLD-POR" in out and "NEW" in out and "NO-TS" in out
    assert out["_meta"]["alive"]["stable"] > 0
    assert out["T9"]["client_order_id"] == "O-9"
    assert any("[ORDER-MAP] pruned 2 entries" in str(c.args[0])
               for c in n._log.info.call_args_list)


def test_own_open_order_is_kept(tmp_path):
    p = tmp_path / "ts_order_map.json"
    _seed(p, time.time())
    n = _Node(p, keep_cb=lambda: [], poll_ids=set())
    n._ts_order_id_to_client_order_id["OLD"] = ClientOrderId("O-old")
    open_order = MagicMock(is_closed=False)
    n._cache.order.side_effect = lambda coid: open_order if str(coid) == "O-old" else None
    n._persist_order_map()
    out = _read(p)
    assert "OLD" in out and "OLD-GRP" not in out


def test_no_prune_without_keep_callback_or_poll(tmp_path):
    now = time.time()
    for cb, poll in ((None, set()), (lambda: [], None)):
        p = tmp_path / f"m{cb is None}.json"
        seeded = _seed(p, now)
        _Node(p, keep_cb=cb, poll_ids=poll)._persist_order_map()
        assert set(_read(p)) == set(seeded)


def test_failing_keep_callback_skips_prune_but_still_persists(tmp_path):
    p = tmp_path / "ts_order_map.json"
    seeded = _seed(p, time.time())

    def boom():
        raise OSError("POR unreadable")
    n = _Node(p, keep_cb=boom, poll_ids=set())
    n._ts_order_id_to_client_order_id["T9"] = ClientOrderId("O-9")
    n._persist_order_map()
    out = _read(p)
    assert set(seeded) <= set(out) and "T9" in out              # safe side: keep all
    assert any("prune skipped" in str(c.args[0]) for c in n._log.warning.call_args_list)


def test_prune_is_throttled(tmp_path):
    p = tmp_path / "ts_order_map.json"
    now = time.time()
    _seed(p, now)
    calls = []
    n = _Node(p, keep_cb=lambda: calls.append(1) or [], poll_ids=set())
    n._persist_order_map()
    _seed(p, now)
    n._persist_order_map()                                       # within the hour: no prune
    assert calls == [1] and "OLD" in _read(p)


def test_peer_pruned_id_does_not_come_back_as_new(tmp_path):
    """Node A prunes an old id; node B still holds it in memory (loaded when it was
    younger) and saves: the id must not return with a fresh submitted_ts."""
    p = tmp_path / "ts_order_map.json"
    now = time.time()
    _seed(p, now)
    b = _Node(p, group="rolling")
    b._load_order_map()                     # B loads at a time OLD was still < 7 days old
    b._ts_order_id_to_client_order_id["OLD"] = ClientOrderId("O-old")
    b._ts_id_submitted_ts["OLD"] = now - 9 * DAY
    a = _Node(p, keep_cb=lambda: [], poll_ids=set())
    a._persist_order_map()
    assert "OLD" not in _read(p)
    b._persist_order_map()                  # B has no keep sources: merges, does not prune
    # old code: re-stamped "now"; review fix: a pruned old id is not written back at all
    assert "OLD" not in _read(p)
    assert b._ts_id_submitted_ts["OLD"] == now - 9 * DAY      # original time kept in memory


def test_load_records_submit_times(tmp_path):
    p = tmp_path / "ts_order_map.json"
    now = time.time()
    _seed(p, now)
    n = _Node(p)
    n._load_order_map()
    assert n._ts_id_submitted_ts["NEW"] == now - 1 * DAY
    assert "OLD" not in n._ts_id_submitted_ts                    # pruned at load as before


async def test_status_poll_records_broker_ids():
    from tests.test_execution import _make_check_statuses_mock
    m = _make_check_statuses_mock(ts_orders=[{"OrderID": "TS-STOP", "Status": "OPN"},
                                             {"OrderID": "PEER-1", "Status": "ACK"}])
    await C._check_order_statuses(m)
    assert m._last_poll_ts_ids == {"TS-STOP", "PEER-1"}


async def test_failed_status_poll_keeps_previous_ids():
    from unittest.mock import AsyncMock
    from tests.test_execution import _make_check_statuses_mock
    m = _make_check_statuses_mock()
    m._last_poll_ts_ids = None
    m._client.get_orders = AsyncMock(side_effect=RuntimeError("read failed"))
    await C._check_order_statuses(m)
    assert m._last_poll_ts_ids is None
