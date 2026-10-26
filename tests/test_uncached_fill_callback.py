"""§151: uncached-fill classification, callback, order-map group + last-alive stamps."""
import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from nautilus_trader.model.identifiers import ClientOrderId

from tradestation_nt_community.execution import (
    TradeStationExecutionClient as C,
    classify_uncached_fill,
)

START = 1790686800.0          # 2026-09-29T13:00:00Z instance start
ALIVE = START - 120.0         # previous process last alive 2 min before start


def _iso(epoch):
    import datetime as dt
    return dt.datetime.fromtimestamp(epoch, dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class TestClassify:
    def test_live(self):
        assert classify_uncached_fill(START + 5, START, "stable", "stable", ALIVE) == "live"

    def test_delivered(self):
        assert classify_uncached_fill(ALIVE - 61, START, "stable", "stable", ALIVE) == "delivered"

    def test_downtime(self):
        assert classify_uncached_fill(ALIVE + 30, START, "stable", "stable", ALIVE) == "downtime"

    def test_boundary_minus_60_is_downtime(self):
        assert classify_uncached_fill(ALIVE - 60, START, "stable", "stable", ALIVE) == "downtime"
        assert classify_uncached_fill(ALIVE - 60.5, START, "stable", "stable", ALIVE) == "delivered"

    def test_other_group_is_delivered_other(self):
        assert classify_uncached_fill(ALIVE + 30, START, "rolling", "stable", ALIVE) == "delivered_other"

    @pytest.mark.parametrize("args", [
        (None, START, "stable", "stable", ALIVE),       # no / unparseable ClosedDateTime
        (ALIVE, START, None, "stable", ALIVE),           # entry has no group (old file)
        (ALIVE, START, "stable", None, ALIVE),           # node has no group configured
        (ALIVE, START, "stable", "stable", None),        # no alive stamp
        (ALIVE, None, "stable", "stable", ALIVE),        # no instance start
    ])
    def test_unknown(self, args):
        assert classify_uncached_fill(*args) == "unknown"


def _client(group="stable", entry_group="stable"):
    m = MagicMock()
    m._ts_order_id_to_client_order_id = {"972944192": ClientOrderId("O-OLD-1")}
    m._node_group = group
    m._ts_id_to_group = {"972944192": entry_group} if entry_group else {}
    m._previous_alive = {"stable": ALIVE, "rolling": ALIVE}
    m._instance_started_s = START
    m._order_last_status = {}
    m._pending_modify_trigger_price = {}
    m._cancel_verify_pending = {}
    m._verify_pending_cancels = AsyncMock()
    m._cache = MagicMock()
    m._cache.order.return_value = None
    m._clock = MagicMock()
    m._clock.timestamp_ns.return_value = 0
    m._log = MagicMock()
    m._loop = MagicMock()
    m._loop.call_soon.side_effect = lambda fn, *a: fn(*a)   # run synchronously
    m._uncached_fill_callback = MagicMock()
    return m


def _order(closed_epoch, status="FLL", exec_qty="2"):
    return {"OrderID": "972944192", "Status": status, "ClosedDateTime": _iso(closed_epoch),
            "AveragePrice": "70.1", "Legs": [{"ExecQuantity": exec_qty, "QuantityOrdered": "2"}]}


class TestCallback:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("closed,kind", [
        (START + 10, "live"), (ALIVE + 10, "downtime")])
    async def test_sse_path_fires(self, closed, kind):
        m = _client()
        await C._process_order_event(m, _order(closed))
        m._uncached_fill_callback.assert_called_once()
        p = m._uncached_fill_callback.call_args[0][0]
        assert p["kind"] == kind and p["ts_order_id"] == "972944192"
        assert p["status"] == "FLL" and p["exec_qty"] == "2" and p["client_order_id"] == "O-OLD-1"
        # callback registered -> the runner is the sole alerter: no WARN tag
        assert not m._log.warning.call_args_list
        assert any("handed to the runner" in str(c) for c in m._log.info.call_args_list)

    @pytest.mark.asyncio
    async def test_no_callback_downtime_warns_with_tag(self):
        m = _client()
        m._uncached_fill_callback = None
        await C._process_order_event(m, _order(ALIVE + 10))
        assert any("[DOWNTIME-FILL]" in str(c) for c in m._log.warning.call_args_list)

    @pytest.mark.asyncio
    async def test_poll_path_fires(self):
        m = _client()
        m._client = MagicMock()
        m._client.get_orders = AsyncMock(return_value=[_order(ALIVE + 10, "FLP", "1")])
        await C._check_order_statuses(m)
        m._uncached_fill_callback.assert_called_once()
        assert m._uncached_fill_callback.call_args[0][0]["kind"] == "downtime"
        assert m._uncached_fill_callback.call_args[0][0]["status"] == "FLP"

    @pytest.mark.asyncio
    async def test_unknown_fires_without_group(self):
        m = _client(entry_group=None)
        await C._process_order_event(m, _order(START + 10))
        assert m._uncached_fill_callback.call_args[0][0]["kind"] == "unknown"

    @pytest.mark.asyncio
    async def test_delivered_does_not_fire(self):
        m = _client()
        await C._process_order_event(m, _order(ALIVE - 3600))
        m._uncached_fill_callback.assert_not_called()
        assert not m._log.warning.call_args_list or all(
            "[UNCACHED-FILL-LIVE]" not in str(c) and "[DOWNTIME-FILL]" not in str(c)
            for c in m._log.warning.call_args_list)
        assert any("already delivered" in str(c) for c in m._log.info.call_args_list)

    @pytest.mark.asyncio
    async def test_other_tag_without_callback_keeps_delivered_other(self):
        """No runner callback (old runner / tests): today's behaviour, unchanged."""
        m = _client(group="rolling", entry_group="stable")
        m._uncached_fill_callback = None
        from tradestation_nt_community.execution import _handle_uncached_fill
        assert _handle_uncached_fill(m, _order(START + 10), "972944192", "O-OLD-1") \
            == "delivered_other"
        assert any("owned by the other node (group stable)" in str(c)
                   for c in m._log.info.call_args_list)
        assert not m._log.warning.call_args_list

    @pytest.mark.asyncio
    async def test_callback_exception_does_not_break_poll(self):
        m = _client()
        m._uncached_fill_callback.side_effect = RuntimeError("boom")
        m._client = MagicMock()
        m._client.get_orders = AsyncMock(return_value=[_order(START + 10)])
        await C._check_order_statuses(m)       # must not raise
        warns = [str(c) for c in m._log.warning.call_args_list]
        assert any("[UNCACHED-FILL-LIVE] TS order 972944192 (O-OLD-1) callback failed" in w
                   and "RuntimeError: boom" in w for w in warns)

    @pytest.mark.asyncio
    async def test_no_callback_registered_is_todays_behaviour(self):
        m = _client()
        m._uncached_fill_callback = None
        await C._process_order_event(m, _order(START + 10))
        assert any("[UNCACHED-FILL-LIVE]" in str(c) for c in m._log.warning.call_args_list)

    def test_scheduling_failure_is_swallowed(self):
        m = _client()
        m._loop.call_soon.side_effect = RuntimeError("closed")
        from tradestation_nt_community.execution import _handle_uncached_fill
        assert _handle_uncached_fill(m, _order(START + 10), "972944192", "O-OLD-1") == "live"
        assert any("[UNCACHED-FILL-LIVE] TS order 972944192 (O-OLD-1) could not schedule" in str(c)
                   for c in m._log.warning.call_args_list)

    @pytest.mark.asyncio
    async def test_downtime_callback_failure_uses_downtime_tag(self):
        m = _client()
        m._uncached_fill_callback.side_effect = RuntimeError("boom")
        await C._process_order_event(m, _order(ALIVE + 10))
        assert any("[DOWNTIME-FILL] TS order 972944192 (O-OLD-1) callback failed" in str(c)
                   for c in m._log.warning.call_args_list)

    def test_setter(self):
        m = MagicMock()
        cb = lambda p: None
        C.set_uncached_fill_callback(m, cb)
        assert m._uncached_fill_callback is cb


class TestHostingDecides233:
    """§233: with a runner callback the submit-time tag no longer decides ownership.
    2026-10-25 #976210241: tagged `stable`, strategy hosted on rolling — rolling dropped
    it as delivered_other, stable's runner said non-local: nobody held or alerted."""

    @pytest.mark.asyncio
    async def test_other_tag_with_callback_fires_live_sse(self):
        m = _client(group="rolling", entry_group="stable")
        await C._process_order_event(m, _order(START + 10))
        m._uncached_fill_callback.assert_called_once()
        p = m._uncached_fill_callback.call_args[0][0]
        assert p["kind"] == "live" and p["group"] == "stable" and p["tag_mismatch"] is True
        assert not m._log.warning.call_args_list           # runner is the sole alerter
        infos = [str(c) for c in m._log.info.call_args_list]
        assert any("tagged group stable (this node rolling)" in s
                   and "handed to the runner" in s for s in infos)
        assert not any("owned by the other node" in s for s in infos)

    @pytest.mark.asyncio
    async def test_other_tag_with_callback_fires_poll(self):
        m = _client(group="rolling", entry_group="stable")
        m._client = MagicMock()
        m._client.get_orders = AsyncMock(return_value=[_order(START + 10)])
        await C._check_order_statuses(m)
        p = m._uncached_fill_callback.call_args[0][0]
        assert p["kind"] == "live" and p["tag_mismatch"] is True

    @pytest.mark.asyncio
    async def test_other_tag_prestart_is_timed_by_own_alive_stamp(self):
        # The tag group's stamp is irrelevant (R3): only rolling's own stamp times it.
        m = _client(group="rolling", entry_group="stable")
        m._previous_alive = {"rolling": ALIVE, "stable": START + 9999}
        await C._process_order_event(m, _order(ALIVE + 10))
        assert m._uncached_fill_callback.call_args[0][0]["kind"] == "downtime"

    @pytest.mark.asyncio
    async def test_other_tag_old_fill_is_delivered_and_not_fired(self):
        m = _client(group="rolling", entry_group="stable")
        await C._process_order_event(m, _order(ALIVE - 3600))
        m._uncached_fill_callback.assert_not_called()
        assert any("already delivered" in str(c) for c in m._log.info.call_args_list)

    @pytest.mark.asyncio
    async def test_other_tag_no_own_alive_stamp_is_unknown_and_fires(self):
        m = _client(group="rolling", entry_group="stable")
        m._previous_alive = {"stable": ALIVE}            # rolling never stamped
        await C._process_order_event(m, _order(ALIVE + 10))
        assert m._uncached_fill_callback.call_args[0][0]["kind"] == "unknown"

    @pytest.mark.asyncio
    async def test_own_tag_payload_unchanged(self):
        m = _client()                                     # stable / stable
        await C._process_order_event(m, _order(START + 10))
        p = m._uncached_fill_callback.call_args[0][0]
        assert p["kind"] == "live" and p["group"] == "stable" and p["tag_mismatch"] is False
        assert not any("tagged group" in str(c) for c in m._log.info.call_args_list)

    @pytest.mark.asyncio
    async def test_no_tag_still_unknown_and_not_a_mismatch(self):
        m = _client(entry_group=None)
        await C._process_order_event(m, _order(START + 10))
        p = m._uncached_fill_callback.call_args[0][0]
        assert p["kind"] == "unknown" and p["group"] is None and p["tag_mismatch"] is False


def _stub(path, group):
    m = MagicMock()
    m._order_map_path = str(path)
    m._node_group = group
    m._ts_order_id_to_client_order_id = {}
    m._client_order_id_to_ts_order_id = {}
    m._ts_id_to_group = {}
    m._previous_alive = {}
    m._alive_loaded = False
    m._last_alive_stamp_s = 0.0
    m._log = MagicMock()
    m._read_order_map_file = lambda: C._read_order_map_file(m)
    m._write_order_map_file = lambda d: C._write_order_map_file(m, d)
    return m


class TestOrderMapGroupAndAlive:
    def test_group_written_for_new_entry_and_alive_stamped(self, tmp_path):
        p = tmp_path / "map.json"
        a = _stub(p, "stable")
        C._load_order_map(a)
        a._ts_order_id_to_client_order_id["T1"] = ClientOrderId("O-1")
        C._persist_order_map(a)
        C._stamp_alive(a, force=True)
        d = json.loads(p.read_text())
        assert d["T1"]["group"] == "stable"
        assert "stable" in d["_meta"]["alive"]

    def test_stamp_not_written_before_load(self, tmp_path):
        p = tmp_path / "map.json"
        a = _stub(p, "stable")
        C._stamp_alive(a, force=True)
        assert not p.exists()

    def test_two_groups_merge_not_clobber(self, tmp_path):
        p = tmp_path / "map.json"
        a, b = _stub(p, "stable"), _stub(p, "rolling")
        C._load_order_map(a); C._load_order_map(b)
        a._ts_order_id_to_client_order_id["T1"] = ClientOrderId("O-1")
        b._ts_order_id_to_client_order_id["T2"] = ClientOrderId("O-2")
        C._persist_order_map(a); C._persist_order_map(b)
        C._stamp_alive(a, force=True); C._stamp_alive(b, force=True)
        C._persist_order_map(a)
        d = json.loads(p.read_text())
        assert d["T1"]["group"] == "stable" and d["T2"]["group"] == "rolling"
        assert set(d["_meta"]["alive"]) == {"stable", "rolling"}

    def test_old_format_persist_regression(self, tmp_path):
        """After load + persist from EITHER node, group fields and _meta survive."""
        p = tmp_path / "map.json"
        import time
        now = time.time()
        p.write_text(json.dumps({
            "T1": {"client_order_id": "O-1", "submitted_ts": now, "group": "stable"},
            "T2": {"client_order_id": "O-2", "submitted_ts": now, "group": "rolling"},
            "T3": {"client_order_id": "O-3", "submitted_ts": now},   # old code, no group
            "_meta": {"alive": {"stable": now - 100, "rolling": now - 90}},
        }))
        for g in ("stable", "rolling"):
            n = _stub(p, g)
            C._load_order_map(n)
            assert n._previous_alive["stable"] == pytest.approx(now - 100)
            assert n._ts_id_to_group == {"T1": "stable", "T2": "rolling"}
            C._persist_order_map(n)
            d = json.loads(p.read_text())
            assert d["T1"]["group"] == "stable" and d["T2"]["group"] == "rolling"
            assert "group" not in d["T3"]            # unknown stays unknown, not mis-stamped
            assert d["_meta"]["alive"]["stable"] == pytest.approx(now - 100)

    def test_old_file_without_group_still_loads(self, tmp_path):
        p = tmp_path / "map.json"
        import time
        p.write_text(json.dumps({"T1": {"client_order_id": "O-1", "submitted_ts": time.time()}}))
        n = _stub(p, "stable")
        C._load_order_map(n)
        assert "T1" in n._ts_order_id_to_client_order_id and n._ts_id_to_group == {}


class TestLegacyGroupStamp:
    """Final review: legacy (group-less) entries seen in this process's own cache get stamped."""

    def test_legacy_entry_stamped_when_in_own_cache_and_persisted(self, tmp_path):
        from tradestation_nt_community.execution import _stamp_group_if_missing
        p = tmp_path / "map.json"
        import time
        p.write_text(json.dumps({"T9": {"client_order_id": "O-9", "submitted_ts": time.time()}}))
        a = _stub(p, "stable")
        C._load_order_map(a)
        assert "T9" not in a._ts_id_to_group              # legacy entry kept as-is on load
        _stamp_group_if_missing(a, "T9")                   # status poll saw it in the cache
        assert a._ts_id_to_group["T9"] == "stable"
        C._persist_order_map(a)
        assert json.loads(p.read_text())["T9"]["group"] == "stable"

    def test_existing_group_not_overwritten(self):
        from tradestation_nt_community.execution import _stamp_group_if_missing
        a = MagicMock()
        a._node_group = "stable"
        a._ts_id_to_group = {"T1": "rolling"}
        _stamp_group_if_missing(a, "T1")
        assert a._ts_id_to_group["T1"] == "rolling"

    @pytest.mark.asyncio
    async def test_poll_stamps_cached_legacy_order(self):
        m = _client(entry_group=None)
        m._cache.order.return_value = MagicMock(is_closed=True)   # own cached order
        from tradestation_nt_community.execution import _stamp_group_if_missing
        _stamp_group_if_missing(m, "972944192")
        assert m._ts_id_to_group["972944192"] == "stable"
