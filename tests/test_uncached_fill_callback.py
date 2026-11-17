"""§151: uncached-fill classification, callback, order-map group + last-alive stamps."""
import json
import time as _time
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from nautilus_trader.model.enums import OrderStatus
from nautilus_trader.model.identifiers import ClientOrderId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import StrategyId
from nautilus_trader.model.identifiers import VenueOrderId
from nautilus_trader.model.objects import Quantity

from tradestation_nt_community.execution import (
    TradeStationExecutionClient as C,
    _emit_fill_delta,
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
    m._modify_inflight = {}
    m._pending_amend_meta = {}
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
        assert any("already delivered" in str(c) and "tagged group stable" in str(c)
                   for c in m._log.info.call_args_list)

    @pytest.mark.asyncio
    async def test_tag_set_node_group_unset_is_unknown_not_a_mismatch(self):
        m = _client(group=None, entry_group="stable")
        await C._process_order_event(m, _order(START + 10))
        m._uncached_fill_callback.assert_called_once()
        p = m._uncached_fill_callback.call_args[0][0]
        assert p["kind"] == "unknown" and p["group"] == "stable" \
            and p["tag_mismatch"] is False

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


# ---------------------------------------------------------------------------------------------
# Restart noise (incident 2026-11-17 03:45Z): a fill the previous process delivered seconds
# before a clean stop lands inside the downtime band (alive_prev - 60 s .. start). The clean
# stop's forced alive stamp also writes ``_meta.delivered[group]`` = the fills that process
# emitted AND NT applied; the next process classifies those as ``delivered``.


class TestClassifyDeliveredPrev:
    def test_delivered_prev_overrides_downtime_band(self):
        assert classify_uncached_fill(ALIVE + 30, START, "stable", "stable", ALIVE,
                                      delivered_prev=True) == "delivered"

    def test_delivered_prev_ignored_for_live(self):
        assert classify_uncached_fill(START + 5, START, "stable", "stable", ALIVE,
                                      delivered_prev=True) == "live"

    def test_no_flag_keeps_downtime(self):
        assert classify_uncached_fill(ALIVE + 30, START, "stable", "stable", ALIVE) == "downtime"
        assert classify_uncached_fill(ALIVE + 30, START, "stable", "stable", ALIVE,
                                      delivered_prev=False) == "downtime"

    def test_delivered_prev_does_not_claim_other_group(self):
        assert classify_uncached_fill(ALIVE + 30, START, "rolling", "stable", ALIVE,
                                      delivered_prev=True) == "delivered_other"


def _applied(qty):
    o = MagicMock()
    o.filled_qty = Quantity.from_str(str(qty))
    return o


def _dstub(path, group, orders=None):
    """Order-map stub with a cache: ``orders`` coid-str -> filled qty (None = not cached)."""
    m = _stub(path, group)
    m._delivered_fills = {}
    m._previous_delivered = {}
    orders = orders or {}
    m._cache = MagicMock()
    m._cache.order.side_effect = lambda coid: (
        _applied(orders[str(coid)]) if orders.get(str(coid)) is not None else None)
    return m


class TestDeliveredStamp:
    def test_forced_stamp_writes_applied_delivered_fills(self, tmp_path):
        p = tmp_path / "map.json"
        a = _dstub(p, "stable", {"O-1": 5, "O-2": 2})
        C._load_order_map(a)
        a._delivered_fills["T1"] = (Decimal(5), ClientOrderId("O-1"))
        a._delivered_fills["T2"] = (Decimal(2), ClientOrderId("O-2"))
        C._stamp_alive(a, force=True)
        d = json.loads(p.read_text())
        assert d["_meta"]["delivered"]["stable"] == {"T1": 5.0, "T2": 2.0}
        assert "stable" in d["_meta"]["alive"]

    def test_periodic_stamp_writes_no_list_and_clears_own_stale_one(self, tmp_path):
        p = tmp_path / "map.json"
        p.write_text(json.dumps({"_meta": {"alive": {"stable": 1.0, "rolling": 1.0},
                                           "delivered": {"stable": {"T0": 1.0},
                                                         "rolling": {"R0": 3.0}}}}))
        a = _dstub(p, "stable", {"O-1": 5})
        C._load_order_map(a)
        a._delivered_fills["T1"] = (Decimal(5), ClientOrderId("O-1"))
        C._stamp_alive(a)                     # periodic (not forced)
        meta = json.loads(p.read_text())["_meta"]
        assert "stable" not in meta.get("delivered", {})
        assert meta["delivered"]["rolling"] == {"R0": 3.0}   # the peer's list untouched
        assert a._previous_delivered["stable"] == {"T0": 1.0}  # in-memory copy kept

    @pytest.mark.parametrize("orders", [{"O-1": 3}, {"O-1": None}])
    def test_unapplied_or_uncached_fill_not_listed(self, tmp_path, orders):
        p = tmp_path / "map.json"
        a = _dstub(p, "stable", orders)
        C._load_order_map(a)
        a._delivered_fills["T1"] = (Decimal(5), ClientOrderId("O-1"))
        C._stamp_alive(a, force=True)
        assert json.loads(p.read_text())["_meta"]["delivered"]["stable"] == {}

    def test_cap_500_keeps_newest(self, tmp_path):
        p = tmp_path / "map.json"
        orders = {f"O-{i}": 1 for i in range(600)}
        a = _dstub(p, "stable", orders)
        C._load_order_map(a)
        for i in range(600):
            a._delivered_fills[f"T{i}"] = (Decimal(1), ClientOrderId(f"O-{i}"))
        C._stamp_alive(a, force=True)
        lst = json.loads(p.read_text())["_meta"]["delivered"]["stable"]
        assert len(lst) == 500 and "T599" in lst and "T99" not in lst and "T100" in lst

    def test_list_failure_warns_writes_no_list_but_stamps_alive(self, tmp_path):
        p = tmp_path / "map.json"
        p.write_text(json.dumps({"_meta": {"delivered": {"stable": {"T0": 1.0}}}}))
        a = _dstub(p, "stable")
        C._load_order_map(a)
        a._cache.order.side_effect = RuntimeError("boom")
        a._delivered_fills["T1"] = (Decimal(5), ClientOrderId("O-1"))
        C._stamp_alive(a, force=True)
        meta = json.loads(p.read_text())["_meta"]
        assert "stable" in meta["alive"] and "stable" not in meta.get("delivered", {})
        assert any("delivered" in str(c) for c in a._log.warning.call_args_list)

    def test_load_reads_previous_list_before_overwrite(self, tmp_path):
        p = tmp_path / "map.json"
        p.write_text(json.dumps({"_meta": {"alive": {"stable": 1.0},
                                           "delivered": {"stable": {"T1": 5.0},
                                                         "rolling": {"R1": 2}}}}))
        b = _dstub(p, "stable")
        C._load_order_map(b)
        C._stamp_alive(b, force=True)          # this instance has delivered nothing
        assert json.loads(p.read_text())["_meta"]["delivered"]["stable"] == {}
        assert b._previous_delivered == {"stable": {"T1": 5.0}, "rolling": {"R1": 2.0}}

    def test_emitter_records_delivered_fill(self):
        m = MagicMock()
        m._emitted_exec_hwm = {}
        m._delivered_fills = {}
        m._cancel_after_ack_sent = {}
        cached = MagicMock()
        cached.quantity = Quantity.from_int(5)
        cached.filled_qty = Quantity.from_int(0)
        coid = ClientOrderId("O-1")
        o = {"OrderID": "T1", "Status": "FLL", "FilledPrice": "0.65",
             "Legs": [{"ExecQuantity": "5", "QuantityOrdered": "5"}]}
        emitted, _, _ = _emit_fill_delta(m, o, "T1", coid, cached, VenueOrderId("T1"), 7,
                                         "sse", partial=False)
        assert emitted and m._delivered_fills == {"T1": (Decimal(5), coid)}

    def test_inflight_applied_fill_is_recorded_and_listed(self, tmp_path):
        """NT applied the fill via in-flight report reconciliation: the later FLL takes the
        already_applied branch, but the fill is still listed by the clean-stop stamp."""
        p = tmp_path / "map.json"
        a = _dstub(p, "stable", {"O-1": 5})
        C._load_order_map(a)
        a._emitted_exec_hwm = {}
        a._cancel_after_ack_sent = {}
        cached = MagicMock()
        cached.quantity = Quantity.from_int(5)
        cached.filled_qty = Quantity.from_int(5)          # already applied by NT
        coid = ClientOrderId("O-1")
        o = {"OrderID": "T1", "Status": "FLL", "FilledPrice": "0.65",
             "Legs": [{"ExecQuantity": "5", "QuantityOrdered": "5"}]}
        emitted, _, reason = _emit_fill_delta(a, o, "T1", coid, cached, VenueOrderId("T1"),
                                              7, "sse", partial=False)
        assert not emitted and reason == "already_applied"
        assert a._delivered_fills == {"T1": (Decimal(5), coid)}
        C._stamp_alive(a, force=True)
        assert json.loads(p.read_text())["_meta"]["delivered"]["stable"] == {"T1": 5.0}

    def test_emitter_tolerates_client_without_the_dict(self):
        m = MagicMock()          # _delivered_fills is a MagicMock, not a dict
        m._emitted_exec_hwm = {}
        m._cancel_after_ack_sent = {}
        cached = MagicMock()
        cached.quantity = Quantity.from_int(5)
        cached.filled_qty = Quantity.from_int(0)
        o = {"OrderID": "T1", "Status": "FLL", "FilledPrice": "0.65",
             "Legs": [{"ExecQuantity": "5", "QuantityOrdered": "5"}]}
        emitted, _, _ = _emit_fill_delta(m, o, "T1", ClientOrderId("O-1"), cached,
                                         VenueOrderId("T1"), 7, "sse", partial=False)
        assert emitted


class TestDeliveredListUncachedFill:
    """The next process: an uncached FLL inside the downtime band."""

    def _b(self, prev):
        m = _client()
        m._previous_delivered = prev
        return m

    @pytest.mark.asyncio
    async def test_listed_fill_is_delivered_no_callback(self):
        m = self._b({"stable": {"972944192": 2.0}})
        await C._process_order_event(m, _order(ALIVE + 10))
        m._uncached_fill_callback.assert_not_called()
        info = " ".join(str(c) for c in m._log.info.call_args_list)
        assert "fill predates this session" in info
        assert "already delivered by the previous process" in info
        assert "(delivered list)" in info
        assert not m._log.warning.call_args_list

    @pytest.mark.asyncio
    async def test_fill_not_in_list_stays_downtime_and_fires(self):
        m = self._b({"stable": {"111": 2.0}})
        await C._process_order_event(m, _order(ALIVE + 10))
        m._uncached_fill_callback.assert_called_once()
        assert m._uncached_fill_callback.call_args[0][0]["kind"] == "downtime"

    @pytest.mark.asyncio
    async def test_grown_partial_fires(self):
        m = self._b({"stable": {"972944192": 2.0}})
        await C._process_order_event(m, _order(ALIVE + 10, exec_qty="5"))
        m._uncached_fill_callback.assert_called_once()
        assert m._uncached_fill_callback.call_args[0][0]["kind"] == "downtime"

    @pytest.mark.asyncio
    async def test_other_groups_list_ignored(self):
        m = self._b({"rolling": {"972944192": 2.0}})
        await C._process_order_event(m, _order(ALIVE + 10))
        m._uncached_fill_callback.assert_called_once()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("prev", [{}, None, "garbage"])
    async def test_no_list_crash_or_old_process_stays_loud(self, prev):
        m = self._b(prev)
        await C._process_order_event(m, _order(ALIVE + 10))
        m._uncached_fill_callback.assert_called_once()

    @pytest.mark.asyncio
    async def test_fake_client_without_attribute_stays_loud(self):
        m = _client()
        del m._previous_delivered            # e.g. an agent-repo fake client
        await C._process_order_event(m, _order(ALIVE + 10))
        m._uncached_fill_callback.assert_called_once()

    @pytest.mark.asyncio
    async def test_live_fill_in_list_still_fires(self):
        m = self._b({"stable": {"972944192": 2.0}})
        await C._process_order_event(m, _order(START + 10))
        assert m._uncached_fill_callback.call_args[0][0]["kind"] == "live"


def _node_a(path, cached):
    """Process A: an own cached exit order at the moment it fills."""
    m = _dstub(path, "stable", {})
    m._cache.order.side_effect = None
    m._cache.order.return_value = cached
    m._ts_order_id_to_client_order_id = {"979712839": ClientOrderId("O-EXIT-1")}
    m._client_order_id_to_ts_order_id = {ClientOrderId("O-EXIT-1"): "979712839"}
    m._ts_id_to_group = {"979712839": "stable"}
    m._order_last_status = {}
    m._pending_modify_trigger_price = {}
    m._modify_inflight = {}
    m._pending_amend_meta = {}
    m._cancel_verify_pending = {}
    m._status_watch = {}
    m._partial_orders = {}
    m._emitted_exec_hwm = {}
    m._emitted_cancel_coids = {}
    m._cancel_after_ack_sent = {}
    m._verify_pending_cancels = AsyncMock()
    m._recheck_watched_statuses = MagicMock()
    m._clock = MagicMock()
    m._clock.timestamp_ns.return_value = 0
    m._uncached_fill_callback = MagicMock()
    return m


class TestIncidentReplay:
    @pytest.mark.asyncio
    async def test_fill_seconds_before_clean_stop_is_delivered_next_process(
            self, tmp_path, monkeypatch):
        T = 1794887101.0                               # 2026-11-17 03:45:01Z fill
        p = tmp_path / "map.json"
        p.write_text(json.dumps({"979712839": {"client_order_id": "O-EXIT-1",
                                               "submitted_ts": T - 60, "group": "stable"}}))
        cached = MagicMock()
        cached.is_closed = False
        cached.status = OrderStatus.ACCEPTED
        cached.strategy_id = StrategyId("S-001")
        cached.instrument_id = InstrumentId.from_str("ADZ25.CME")
        cached.quantity = Quantity.from_int(5)
        cached.filled_qty = Quantity.from_int(0)
        cached.trigger_price = None
        cached.price = None
        fll = {"OrderID": "979712839", "Status": "FLL", "ClosedDateTime": _iso(T),
               "FilledPrice": "0.6512",
               "Legs": [{"ExecQuantity": "5", "QuantityOrdered": "5"}]}

        now = {"t": T - 3600}
        monkeypatch.setattr(_time, "time", lambda: now["t"])
        a = _node_a(p, cached)
        C._load_order_map(a)
        now["t"] = T
        await C._process_order_event(a, dict(fll))
        assert a.generate_order_filled.call_count == 1
        cached.filled_qty = Quantity.from_int(5)       # NT applied the fill
        cached.is_closed = True
        now["t"] = T + 9                               # SIGTERM -> _disconnect forced stamp
        C._stamp_alive(a, force=True)
        meta = json.loads(p.read_text())["_meta"]
        assert meta["delivered"]["stable"] == {"979712839": 5.0}

        now["t"] = T + 20                              # process B starts
        b = _client()
        b._order_map_path = str(p)
        b._ts_order_id_to_client_order_id = {}
        b._client_order_id_to_ts_order_id = {}
        b._ts_id_to_group = {}
        b._ts_id_submitted_ts = {}
        b._previous_alive = {}
        b._previous_delivered = {}
        b._alive_loaded = False
        b._instance_started_s = T + 20
        C._load_order_map(b)
        assert b._previous_alive["stable"] == pytest.approx(T + 9)
        await C._process_order_event(b, dict(fll))
        b._uncached_fill_callback.assert_not_called()
        info = " ".join(str(c) for c in b._log.info.call_args_list)
        assert "predates this session" in info and "(delivered list)" in info
        assert not b._log.warning.call_args_list

    @pytest.mark.asyncio
    async def test_same_fill_without_list_is_still_downtime(self, tmp_path):
        """Control: the band alone (crash/SIGKILL, or an old process) stays loud."""
        T = 1794887101.0
        b = _client()
        b._previous_alive = {"stable": T + 9}
        b._previous_delivered = {}
        b._instance_started_s = T + 20
        b._ts_order_id_to_client_order_id = {"979712839": ClientOrderId("O-EXIT-1")}
        b._ts_id_to_group = {"979712839": "stable"}
        fll = {"OrderID": "979712839", "Status": "FLL", "ClosedDateTime": _iso(T),
               "FilledPrice": "0.6512",
               "Legs": [{"ExecQuantity": "5", "QuantityOrdered": "5"}]}
        await C._process_order_event(b, fll)
        assert b._uncached_fill_callback.call_args[0][0]["kind"] == "downtime"
