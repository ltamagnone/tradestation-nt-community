"""Task 2 (2026-10-13): cancel of an FPR remainder withholds OrderCanceled (binding
ruling), Task-1 re-review minors m-1..m-3, requested-cancel filter, [RECON-SHADOW-OWN]."""
from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock
from types import SimpleNamespace as NS

import pytest
from nautilus_trader.model.enums import OrderStatus
from nautilus_trader.model.identifiers import VenueOrderId

from tests.test_inflight_and_partials import (
    COID, IID, TS_ID, _engine, _fills, _m, _poll, _query, _sse, _stub, ts_order)
from tradestation_nt_community.execution import TradeStationExecutionClient as C
from tradestation_nt_community.execution import _track_partial


def _cancel_stub(partial: bool):
    m = MagicMock()
    m._client_order_id_to_ts_order_id = {COID: TS_ID}
    m._partial_orders = {COID: (2, 1)} if partial else {}
    m._client.cancel_order = AsyncMock(return_value=None)
    m._clock.timestamp_ns.return_value = 5
    m._log = MagicMock()
    return m


def _cmd():
    return NS(client_order_id=COID, instrument_id=IID, strategy_id=MagicMock(), )


async def test_cancel_of_partial_order_withholds_ordercanceled():
    m = _cancel_stub(partial=True)
    await C._cancel_order(m, _cmd())
    m._client.cancel_order.assert_awaited_once_with(order_id=TS_ID)
    m.generate_order_canceled.assert_not_called()
    m.generate_order_cancel_rejected.assert_not_called()
    assert COID in m._partial_orders  # the broker's FLP/FLL clears it
    m._note_cancel_requested.assert_called_once_with(COID)
    m._register_cancel_verify.assert_called_once()


async def test_cancel_of_ordinary_order_still_emits_canceled():
    m = _cancel_stub(partial=False)
    await C._cancel_order(m, _cmd())
    m.generate_order_canceled.assert_called_once()


async def test_cancel_all_withholds_only_partial_orders():
    m = MagicMock()
    p, q = MagicMock(), MagicMock()
    p.client_order_id, p.venue_order_id = COID, VenueOrderId("1")
    q.client_order_id, q.venue_order_id = MagicMock(), VenueOrderId("2")
    m._cache.orders_open.return_value = [p, q]
    m._cache.orders_inflight.return_value = []
    m._partial_orders = {COID: (1, 1)}
    m._client.cancel_order = AsyncMock()
    from nautilus_trader.model.enums import OrderSide
    cmd = NS(instrument_id=IID, strategy_id=MagicMock(), order_side=OrderSide.NO_ORDER_SIDE)
    await C._cancel_all_orders(m, cmd)
    assert m._client.cancel_order.await_count == 2
    assert m.generate_order_canceled.call_count == 1
    assert m.generate_order_canceled.call_args.kwargs["client_order_id"] is q.client_order_id


async def test_partial_cancel_then_flp_is_fill_of_executed_then_cancel():
    m = _m(qty=3)
    m._partial_orders = {COID: (2, 1)}
    await _poll(m, ts_order("FLP", exec_qty=2))
    fills = _fills(m)
    assert len(fills) == 1 and float(fills[0]["last_qty"]) == 2
    m.generate_order_canceled.assert_called_once()
    assert m._partial_orders == {}


async def test_partial_cancel_race_lat_then_fll_is_one_full_fill_no_cancel():
    m = _m(qty=3)
    m._partial_orders = {COID: (2, 1)}
    await _poll(m, ts_order("LAT", exec_qty=2))  # the cancel was too late
    assert _fills(m) == []
    m.generate_order_canceled.assert_not_called()
    await _poll(m, ts_order("FLL", exec_qty=3))
    fills = _fills(m)
    assert len(fills) == 1 and float(fills[0]["last_qty"]) == 3
    m.generate_order_canceled.assert_not_called()


# ------------------------------------------------------------------ minors m-1..m-3
def test_m1_inflight_recovery_failure_returns_none():
    loop, _, cache, order = _engine("accepted")
    order.apply(__import__("nautilus_trader.test_kit.stubs.events", fromlist=["x"])
                .TestEventStubs.order_pending_update(order))
    from nautilus_trader.model.objects import Price
    s = _stub(cache, [ts_order("OPN")])
    s._pending_modify_trigger_price = {order.client_order_id: Price.from_str("99.0")}
    s._check_order_statuses = AsyncMock(side_effect=RuntimeError("HTTP 500"))
    assert _query(loop, s) is None  # no exception out of generate_order_status_report


@pytest.mark.parametrize("deliver", [_sse, _poll])
async def test_m2_fill_emit_raise_un_advances_status_so_next_read_retries(deliver):
    m = _m(qty=3)
    m.generate_order_filled.side_effect = RuntimeError("boom")
    try:
        await deliver(m, ts_order("FLL", exec_qty=3))
    except RuntimeError:
        pass
    assert TS_ID not in m._order_last_status
    m.generate_order_filled.side_effect = None
    await deliver(m, ts_order("FLL", exec_qty=3))
    assert len(_fills(m)) == 2 and m._order_last_status[TS_ID] == "FLL"  # retried (1 failed + 1 ok)


@pytest.mark.parametrize("deliver", [_sse, _poll])
async def test_m2_flp_emit_raise_un_advances_status(deliver):
    m = _m(qty=3)
    m.generate_order_filled.side_effect = RuntimeError("boom")
    await deliver(m, ts_order("FLP", exec_qty=2))
    assert TS_ID not in m._order_last_status


def test_m3_partial_qty_summed_over_legs():
    m = MagicMock()
    m._partial_orders = {}
    o = ts_order("FPR", qty=2, exec_qty=1)
    o["Legs"].append({"QuantityOrdered": "1", "ExecQuantity": "1"})
    _track_partial(m, o, COID, 5)
    msg = m._log.warning.call_args.args[0]
    assert "[PARTIAL-FILL]" in msg and "2/3" in msg


@pytest.mark.parametrize("deliver", [_sse, _poll])
async def test_m3_stale_partial_cleared_when_nt_order_closed_and_status_unchanged(deliver):
    m = _m(qty=3)
    await deliver(m, ts_order("FPR", exec_qty=1))
    assert COID in m._partial_orders
    m._cache.order.return_value.is_closed = True  # closed by NT meanwhile, broker still FPR
    await deliver(m, ts_order("FPR", exec_qty=1))
    assert m._partial_orders == {}


# ------------------------------------------------------- requested-cancel filter (shared)
def test_requested_cancel_filter_is_the_single_helper():
    s = MagicMock()
    s._cancel_was_requested = lambda c: False
    out = C._requested_cancel_filter(s, {"Status": "CAN"}, COID, TS_ID)
    assert out["Status"] == "ACK"
    s._cancel_was_requested = lambda c: True
    assert C._requested_cancel_filter(s, {"Status": "OUT"}, COID, TS_ID)["Status"] == "OUT"
    assert C._requested_cancel_filter(s, {"Status": "OPN"}, COID, TS_ID)["Status"] == "OPN"


# ------------------------------------------------------------------ shadow mode
def _shadow_client(order, requested=False):
    s = MagicMock()
    s._cache.order.return_value = order
    s._log = MagicMock()
    s._shadow_own_sig = {}
    s._cancel_was_requested = lambda c: requested
    s._remember_shadow_sig = lambda c, g: C._remember_shadow_sig(s, c, g)
    return s


def _lines(s):
    return [c.args[0] for c in s._log.info.call_args_list
            if "[RECON-SHADOW-OWN]" in str(c.args[0])]


def test_shadow_logs_one_json_line_when_broker_differs_and_emits_nothing():
    _, _, cache, order = _engine("accepted")
    s = _shadow_client(order, requested=True)
    C._shadow_own_order(s, ts_order("FLP", exec_qty=2), order.client_order_id, TS_ID)
    (line,) = _lines(s)
    rec = json.loads(line.split("[RECON-SHADOW-OWN] ", 1)[1])
    assert rec["status"] == "FLP" and rec["exec_qty"] == "2" and rec["filled_qty"] == "0"
    assert rec["avg_px_present"] is True and rec["requested_cancel"] is True
    assert rec["cache_status"] == "ACCEPTED"
    # dedup: the same state again logs nothing more
    C._shadow_own_order(s, ts_order("FLP", exec_qty=2), order.client_order_id, TS_ID)
    assert len(_lines(s)) == 1
    for name in ("generate_order_filled", "generate_order_canceled", "generate_order_rejected",
                 "generate_order_updated", "generate_order_status_report"):
        getattr(s, name).assert_not_called()


def test_shadow_silent_when_broker_agrees_with_cache():
    _, _, cache, order = _engine("accepted")
    s = _shadow_client(order)
    C._shadow_own_order(s, ts_order("OPN", exec_qty=0), order.client_order_id, TS_ID)
    assert _lines(s) == []


def test_shadow_avg_px_absent_flag():
    _, _, cache, order = _engine("accepted")
    s = _shadow_client(order)
    C._shadow_own_order(s, ts_order("FLL", exec_qty=3, price="0"), order.client_order_id, TS_ID)
    assert json.loads(_lines(s)[0].split("] ", 1)[1])["avg_px_present"] is False


async def test_mass_status_reports_stay_withheld_and_log_shadow():
    _, _, cache, order = _engine("accepted")
    s = MagicMock()
    s._client.get_orders = AsyncMock(return_value=[ts_order("FLL", exec_qty=3)])
    s._account_id = "SIM0000001F"
    s._ts_order_id_to_client_order_id = {TS_ID: order.client_order_id}
    s._is_own_cached_order = lambda c, t=None: True
    s._shadow_own_order = lambda o, c, t: C._shadow_own_order(s, o, c, t)
    s._cache = cache
    s._log = MagicMock()
    s._shadow_own_sig = {}
    s._cancel_was_requested = lambda c: False
    s._remember_shadow_sig = lambda c, g: C._remember_shadow_sig(s, c, g)
    cmd = NS(instrument_id=None, open_only=False)
    assert await C.generate_order_status_reports(s, cmd) == []
    assert len(_lines(s)) == 1
    s._parse_order_status_report.assert_not_called()


def test_report_own_orders_flag_defaults_false_and_is_unused():
    from tradestation_nt_community.config import TradeStationExecClientConfig
    assert TradeStationExecClientConfig(account_id="x").report_own_orders is False


# ------------------------------------------------------- I-2: CAN/OUT/EXP with executed qty
@pytest.mark.parametrize("deliver", [_sse, _poll])
@pytest.mark.parametrize("code", ["CAN", "OUT", "EXP"])
async def test_cancel_with_executed_qty_delivers_fill_before_cancel(deliver, code):
    m = _m(qty=3)
    order_of_events = []
    m.generate_order_filled.side_effect = lambda **k: order_of_events.append("fill")
    m.generate_order_canceled.side_effect = lambda **k: order_of_events.append("cancel")
    await deliver(m, ts_order(code, exec_qty=2))
    assert order_of_events == ["fill", "cancel"]
    assert float(_fills(m)[0]["last_qty"]) == 2


@pytest.mark.parametrize("deliver", [_sse, _poll])
async def test_cancel_without_executed_qty_has_no_fill(deliver):
    m = _m(qty=3)
    await deliver(m, ts_order("CAN", exec_qty=0))
    assert _fills(m) == []
    m.generate_order_canceled.assert_called_once()


@pytest.mark.parametrize("deliver", [_sse, _poll])
async def test_cancel_with_executed_qty_but_no_price_retries_without_cancelling(deliver):
    m = _m(qty=3)
    m._cache.order.return_value.price = None
    await deliver(m, ts_order("CAN", exec_qty=2, price=None, LimitPrice="0"))
    m.generate_order_canceled.assert_not_called()
    assert TS_ID not in m._order_last_status


def test_inflight_requested_cancel_with_executed_qty_returns_none():
    loop, _, cache, _ = _engine("pending_cancel")
    assert _query(loop, _stub(cache, [ts_order("CAN", exec_qty=2)], requested=True)) is None
    # nothing executed: the requested cancel still reports CANCELED
    rep = _query(loop, _stub(cache, [ts_order("CAN", exec_qty=0)], requested=True))
    assert rep.order_status == OrderStatus.CANCELED


def test_shadow_signature_dict_is_bounded():
    s = MagicMock()
    s._shadow_own_sig = {}
    for i in range(2105):
        C._remember_shadow_sig(s, i, "x")
    assert len(s._shadow_own_sig) == 2000 and 0 not in s._shadow_own_sig
