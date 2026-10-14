"""[FILL-AFTER-ACK-CANCEL]: measure fills that land between the ack and the follow-up cancel.

Residual A of strategies LESSONS §166: a cancel requested before the TradeStation ack is
sent right after the ack (_cancel_after_ack), but the order is live for one HTTP round
trip. A fill in that window changes the position behind the strategy's back (it has
already untracked the exit). Observe-only: the fill is still emitted exactly as before;
an ERROR line makes each occurrence countable before anything heavier is built.
"""
from __future__ import annotations

from unittest.mock import AsyncMock

from nautilus_trader.model.identifiers import VenueOrderId

from tests.test_inflight_and_partials import COID, TS_ID, _fills, _m, ts_order
from tradestation_nt_community.execution import TradeStationExecutionClient as C
from tradestation_nt_community.execution import _emit_fill_delta


def _errors(m):
    return [str(c.args[0]) for c in m._log.error.call_args_list]


def test_fill_on_an_order_cancelled_after_ack_is_logged_and_still_emitted():
    m = _m(qty=1)
    m._emitted_exec_hwm = {}
    m._cancel_after_ack_sent = {COID: None}
    cached = m._cache.order.return_value
    emitted, _, _ = _emit_fill_delta(m, ts_order("FLL", qty=1, exec_qty=1), TS_ID, COID,
                                     cached, VenueOrderId(TS_ID), 7, "sse", partial=False)
    assert emitted is True and len(_fills(m)) == 1          # observe-only: fill unchanged
    assert any("[FILL-AFTER-ACK-CANCEL]" in e for e in _errors(m))


def test_ordinary_fill_is_not_flagged():
    m = _m(qty=1)
    m._emitted_exec_hwm = {}
    m._cancel_after_ack_sent = {}
    cached = m._cache.order.return_value
    _emit_fill_delta(m, ts_order("FLL", qty=1, exec_qty=1), TS_ID, COID, cached,
                     VenueOrderId(TS_ID), 7, "sse", partial=False)
    assert not any("[FILL-AFTER-ACK-CANCEL]" in e for e in _errors(m))


async def test_cancel_after_ack_records_the_order():
    m = _m(qty=1)
    m._cancel_after_ack_coids = {COID: None}
    m._cancel_requested_coids = {}
    m._note_cancel_requested = lambda coid: C._note_cancel_requested(m, coid)
    m._client.cancel_order = AsyncMock(return_value={})
    sent = await C._cancel_after_ack(m, COID, TS_ID)
    assert sent is True
    assert COID in m._cancel_after_ack_sent


def test_sent_map_is_bounded():
    m = _m(qty=1)
    m._cancel_after_ack_sent = {}
    for i in range(600):
        C._remember_cancel_after_ack_sent(m, f"O-{i}")
    assert len(m._cancel_after_ack_sent) == 500
