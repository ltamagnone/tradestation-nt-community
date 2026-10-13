"""Task 1 (2026-10-13): F4 in-flight broker read, F1 executed qty, price fallback, F3
ClosedDateTime, FLP terminal fill, FPR tracking."""
from __future__ import annotations

import asyncio
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock, MagicMock

import pytest
from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock, MessageBus
from nautilus_trader.common.factories import OrderFactory
from nautilus_trader.config import LiveExecEngineConfig
from nautilus_trader.live.execution_engine import LiveExecutionEngine
from nautilus_trader.model.enums import OrderSide, OrderStatus
from nautilus_trader.model.identifiers import (AccountId, ClientOrderId, InstrumentId, StrategyId,
                                                TraderId, VenueOrderId)
from nautilus_trader.model.objects import Price, Quantity
from nautilus_trader.test_kit.stubs.events import TestEventStubs

from tests.test_kit import TSTestInstrumentStubs
from tradestation_nt_community.execution import TradeStationExecutionClient as C
from tradestation_nt_community.execution import _emit_fill_delta

ACCT = AccountId("TRADESTATION-SIM0000001F")
IID = InstrumentId.from_str("GCJ26.TRADESTATION")
TS_ID = "286179863"
COID = ClientOrderId("O-1")
CLOSED = "2026-10-13T10:00:00Z"
CLOSED_NS = 1_791_885_600_000_000_000  # 2026-10-13T10:00:00Z


def ts_order(status, qty=3, exec_qty=0, price="100.0", **extra):
    o = {
        "OrderID": TS_ID, "Status": status, "OrderType": "Limit", "LimitPrice": "100.0",
        "Duration": "GTC",
        "Legs": [{"QuantityOrdered": str(qty), "ExecQuantity": str(exec_qty),
                  "BuyOrSell": "Buy", "Symbol": "GCJ26"}],
    }
    if price is not None:
        o["FilledPrice"] = price
    o.update(extra)
    return o


# ---------------------------------------------------------------- real-engine F4 tests
def _engine(state="submitted", qty=3):
    loop = asyncio.new_event_loop()
    clock = LiveClock()
    trader = TraderId("TESTER-001")
    cache = Cache()
    cache.add_instrument(TSTestInstrumentStubs.gc_futures_contract())
    eng = LiveExecutionEngine(
        loop=loop, msgbus=MessageBus(trader_id=trader, clock=clock), cache=cache, clock=clock,
        config=LiveExecEngineConfig(reconciliation=True, filter_unclaimed_external_orders=True))
    order = OrderFactory(trader_id=trader, strategy_id=StrategyId("S-001"), clock=clock).limit(
        IID, OrderSide.BUY, Quantity.from_int(qty), Price.from_str("100.0"))
    cache.add_order(order)
    order.apply(TestEventStubs.order_submitted(order))
    if state in ("accepted", "pending_cancel"):
        order.apply(TestEventStubs.order_accepted(order, venue_order_id=VenueOrderId(TS_ID)))
    if state == "pending_cancel":
        order.apply(TestEventStubs.order_pending_cancel(order))
    cache.update_order(order)
    return loop, eng, cache, order


def _stub(cache, orders=None, fail=False, requested=False):
    coid = next(iter(cache.orders())).client_order_id
    s = MagicMock()
    s._account_id = "SIM0000001F"
    s._account_id_nautilus = ACCT
    s._clock.timestamp_ns = lambda: 5
    s._cache = cache
    s._log = MagicMock()
    s._ts_order_id_to_client_order_id = {TS_ID: coid}
    s._client_order_id_to_ts_order_id = {coid: TS_ID}
    s._cancel_verify_pending = {}
    s._pending_modify_trigger_price = {}
    s._check_order_statuses = AsyncMock()
    s._cancel_requested_coids = {coid: None} if requested else {}
    if fail:
        s._client.get_orders = AsyncMock(side_effect=RuntimeError("HTTP 500"))
    else:
        s._client.get_orders = AsyncMock(return_value=orders or [])
    from tradestation_nt_community.parsing.execution import parse_order_status_report
    now = CLOSED_NS + 10**9
    s._parse_order_status_report = lambda o, i, c: parse_order_status_report(o, i, c, ACCT, now)
    s._is_own_cached_order = lambda c, t=None: True
    s._cancel_was_requested = lambda c: C._cancel_was_requested(s, c)
    s._requested_cancel_filter = lambda r, c, t: C._requested_cancel_filter(s, r, c, t)
    s._inflight_status_report = lambda i, c, t: C._inflight_status_report(s, i, c, t)
    return s


def _query(loop, s):
    coid = next(iter(s._client_order_id_to_ts_order_id))
    cmd = NS(instrument_id=IID, client_order_id=coid, venue_order_id=VenueOrderId(TS_ID))
    return loop.run_until_complete(C.generate_order_status_report(s, cmd))


def test_submitted_plus_broker_ack_is_accepted_never_rejected():
    loop, eng, cache, order = _engine("submitted")
    for code in ("ACK", "OPN", "DON", "UCN", "LAT", "PLA", "FPR"):
        rep = _query(loop, _stub(cache, [ts_order(code, exec_qty=1 if code == "FPR" else 0)]))
        assert rep.order_status == OrderStatus.ACCEPTED, code
    eng._reconcile_order_report(rep, [])
    assert order.status == OrderStatus.ACCEPTED


def test_0921_pending_cancel_with_broker_still_open_is_not_canceled():
    loop, eng, cache, order = _engine("pending_cancel")
    assert order.status == OrderStatus.PENDING_CANCEL
    rep = _query(loop, _stub(cache, [ts_order("OPN")]))  # cancel HTTP 500, broker still OPN
    assert rep.order_status == OrderStatus.ACCEPTED
    eng._reconcile_order_report(rep, [])
    assert order.status == OrderStatus.ACCEPTED and order.status != OrderStatus.CANCELED


def test_broker_read_failure_returns_none():
    loop, _, cache, _ = _engine("submitted")
    assert _query(loop, _stub(cache, fail=True)) is None


def test_order_missing_from_broker_list_returns_none():
    loop, _, cache, _ = _engine("submitted")
    assert _query(loop, _stub(cache, [{"OrderID": "other", "Status": "OPN"}])) is None


def test_rej_is_rejected():
    loop, _, cache, _ = _engine("submitted")
    rep = _query(loop, _stub(cache, [ts_order("REJ")]))
    assert rep.order_status == OrderStatus.REJECTED


@pytest.mark.parametrize("code,expect", [
    ("CAN", OrderStatus.CANCELED), ("OUT", OrderStatus.CANCELED), ("EXP", OrderStatus.EXPIRED)])
def test_terminal_cancel_codes_only_when_we_requested(code, expect):
    loop, _, cache, _ = _engine("pending_cancel")
    rep = _query(loop, _stub(cache, [ts_order(code)], requested=True))
    assert rep.order_status == expect
    s = _stub(cache, [ts_order(code)], requested=False)
    rep = _query(loop, s)
    assert rep.order_status == OrderStatus.ACCEPTED
    assert any("[CANCEL-MISMATCH]" in str(c) for c in s._log.error.call_args_list)


def test_fll_report_carries_executed_qty_and_flp_partial():
    loop, _, cache, _ = _engine("submitted")
    rep = _query(loop, _stub(cache, [ts_order("FLL", exec_qty=3)]))
    assert rep.order_status == OrderStatus.FILLED and rep.filled_qty == Quantity.from_int(3)
    rep = _query(loop, _stub(cache, [ts_order("FLP", exec_qty=2)]))
    assert rep.order_status == OrderStatus.PARTIALLY_FILLED
    assert rep.filled_qty == Quantity.from_int(2)
    assert _query(loop, _stub(cache, [ts_order("FLP", exec_qty=0)])) is None


def test_cancel_was_requested_uses_registry_and_attempt_marker():
    s = MagicMock()
    s._cancel_requested_coids = {}
    s._client_order_id_to_ts_order_id = {COID: TS_ID}
    s._cancel_verify_pending = {}
    assert C._cancel_was_requested(s, COID) is False
    s._cancel_verify_pending = {TS_ID: (0.0, 0, False, False)}
    assert C._cancel_was_requested(s, COID) is False  # observed, not requested
    s._cancel_verify_pending = {TS_ID: (0.0, 0, True, False)}
    assert C._cancel_was_requested(s, COID) is True
    s._cancel_verify_pending = {}
    C._note_cancel_requested(s, COID)
    assert C._cancel_was_requested(s, COID) is True


# ---------------------------------------------------------------- fills / FLP / FPR
def _m(qty=3, filled=0):
    cached = MagicMock()
    cached.is_closed = False
    cached.status = OrderStatus.ACCEPTED
    cached.strategy_id = StrategyId("S-001")
    cached.instrument_id = IID
    cached.quantity = Quantity.from_int(qty)
    cached.filled_qty = Quantity.from_int(filled)
    cached.trigger_price = None
    cached.price = None
    m = MagicMock()
    m._ts_order_id_to_client_order_id = {TS_ID: COID}
    m._order_last_status = {}
    m._pending_modify_trigger_price = {}
    m._cancel_verify_pending = {}
    m._status_watch = {}
    m._partial_orders = {}
    m._verify_pending_cancels = AsyncMock()
    m._recheck_watched_statuses = MagicMock()
    m._cache = MagicMock()
    m._cache.order.return_value = cached
    m._clock = MagicMock()
    m._clock.timestamp_ns.return_value = CLOSED_NS + 10**9
    m._log = MagicMock()
    return m


async def _sse(m, o):
    await C._process_order_event(m, o)


async def _poll(m, o):
    m._client = MagicMock()
    m._client.get_orders = AsyncMock(return_value=[o])
    await C._check_order_statuses(m)


def _fills(m):
    return [c.kwargs for c in m.generate_order_filled.call_args_list]


@pytest.mark.parametrize("deliver", [_sse, _poll])
class TestFills:
    async def test_fll_uses_executed_qty_not_ordered(self, deliver):
        m = _m(qty=3)
        await deliver(m, ts_order("FLL", exec_qty=3))
        f = _fills(m)
        assert len(f) == 1 and f[0]["last_qty"] == Quantity.from_int(3)

    async def test_fll_is_a_delta_against_filled_qty(self, deliver):
        m = _m(qty=3, filled=1)
        await deliver(m, ts_order("FLL", exec_qty=3))
        assert _fills(m)[0]["last_qty"] == Quantity.from_int(2)

    async def test_fll_exec_summed_over_legs(self, deliver):
        m = _m(qty=3)
        o = ts_order("FLL", exec_qty=2)
        o["Legs"].append({"QuantityOrdered": "1", "ExecQuantity": "1", "BuyOrSell": "Buy"})
        await deliver(m, o)
        assert _fills(m)[0]["last_qty"] == Quantity.from_int(3)

    async def test_never_a_zero_or_negative_fill(self, deliver):
        m = _m(qty=3, filled=3)  # already fully applied
        await deliver(m, ts_order("FLL", exec_qty=3))
        assert _fills(m) == []
        m = _m(qty=3, filled=3)
        await deliver(m, ts_order("FLL", exec_qty=2))  # stale, smaller than applied
        assert _fills(m) == []

    async def test_price_falls_back_to_leg_execution_price(self, deliver):
        for fp in ("0", "0.0", "", None):
            m = _m()
            o = ts_order("FLL", exec_qty=3, price=fp)
            o["Legs"][0]["ExecutionPrice"] = "101.5"
            await deliver(m, o)
            assert float(_fills(m)[0]["last_px"]) == 101.5, fp

    async def test_no_price_anywhere_logs_error_and_skips(self, deliver):
        m = _m()
        o = ts_order("FLL", exec_qty=3, price="0")
        await deliver(m, o)
        assert _fills(m) == []
        assert m._log.error.called

    async def test_f3_closed_datetime_else_now(self, deliver):
        m = _m()
        await deliver(m, ts_order("FLL", exec_qty=3, ClosedDateTime=CLOSED))
        assert _fills(m)[0]["ts_event"] == CLOSED_NS
        m = _m()
        await deliver(m, ts_order("FLL", exec_qty=3))
        assert _fills(m)[0]["ts_event"] == m._clock.timestamp_ns.return_value

    async def test_f3_cancel_and_reject_events_use_closed_datetime(self, deliver):
        m = _m()
        await deliver(m, ts_order("OUT", ClosedDateTime=CLOSED))
        assert m.generate_order_canceled.call_args.kwargs["ts_event"] == CLOSED_NS
        m = _m()
        await deliver(m, ts_order("REJ", ClosedDateTime=CLOSED))
        assert m.generate_order_rejected.call_args.kwargs["ts_event"] == CLOSED_NS


@pytest.mark.parametrize("deliver", [_sse, _poll])
class TestFlp:
    async def test_flp_2_of_3_is_one_fill_of_2_then_cancel(self, deliver):
        m = _m(qty=3)
        await deliver(m, ts_order("FLP", exec_qty=2, ClosedDateTime=CLOSED))
        f = _fills(m)
        assert len(f) == 1 and f[0]["last_qty"] == Quantity.from_int(2)
        m.generate_order_canceled.assert_called_once()
        names = [c[0] for c in m.mock_calls if c[0] in ("generate_order_filled",
                                                         "generate_order_canceled")]
        assert names == ["generate_order_filled", "generate_order_canceled"]
        m._watch_status.assert_not_called()

    async def test_flp_after_earlier_delivery_is_only_the_delta(self, deliver):
        m = _m(qty=3, filled=1)
        await deliver(m, ts_order("FLP", exec_qty=2))
        assert _fills(m)[0]["last_qty"] == Quantity.from_int(1)
        m.generate_order_canceled.assert_called_once()

    async def test_flp_fully_executed_has_no_remainder_cancel(self, deliver):
        m = _m(qty=3)
        await deliver(m, ts_order("FLP", exec_qty=3))
        assert _fills(m)[0]["last_qty"] == Quantity.from_int(3)
        m.generate_order_canceled.assert_not_called()

    async def test_flp_without_qty_keeps_pending_path(self, deliver):
        m = _m(qty=3)
        await deliver(m, ts_order("FLP", exec_qty=""))
        assert _fills(m) == []
        m.generate_order_canceled.assert_not_called()
        m._watch_status.assert_called_once()
        assert "FLP-NO-QTY" in m._watch_status.call_args.kwargs["note"]


@pytest.mark.parametrize("deliver", [_sse, _poll])
class TestFpr:
    async def test_fpr_records_warns_once_emits_nothing(self, deliver):
        m = _m(qty=3)
        await deliver(m, ts_order("FPR", exec_qty=1))
        assert m._partial_orders == {COID: (1, m._clock.timestamp_ns.return_value)}
        warns = [c for c in m._log.warning.call_args_list if "[PARTIAL-FILL]" in str(c)]
        assert len(warns) == 1 and f"{COID} 1/3" in str(warns[0])
        m.generate_order_filled.assert_not_called()
        m.generate_order_canceled.assert_not_called()
        m.generate_order_rejected.assert_not_called()
        # more fills, same FPR status: qty updates, since_ns kept, still one warning
        m._clock.timestamp_ns.return_value += 10
        await deliver(m, ts_order("FPR", exec_qty=2))
        assert m._partial_orders[COID][0] == 2
        assert m._partial_orders[COID][1] == CLOSED_NS + 10**9
        assert len([c for c in m._log.warning.call_args_list if "[PARTIAL-FILL]" in str(c)]) == 1

    @pytest.mark.parametrize("terminal", [("FLL", 3), ("FLP", 2), ("OUT", 0), ("REJ", 0)])
    async def test_cleared_on_terminal_status(self, deliver, terminal):
        m = _m(qty=3)
        await deliver(m, ts_order("FPR", exec_qty=1))
        assert COID in m._partial_orders
        await deliver(m, ts_order(terminal[0], exec_qty=terminal[1]))
        assert COID not in m._partial_orders

    async def test_cleared_when_nt_order_already_closed(self, deliver):
        m = _m(qty=3)
        await deliver(m, ts_order("FPR", exec_qty=1))
        m._cache.order.return_value.is_closed = True
        await deliver(m, ts_order("FLL", exec_qty=3))
        assert COID not in m._partial_orders


def test_partial_orders_accessor_is_a_copy():
    s = MagicMock()
    s._partial_orders = {COID: (1, 7)}
    out = C.partial_orders(s)
    assert out == {COID: (1, 7)}
    out.clear()
    assert s._partial_orders == {COID: (1, 7)}


# ---------------------------------------------------------------- fix round 1
@pytest.mark.parametrize("deliver", [_sse, _poll])
class TestPriceResolver:
    async def test_falls_back_to_the_orders_own_price_with_warning(self, deliver):
        m = _m()
        m._cache.order.return_value.price = Price.from_str("99.5")
        await deliver(m, ts_order("FLL", exec_qty=3, price="0"))
        assert float(_fills(m)[0]["last_px"]) == 99.5
        assert any("[FILL-PRICE-FALLBACK]" in str(c) for c in m._log.warning.call_args_list)

    async def test_stop_trigger_price_is_preferred_own_price(self, deliver):
        m = _m()
        m._cache.order.return_value.trigger_price = Price.from_str("98.0")
        m._cache.order.return_value.price = None
        await deliver(m, ts_order("FLL", exec_qty=3, price="0"))
        assert float(_fills(m)[0]["last_px"]) == 98.0

    async def test_no_price_anywhere_retries_next_status_read(self, deliver):
        m = _m()
        await deliver(m, ts_order("FLL", exec_qty=3, price="0"))
        assert _fills(m) == [] and TS_ID not in m._order_last_status
        assert any("[FILL-NO-PRICE]" in str(c) for c in m._log.error.call_args_list)
        await deliver(m, ts_order("FLL", exec_qty=3, price="101.0"))  # price shows up
        assert len(_fills(m)) == 1

    async def test_flp_without_price_keeps_order_open_and_retries(self, deliver):
        m = _m()
        await deliver(m, ts_order("FLP", exec_qty=2, price="0"))
        assert _fills(m) == []
        m.generate_order_canceled.assert_not_called()
        assert TS_ID not in m._order_last_status
        await deliver(m, ts_order("FLP", exec_qty=2, price="101.0"))
        assert _fills(m)[0]["last_qty"] == Quantity.from_int(2)
        m.generate_order_canceled.assert_called_once()


def test_inflight_fill_report_never_carries_px_zero():
    loop, _, cache, order = _engine("accepted")
    o = ts_order("FLL", exec_qty=3, price="0")
    o["Legs"][0]["ExecutionPrice"] = "101.5"
    rep = _query(loop, _stub(cache, [o]))
    assert float(rep.avg_px) == 101.5
    o = ts_order("FLP", exec_qty=2, price="0")  # nothing at TS: the limit price is the fallback
    rep = _query(loop, _stub(cache, [o]))
    assert float(rep.avg_px) == 100.0
    s = _stub(cache, [o])
    s._cache = MagicMock()
    s._cache.order.return_value = None  # no own price either
    assert _query(loop, s) is None


def test_inflight_pending_update_runs_modify_recovery_instead_of_answering():
    loop, _, cache, order = _engine("accepted")
    order.apply(TestEventStubs.order_pending_update(order))
    assert order.status == OrderStatus.PENDING_UPDATE
    s = _stub(cache, [ts_order("OPN")])
    s._pending_modify_trigger_price = {order.client_order_id: Price.from_str("99.0")}
    assert _query(loop, s) is None
    s._check_order_statuses.assert_awaited_once()
    # no pending amend -> a normal ACCEPTED answer
    s = _stub(cache, [ts_order("OPN")])
    assert _query(loop, s).order_status == OrderStatus.ACCEPTED
    # broker already terminal -> answer even with a pending amend
    s = _stub(cache, [ts_order("FLL", exec_qty=3)])
    s._pending_modify_trigger_price = {order.client_order_id: Price.from_str("99.0")}
    assert _query(loop, s).order_status == OrderStatus.FILLED
    s._check_order_statuses.assert_not_awaited()


def test_status_report_ts_last_uses_closed_datetime():
    loop, _, cache, _ = _engine("pending_cancel")
    rep = _query(loop, _stub(cache, [ts_order("CAN", ClosedDateTime=CLOSED)], requested=True))
    assert rep.ts_last == CLOSED_NS and rep.ts_init == CLOSED_NS + 10**9
    rep = _query(loop, _stub(cache, [ts_order("OPN")]))
    assert rep.ts_last == CLOSED_NS + 10**9


def test_inflight_filled_qty_sums_legs():
    loop, _, cache, _ = _engine("accepted")
    o = ts_order("FLP", exec_qty=1)
    o["Legs"].append({"QuantityOrdered": "0", "ExecQuantity": "1", "BuyOrSell": "Buy"})
    rep = _query(loop, _stub(cache, [o]))
    assert rep.filled_qty == Quantity.from_int(2)


@pytest.mark.parametrize("code,qty", [("FLP", 2), ("FLL", 3)])
def test_inflight_report_then_adapter_fill_does_not_double_deliver(code, qty):
    """Real engine: NT infers the fill from the in-flight report; the adapter's later
    SSE/poll FLL/FLP then finds nothing left to deliver (dedup by quantity)."""
    loop, eng, cache, order = _engine("pending_cancel")
    rep = _query(loop, _stub(cache, [ts_order(code, exec_qty=qty)]))
    eng._reconcile_order_report(rep, [])
    assert order.filled_qty == Quantity.from_int(qty)
    client = _m()
    client._cache = cache
    emitted, exec_total, reason = _emit_fill_delta(
        client, ts_order(code, exec_qty=qty), TS_ID, order.client_order_id, order,
        VenueOrderId(TS_ID), 7, "sse", partial=(code == "FLP"))
    assert emitted is False and reason == "already_applied"
    client.generate_order_filled.assert_not_called()


@pytest.mark.parametrize("deliver", [_sse, _poll])
class TestFprOwnOpenOnly:
    async def test_not_tracked_for_closed_or_uncached(self, deliver):
        m = _m()
        m._cache.order.return_value.is_closed = True
        await deliver(m, ts_order("FPR", exec_qty=1))
        assert m._partial_orders == {}
        m = _m()
        m._cache.order.return_value = None
        await deliver(m, ts_order("FPR", exec_qty=1))
        assert m._partial_orders == {}


# ---------------------------------------------------------------- M-7 same-drain double fill
class TestEmittedHwm:
    async def test_two_emits_before_the_engine_applies_the_first(self):
        """SSE FLP then a poll CAN-with-exec in one drain: filled_qty still lags."""
        m = _m(qty=3)
        m._emitted_exec_hwm = {}
        cached = m._cache.order.return_value          # filled_qty stays 0: engine hasn't run
        for st_code, partial in (("FLP", True), ("CAN", True)):
            _emit_fill_delta(m, ts_order(st_code, exec_qty=2), TS_ID, COID, cached,
                             VenueOrderId(TS_ID), 7, "t", partial=partial)
        f = _fills(m)
        assert len(f) == 1 and f[0]["last_qty"] == Quantity.from_int(2)

    async def test_later_growth_is_only_the_increment(self):
        m = _m(qty=3)
        m._emitted_exec_hwm = {}
        cached = m._cache.order.return_value
        _emit_fill_delta(m, ts_order("FLP", exec_qty=1), TS_ID, COID, cached,
                         VenueOrderId(TS_ID), 7, "t", partial=True)
        _emit_fill_delta(m, ts_order("FLL", exec_qty=3), TS_ID, COID, cached,
                         VenueOrderId(TS_ID), 8, "t", partial=False)
        assert [c["last_qty"] for c in _fills(m)] == [Quantity.from_int(1), Quantity.from_int(2)]

    async def test_cleared_once_nt_shows_the_order_closed(self):
        m = _m(qty=3)
        m._emitted_exec_hwm = {COID: 2}
        m._cache.order.return_value.is_closed = True
        await _sse(m, ts_order("CAN", exec_qty=2))
        assert m._emitted_exec_hwm == {}
