"""§268 Part A: fast-fill probe for MARKET orders.

The TS order stream can stay connected while it delivers no order events (11-05: an MNQ
MARKET fill seen 35 s late, by the safety poll). ``fast_fill_probe_secs`` after a MARKET
accept, the adapter reads that one order only if its outcome was not already processed;
a fill found there is booked (``fill-source=fast-probe``), ``[ORDER-STREAM-SILENT]`` is
logged, a catch-up poll runs and the order stream is reconnected (at most once per 60 s).
"""
from __future__ import annotations

import asyncio
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock, MagicMock

import pytest
from nautilus_trader.model.enums import OrderStatus, OrderType
from nautilus_trader.model.identifiers import ClientOrderId, InstrumentId, StrategyId
from nautilus_trader.model.objects import Quantity

import tradestation_nt_community.execution as ex
from tradestation_nt_community.config import TradeStationExecClientConfig
from tradestation_nt_community.execution import TradeStationExecutionClient as C
from tradestation_nt_community.http.client import BrokerReadError
from tests.test_kit import make_market_order
from tests.test_late_accept_cancel import _group_stub
from tests.test_order_attribution import _build_submit_command, _make_submit_order_stub

IID = InstrumentId.from_str("MNQZ26.TRADESTATION")
TS_ID = "1001"
COID = ClientOrderId("O-MKT")
DELAY = 0.01


def row(status, exec_qty=1, price="21000.25"):
    return {
        "OrderID": TS_ID, "Status": status, "OrderType": "Market", "FilledPrice": price,
        "Legs": [{"QuantityOrdered": "1", "ExecQuantity": str(exec_qty),
                  "BuyOrSell": "Buy", "Symbol": "MNQZ26"}],
    }


def _client(by_id=None, poll_rows=None):
    """A stub execution client with the real probe/event/poll methods' state."""
    cached = MagicMock()
    cached.is_closed = False
    cached.status = OrderStatus.ACCEPTED
    cached.strategy_id = StrategyId("S-001")
    cached.instrument_id = IID
    cached.quantity = Quantity.from_int(1)
    cached.filled_qty = Quantity.from_int(0)
    cached.trigger_price = None
    cached.price = None
    m = MagicMock()
    m._loop = asyncio.get_running_loop()
    m._account_id = "SIM1"
    m._ts_order_id_to_client_order_id = {TS_ID: COID}
    m._order_last_status = {}
    m._pending_modify_trigger_price = {}
    m._modify_inflight = {}
    m._pending_amend_meta = {}
    m._cancel_verify_pending = {}
    m._status_watch = {}
    m._partial_orders = {}
    m._emitted_exec_hwm = {}
    m._verify_pending_cancels = AsyncMock()
    m._recheck_watched_statuses = MagicMock()
    m._cache = MagicMock()
    m._cache.order.return_value = cached
    m._clock = MagicMock()
    m._clock.timestamp_ns.return_value = 1_800_000_000_000_000_000
    m._log = MagicMock()
    m._fast_fill_probe_secs = DELAY
    m._fast_fill_probes = {}
    m._stream_terminal_seen = {}
    m._disconnecting = False
    m._last_forced_order_reconnect = None
    m._FORCED_RECONNECT_MIN_S = C._FORCED_RECONNECT_MIN_S
    m._stream_client = MagicMock()
    m._stream_client.request_reconnect = MagicMock(return_value=True)
    m._client = MagicMock()
    m._client.get_orders_by_id = AsyncMock(return_value=by_id if by_id is not None else [])
    m._client.get_orders = AsyncMock(return_value=poll_rows or [])
    m._check_order_statuses = AsyncMock()
    return m


def _market():
    return NS(order_type=OrderType.MARKET, client_order_id=COID)


async def _run_probe(m):
    C._schedule_fast_fill_probe(m, _market(), TS_ID)
    tasks = list(m._fast_fill_probes.values())
    assert len(tasks) == 1
    await asyncio.gather(*tasks)


def _fills(m):
    return m.generate_order_filled.call_args_list


def _logs(fn):
    return [str(c.args[0]) for c in fn.call_args_list]


# ------------------------------------------------------------------- config / wiring
def test_config_default_is_five_seconds():
    assert TradeStationExecClientConfig(account_id="X").fast_fill_probe_secs == 5.0


def test_factory_passes_the_setting_through():
    import inspect

    from tradestation_nt_community import factories
    assert "fast_fill_probe_secs=config.fast_fill_probe_secs" in inspect.getsource(factories)
    assert inspect.signature(C.__init__).parameters["fast_fill_probe_secs"].default == 5.0


# ------------------------------------------------------------------------- behaviour
async def test_probe_noop_when_stream_filled_first():
    m = _client(by_id=[row("FLL")])
    C._schedule_fast_fill_probe(m, _market(), TS_ID)
    await C._process_order_event(m, row("FLL"))        # the stream delivers in < 1 s
    await asyncio.gather(*m._fast_fill_probes.values())
    m._client.get_orders_by_id.assert_not_awaited()     # healthy stream: no broker call
    assert len(_fills(m)) == 1
    assert "[fill-source=sse]" in " ".join(_logs(m._log.info))
    m._stream_client.request_reconnect.assert_not_called()
    m._check_order_statuses.assert_not_awaited()


async def test_probe_noop_when_nt_order_already_closed():
    m = _client(by_id=[row("FLL")])
    m._cache.order.return_value.is_closed = True
    await _run_probe(m)
    m._client.get_orders_by_id.assert_not_awaited()


async def test_probe_books_dropped_fill_and_reconnects():
    m = _client(by_id=[row("FLL")])
    await _run_probe(m)
    m._client.get_orders_by_id.assert_awaited_once_with("SIM1", [TS_ID])
    assert len(_fills(m)) == 1
    assert _fills(m)[0].kwargs["client_order_id"] == COID
    assert any("[fill-source=fast-probe]" in s for s in _logs(m._log.info))
    silent = [s for s in _logs(m._log.warning) if "[ORDER-STREAM-SILENT]" in s]
    assert len(silent) == 1
    assert silent[0].startswith(
        f"[ORDER-STREAM-SILENT] TS order {TS_ID} ({COID}): FLL seen by the fast probe ")
    assert silent[0].endswith(
        "s after accept, not by the order stream - catch-up poll + stream reconnect")
    m._check_order_statuses.assert_awaited_once()
    m._stream_client.request_reconnect.assert_called_once_with("orders")
    assert m._order_last_status[TS_ID] == "FLL"
    assert m._fast_fill_probes == {}                     # done callback cleaned up


@pytest.mark.parametrize("status", ["OPN", "ACK", "DON", "FPR", "UCN", "TSC", "ZZZ"])
async def test_probe_still_working_does_nothing(status):
    m = _client(by_id=[row(status)])
    await _run_probe(m)
    m._client.get_orders_by_id.assert_awaited_once()
    m.generate_order_filled.assert_not_called()
    m.generate_order_canceled.assert_not_called()
    m.generate_order_rejected.assert_not_called()
    assert TS_ID not in m._order_last_status            # not fed through the status path
    m._stream_client.request_reconnect.assert_not_called()
    m._check_order_statuses.assert_not_awaited()
    assert not any("[ORDER-STREAM-SILENT]" in s for s in _logs(m._log.warning))


@pytest.mark.parametrize("exc", [BrokerReadError("orders/1001", ["ServiceUnavailable"]),
                                 asyncio.TimeoutError(), RuntimeError("HTTP 503")])
async def test_probe_read_failure_is_unknown(exc):
    m = _client()
    m._client.get_orders_by_id = AsyncMock(side_effect=exc)
    await _run_probe(m)
    m.generate_order_filled.assert_not_called()
    m._stream_client.request_reconnect.assert_not_called()
    m._check_order_statuses.assert_not_awaited()
    assert m._log.warning.call_count == 1
    assert "state unknown" in _logs(m._log.warning)[0]
    assert TS_ID not in m._order_last_status


@pytest.mark.parametrize("rows", [[], None, [{"OrderID": "999", "Status": "FLL"}],
                                  [{"OrderID": TS_ID}]])
async def test_probe_not_found_is_unknown(rows):
    m = _client(by_id=rows)
    m._client.get_orders_by_id = AsyncMock(return_value=rows)
    await _run_probe(m)
    m.generate_order_filled.assert_not_called()
    m._stream_client.request_reconnect.assert_not_called()
    m._check_order_statuses.assert_not_awaited()
    assert m._log.warning.call_count == 1
    assert "not found at the broker - state unknown" in _logs(m._log.warning)[0]


async def test_probe_and_stream_same_fill_once():
    """Review Focus 1: probe, stream and safety poll all see the same FLL -> one fill."""
    m = _client(by_id=[row("FLL")], poll_rows=[row("FLL")])
    m._check_order_statuses = lambda: C._check_order_statuses(m)  # the real catch-up poll
    await _run_probe(m)
    await C._process_order_event(m, row("FLL"))          # the late stream copy
    await C._check_order_statuses(m)                     # the 30 s safety poll
    assert len(_fills(m)) == 1


async def test_stream_fill_during_probe_read_is_not_silent():
    """The stream delivers while the by-id read is in flight: one fill, no false alarm."""
    m = _client()

    async def read(*_a, **_k):
        await C._process_order_event(m, row("FLL"))      # stream lands mid-read
        return [row("FLL")]

    m._client.get_orders_by_id = AsyncMock(side_effect=read)
    await _run_probe(m)
    assert len(_fills(m)) == 1
    assert not any("[ORDER-STREAM-SILENT]" in s for s in _logs(m._log.warning))
    m._stream_client.request_reconnect.assert_not_called()


async def test_probe_finds_rejected_market_books_it_and_flags_stream():
    m = _client(by_id=[{"OrderID": TS_ID, "Status": "REJ", "RejectReason": "margin"}])
    await _run_probe(m)
    m.generate_order_rejected.assert_called_once()
    m._stream_client.request_reconnect.assert_called_once_with("orders")


async def test_reconnect_rate_limited_60s(monkeypatch):
    t = [1000.0]
    monkeypatch.setattr(ex, "_mono", lambda: t[0])
    m = _client(by_id=[row("FLL")])
    await _run_probe(m)
    assert m._stream_client.request_reconnect.call_count == 1
    # a second silent fill on another order 10 s later
    t[0] = 1010.0
    m._ts_order_id_to_client_order_id["1002"] = ClientOrderId("O-MKT2")
    r2 = dict(row("FLL"), OrderID="1002")
    m._client.get_orders_by_id = AsyncMock(return_value=[r2])
    C._schedule_fast_fill_probe(m, NS(order_type=OrderType.MARKET,
                                      client_order_id=ClientOrderId("O-MKT2")), "1002")
    await asyncio.gather(*m._fast_fill_probes.values())
    assert m._stream_client.request_reconnect.call_count == 1   # rate limited
    assert m._check_order_statuses.await_count == 2             # catch-up poll still runs
    # after 60 s another forced reconnect is allowed
    t[0] = 1061.0
    C._force_order_stream_reconnect(m)
    assert m._stream_client.request_reconnect.call_count == 2


@pytest.mark.parametrize("otype", [OrderType.LIMIT, OrderType.STOP_MARKET,
                                   OrderType.STOP_LIMIT, OrderType.MARKET_IF_TOUCHED])
async def test_only_market_orders_probed(otype):
    m = _client()
    C._schedule_fast_fill_probe(m, NS(order_type=otype, client_order_id=COID), TS_ID)
    assert m._fast_fill_probes == {}


async def test_submit_order_schedules_probe_for_market():
    order = make_market_order(tags=None)
    stub = _make_submit_order_stub()
    stub._cancel_if_closed_before_accept = AsyncMock(return_value=False)
    stub._loop = asyncio.get_running_loop()
    stub._fast_fill_probe_secs = 3600.0     # never fires during the test
    stub._fast_fill_probes = {}
    stub._stream_client = MagicMock()
    stub._schedule_fast_fill_probe = MagicMock(side_effect=AssertionError("must be class call"))
    await C._submit_order(stub, _build_submit_command(order))
    stub.generate_order_accepted.assert_called_once()
    stub.generate_order_rejected.assert_not_called()
    assert list(stub._fast_fill_probes) == [order.client_order_id]
    await C._cancel_fast_fill_probes(stub)
    assert stub._fast_fill_probes == {}


async def test_submit_order_never_rejects_when_scheduling_fails():
    """A probe scheduling error must never turn an accepted order into a reject."""
    order = make_market_order(tags=None)
    stub = _make_submit_order_stub()
    stub._cancel_if_closed_before_accept = AsyncMock(return_value=False)
    stub._loop = MagicMock()
    stub._loop.create_task.side_effect = RuntimeError("loop closed")
    stub._fast_fill_probe_secs = 5.0
    stub._fast_fill_probes = {}
    stub._stream_client = MagicMock()
    await C._submit_order(stub, _build_submit_command(order))
    stub.generate_order_accepted.assert_called_once()
    stub.generate_order_rejected.assert_not_called()


async def test_existing_mock_stubs_schedule_nothing():
    """Stubs without the probe attributes (MagicMock secs) schedule no probe."""
    order = make_market_order(tags=None)
    stub = _make_submit_order_stub()
    stub._cancel_if_closed_before_accept = AsyncMock(return_value=False)
    await C._submit_order(stub, _build_submit_command(order))
    stub._loop.create_task.assert_not_called()
    stub.generate_order_rejected.assert_not_called()


async def test_group_orders_never_probed():
    stub, cmd = _group_stub([False, False])
    for o in cmd.order_list.orders:
        o.order_type = OrderType.MARKET
    stub._loop = asyncio.get_running_loop()
    stub._fast_fill_probe_secs = 5.0
    stub._fast_fill_probes = {}
    stub._stream_client = MagicMock()
    await C._submit_order_group(stub, cmd, "OCO", [{}, {}])
    assert stub.generate_order_accepted.call_count == 2
    assert stub._fast_fill_probes == {}


async def test_polling_mode_schedules_nothing():
    m = _client()
    m._stream_client = None
    C._schedule_fast_fill_probe(m, _market(), TS_ID)
    assert m._fast_fill_probes == {}


async def test_probe_cancelled_on_disconnect():
    """Review Focus 2: a pending probe at disconnect -> cancelled, no read, no exception."""
    m = _client(by_id=[row("FLL")])
    m._fast_fill_probe_secs = 3600.0
    C._schedule_fast_fill_probe(m, _market(), TS_ID)
    task = next(iter(m._fast_fill_probes.values()))
    await asyncio.sleep(0)                              # probe is now sleeping
    m._fill_poll_task = None
    m._token_keepalive_task = None
    m._status_safety_poll_task = None
    m._client.close = AsyncMock()
    await C._disconnect(m)
    assert task.cancelled()
    assert m._fast_fill_probes == {}
    m._client.get_orders_by_id.assert_not_awaited()
    m.generate_order_filled.assert_not_called()
    m._log.error.assert_not_called()
    m._client.close.assert_awaited_once()


async def test_probe_cancelled_mid_read_does_nothing():
    m = _client()
    started = asyncio.Event()

    async def hang(*_a, **_k):
        started.set()
        await asyncio.sleep(3600)

    m._client.get_orders_by_id = AsyncMock(side_effect=hang)
    C._schedule_fast_fill_probe(m, _market(), TS_ID)
    await started.wait()
    await C._cancel_fast_fill_probes(m)
    m.generate_order_filled.assert_not_called()
    m._log.warning.assert_not_called()
    m._log.error.assert_not_called()


async def test_probe_disabled_with_zero():
    m = _client(by_id=[row("FLL")])
    m._fast_fill_probe_secs = 0.0
    C._schedule_fast_fill_probe(m, _market(), TS_ID)
    assert m._fast_fill_probes == {}
    await asyncio.sleep(DELAY * 3)
    m._client.get_orders_by_id.assert_not_awaited()


async def test_process_order_event_default_label_is_sse():
    m = _client()
    await C._process_order_event(m, row("FLL"))
    assert any("[fill-source=sse]" in s for s in _logs(m._log.info))


async def test_stream_delivered_but_unprocessed_is_not_silent(monkeypatch):
    """Review minor: the stream DID deliver FLL but processing failed -> the probe retries,
    logs [FAST-PROBE], and neither logs SILENT nor forces a reconnect."""
    m = _client(by_id=[row("FLL")])
    orig = ex._emit_fill_delta
    calls = []

    def flaky(*a, **k):
        calls.append(1)
        if len(calls) == 1:
            raise RuntimeError("boom")
        return orig(*a, **k)

    monkeypatch.setattr(ex, "_emit_fill_delta", flaky)
    C._schedule_fast_fill_probe(m, _market(), TS_ID)
    with pytest.raises(RuntimeError):
        await C._process_order_event(m, row("FLL"))      # stream path: fails, un-advances
    assert TS_ID not in m._order_last_status
    await asyncio.gather(*m._fast_fill_probes.values())
    assert len(_fills(m)) == 1                           # the probe booked it
    warns = _logs(m._log.warning)
    assert not any("[ORDER-STREAM-SILENT]" in w for w in warns)
    assert (f"[FAST-PROBE] TS order {TS_ID} ({COID}): stream delivered FLL but it was not "
            f"processed - retried via the probe, no stream reconnect") in warns
    m._stream_client.request_reconnect.assert_not_called()
    assert TS_ID not in m._stream_terminal_seen          # cleaned with the probe


async def test_probe_own_processing_failure_is_not_silent(monkeypatch):
    m = _client(by_id=[row("FLL")])

    def boom(*a, **k):
        raise RuntimeError("kaput")

    monkeypatch.setattr(ex, "_emit_fill_delta", boom)
    await _run_probe(m)
    assert not any("[ORDER-STREAM-SILENT]" in w for w in _logs(m._log.warning))
    errs = _logs(m._log.error)
    assert any(f"[FAST-PROBE] TS order {TS_ID} ({COID}): processing failed: kaput" in e
               for e in errs)
    m._stream_client.request_reconnect.assert_not_called()
    m._check_order_statuses.assert_not_awaited()


async def test_probe_not_scheduled_while_disconnecting():
    m = _client(by_id=[row("FLL")])
    m._disconnecting = True
    C._schedule_fast_fill_probe(m, _market(), TS_ID)
    assert m._fast_fill_probes == {}
    m._disconnecting = False
    C._schedule_fast_fill_probe(m, _market(), TS_ID)
    assert len(m._fast_fill_probes) == 1
    await C._cancel_fast_fill_probes(m)


async def test_disconnect_sets_flag_and_connect_clears_it():
    import inspect
    src_d = inspect.getsource(C._disconnect)
    assert src_d.index("self._disconnecting = True") < src_d.index("_cancel_fast_fill_probes")
    assert "self._disconnecting = False" in inspect.getsource(C._connect)
