"""[BAR-LATE] logging and the reconnect-batch tag (strategies §260 Task 7).

A late stream bar (Closed with IsRealtime False, or closed more than step + 10 s after its end)
is held at most 0.5 s so the bars of one reconnect batch are emitted together, oldest first,
each tagged in ``bar_tags`` before NT gets it: every bar of the batch except the newest is
``superseded``. What is emitted, and in which order, never changes.

The case reproduced is FESX 15-MINUTE 2026-11-02 10:06:11.6Z (s18-and-upstreamable.md,
Question A): a reconnect delivered a silent Closed 09:40 (IsRealtime absent) and a Closed 09:55
(IsRealtime False) in one batch, then the forming 10:10 bar.
"""

from __future__ import annotations

import asyncio
import types

import pytest
from nautilus_trader.model.data import BarType

from tests.test_bar_stream import (
    BT,
    BTD,
    DAY_STEPS,
    GOLDEN,
    REAL_SLEEP,
    S,
    SCENARIOS,
    C,
    Harness,
    _fake,
    _feed,
    _ns,
    ev,
    rest_hole,
)
from tradestation_nt_community import bar_tags
from tradestation_nt_community import data as data_mod
from tradestation_nt_community.bar_gate import BarCloseGate, bar_end_ns
from tradestation_nt_community.common.enums import TradeStationBarUnit
from tradestation_nt_community.parsing.data import parse_bars

FX = BarType.from_str("FESXZ26.TRADESTATION-15-MINUTE-LAST-EXTERNAL")
D = "2026-11-02T"
_ABSENT = object()


@pytest.fixture(autouse=True)
def _clean_tags():
    bar_tags.clear()
    yield
    bar_tags.clear()


def fx(hhmm, status="Open", realtime=_ABSENT, c=6000):
    e = {"TimeStamp": f"{D}{hhmm}:00Z", "BarStatus": status,
         "Open": c, "High": c, "Low": c, "Close": c, "TotalVolume": 10}
    if realtime is not _ABSENT:
        e["IsRealtime"] = realtime
    return e


def _enable(fake):
    fake._late_batches = {}
    fake._late_timers = {}
    return fake


def _late_timers(timers):
    return [h for h in timers if getattr(h, "cb", None) is data_mod._late_batch_quiet]


def _info(fake, tag):
    return [str(c.args[0]) for c in fake._log.info.call_args_list if tag in str(c.args[0])]


def _seeded(fake, bt, ts):
    g = BarCloseGate()
    g.seed_from_history(_ns(ts))
    g.reset_connection()  # a reconnect: seed bars newer than the watermark are replayed
    fake._bar_gates[bt] = g
    return g


def _feed_bt(fake, bt, *events, live_stream=True):
    gate = fake._bar_gates.setdefault(bt, BarCloseGate())
    for e in events:
        C._emit_gated(fake, gate, e, bt, None, live_stream=live_stream)
    return gate


# -- the real client has the state (a fake-only feature would be off in production) ----------


def test_real_client_has_the_late_batch_state():
    from unittest.mock import MagicMock

    from nautilus_trader.common.providers import InstrumentProvider
    from nautilus_trader.test_kit.stubs.component import TestComponentStubs

    loop = asyncio.new_event_loop()
    try:
        client = data_mod.TradeStationDataClient(
            loop=loop,
            client=MagicMock(),
            msgbus=TestComponentStubs.msgbus(),
            cache=TestComponentStubs.cache(),
            clock=TestComponentStubs.clock(),
            instrument_provider=InstrumentProvider(),
        )
        assert client._late_batches == {} and client._late_timers == {}
    finally:
        loop.close()


# -- on time: never held, never tagged ------------------------------------------------------


@pytest.mark.parametrize("mode", ["off", "shadow", "on"])
async def test_on_time_closed_bar_emits_at_once_untagged_or_not_late(mode):
    fake = _enable(_fake([], mode=mode))
    fake.now[0] = _ns("2026-10-16T16:05:00.1Z")
    _feed(fake, ev("16:05"), ev("16:05", "Closed"))
    assert fake.emitted == ["16:05"]  # synchronously, no hold
    assert _late_timers(fake.timers) == [] and fake._late_batches == {}
    assert bar_tags.lookup(BT, _ns("2026-10-16T16:05:00Z")) is None
    assert _info(fake, "[BAR-LATE]") == []
    # end + step + grace exactly is still on time
    fake.now[0] = _ns("2026-10-16T16:15:10Z")
    _feed(fake, ev("16:10", "Closed"))
    assert fake.emitted == ["16:05", "16:10"] and _late_timers(fake.timers) == []


# -- a lone late bar ------------------------------------------------------------------------


async def test_lone_late_bar_is_batch_of_one_newest():
    fake = _enable(_fake([], mode="shadow"))
    fake.now[0] = _ns(D + "10:00:00Z")
    _feed_bt(fake, FX, fx("09:40", "Closed", True))  # 20 min late, IsRealtime True
    assert fake.emitted == []  # held
    timers = _late_timers(fake.timers)
    assert len(timers) == 1 and timers[0].delay == 0.5
    timers[0].cb(*timers[0].args)
    assert fake.emitted == ["09:40"]
    t = bar_tags.lookup(FX, _ns(D + "09:40:00Z"))
    assert t == bar_tags.BarTag(late=True, superseded=False, is_realtime=True, lag_s=1200.0,
                                batch=(1, 1))
    assert _info(fake, "[BAR-LATE]") == [
        f"[BAR-LATE] {FX} ts={D}09:40:00Z IsRealtime=True lag_s=1200.000 batch=1/1"
    ]
    timers[0].cb(*timers[0].args)  # a stale repeat: nothing more
    assert fake.emitted == ["09:40"]


async def test_hold_deadline_is_not_extended_by_a_later_late_bar():
    fake = _enable(_fake([], mode="off"))
    _seeded(fake, FX, D + "09:25:00Z")
    fake.now[0] = _ns(D + "10:06:11Z")
    _feed_bt(fake, FX, fx("09:40", "Closed"))
    fake.now[0] += int(0.4 * S)
    _feed_bt(fake, FX, fx("09:55", "Closed", False))
    timers = _late_timers(fake.timers)
    assert len(timers) == 1 and timers[0].delay == 0.5 and not timers[0].cancelled
    timers[0].cb(*timers[0].args)
    assert fake.emitted == ["09:40", "09:55"]
    assert bar_tags.lookup(FX, _ns(D + "09:40:00Z")).superseded is True
    assert bar_tags.lookup(FX, _ns(D + "09:55:00Z")).superseded is False


# -- the FESX 10:06 reconnect batch ---------------------------------------------------------


def _fesx_harness(mode, rest=None):
    h = Harness(mode, rest=rest)
    f = _enable(h.fake)
    g = BarCloseGate()
    g.seed_from_history(_ns(D + "09:10:00Z"))  # watermark before the event
    f._bar_gates[FX] = g
    f._parse_bars = lambda raws, bt, inst: [
        types.SimpleNamespace(label=r["TimeStamp"][11:16], bar_type=bt,
                              ts_event=bar_end_ns(r["TimeStamp"])) for r in raws
    ]
    calls = []  # (label, wall ns, tag at the moment NT gets the bar)
    f._handle_data = lambda b: (
        calls.append((b.label, h.now, bar_tags.lookup(b.bar_type, b.ts_event))),
        f.emitted.append(b.label),
    )
    return h, calls


FESX_STEPS = [
    (D + "10:06:11.6", {"_reconnected": True}),
    (D + "10:06:11.6", fx("09:40", "Closed")),  # silent: IsRealtime absent
    (D + "10:06:11.6", fx("09:55", "Closed", False)),  # [BAR-GAP-REPLAY]
    (D + "10:06:11.7", fx("10:10", "Open", False)),  # the forming bar flushes the batch
    (D + "10:06:30", fx("10:10", "Open", True)),
]


async def test_two_bar_reconnect_batch_tags_older_superseded(monkeypatch):
    h, calls = _fesx_harness("shadow", rest=lambda now: [])
    await h.run(FESX_STEPS, bar_type=FX, monkeypatch=monkeypatch)
    flushed_at = _ns(D + "10:06:11.7")  # the Open, not the 0.5 s timer, not the stream end
    assert [(c[0], c[1]) for c in calls] == [("09:40", flushed_at), ("09:55", flushed_at)]
    t40, t55 = calls[0][2], calls[1][2]  # written before _handle_data
    assert t40 == bar_tags.BarTag(late=True, superseded=True, is_realtime=None,
                                  lag_s=t40.lag_s, batch=(1, 2))
    assert t55 == bar_tags.BarTag(late=True, superseded=False, is_realtime=False,
                                  lag_s=t55.lag_s, batch=(2, 2))
    assert round(t40.lag_s, 1) == 1571.6 and round(t55.lag_s, 1) == 671.6
    late = [x for x in h.lines(("info",)) if "[BAR-LATE]" in x]
    assert late == [
        f"info:[BAR-LATE] {FX} ts={D}09:40:00Z IsRealtime=None lag_s=1571.600 batch=1/2",
        f"info:[BAR-LATE] {FX} ts={D}09:55:00Z IsRealtime=False lag_s=671.600 batch=2/2",
    ]
    replay = [x for x in h.lines(("info",)) if "[BAR-GAP-REPLAY]" in x]
    assert len(replay) == 1 and f"ts={D}09:55:00Z" in replay[0]
    assert replay[0].endswith(" IsRealtime=False")
    bf = [x for x in h.lines(("info",)) if "[BAR-BACKFILL-SHADOW]" in x]
    assert bf and f"missing=[{D}09:25:00Z]" in bf[0]
    timers = [x[1] for x in h.timers if x[1].cb is data_mod._late_batch_quiet]
    assert len(timers) == 1 and timers[0].cancelled  # flushed by the Open, never by the timer


async def test_two_bar_reconnect_batch_in_mode_on_loses_and_duplicates_nothing(monkeypatch):
    rest = [fx("09:25", "Closed", False, c=6001), fx("09:40", "Closed", False),
            fx("09:55", "Closed", False)]
    h, calls = _fesx_harness("on", rest=lambda now: rest)
    await h.run(FESX_STEPS, bar_type=FX, monkeypatch=monkeypatch)
    # backfill first, in order, each once; 10:10 is the on-time [BAR-RELEASE] at end + 10 s
    assert [c[0] for c in calls] == ["09:25", "09:40", "09:55", "10:10"]
    assert all(c[2] is not None and c[2].late for c in calls[:3])
    assert calls[2][2].superseded is False and calls[3][2] is None
    assert calls[3][1] == _ns(D + "10:10:10")
    assert h.fake._late_batches == {}
    assert h.fake._bar_gates[FX].last_emitted_ns == _ns(D + "10:10:00Z")


# -- flush triggers -------------------------------------------------------------------------


async def test_batch_flushes_before_an_on_time_bar():
    fake = _enable(_fake([], mode="shadow"))
    _seeded(fake, BT, "2026-10-16T16:00:00Z")
    fake.now[0] = _ns("2026-10-16T16:10:00.1Z")
    _feed(fake, ev("16:05", "Closed", False))  # gap replay: late, held
    assert fake.emitted == []
    _feed(fake, ev("16:10", "Closed"))  # on time: the batch goes first, then it, at once
    assert fake.emitted == ["16:05", "16:10"]
    assert _late_timers(fake.timers)[0].cancelled
    t = bar_tags.lookup(BT, _ns("2026-10-16T16:05:00Z"))
    assert t.batch == (1, 1) and t.superseded is True  # the on-time 16:10 is the newest
    assert bar_tags.lookup(BT, _ns("2026-10-16T16:10:00Z")) is None


@pytest.mark.parametrize("trigger", ["on_time_closed", "open", "deadline", "reset"])
async def test_superseded_depends_on_the_flush_trigger(trigger):
    """Controller ruling 2026-11-04: only the newest bar of a burst may place entries."""
    fake = _enable(_fake([], mode="shadow"))
    _seeded(fake, BT, "2026-10-16T16:00:00Z")
    fake.now[0] = _ns("2026-10-16T16:15:00.1Z")
    _feed(fake, ev("16:05", "Closed", False), ev("16:10", "Closed", False))  # one late batch
    assert fake.emitted == []
    if trigger == "on_time_closed":
        _feed(fake, ev("16:15", "Closed"))
    elif trigger == "open":
        _feed(fake, ev("16:15", "Open", False))
    elif trigger == "deadline":
        (h,) = _late_timers(fake.timers)
        h.cb(*h.args)
    else:
        data_mod._cancel_bar_jobs(fake, BT)
    want = ["16:05", "16:10", "16:15"] if trigger == "on_time_closed" else ["16:05", "16:10"]
    assert fake.emitted == want
    old = bar_tags.lookup(BT, _ns("2026-10-16T16:05:00Z"))
    newest = bar_tags.lookup(BT, _ns("2026-10-16T16:10:00Z"))
    assert old.superseded is True and old.batch == (1, 2) and newest.batch == (2, 2)
    assert newest.superseded is (trigger == "on_time_closed")
    assert bar_tags.lookup(BT, _ns("2026-10-16T16:15:00Z")) is None  # on time: untagged


async def test_an_on_time_bar_without_a_held_batch_tags_nothing():
    fake = _enable(_fake([], mode="off"))
    fake.now[0] = _ns("2026-10-16T16:05:00.1Z")
    _feed(fake, ev("16:05", "Closed"))
    assert fake.emitted == ["16:05"] and bar_tags._store == {}


async def test_a_release_flushes_the_batch_first_and_is_never_held():
    fake = _enable(_fake([], mode="on"))
    _seeded(fake, BT, "2026-10-16T16:00:00Z")
    fake.now[0] = _ns("2026-10-16T16:10:10Z")
    _feed(fake, ev("16:05", "Closed", False))  # late, held
    released = ev("16:10")  # a [BAR-RELEASE] raw: the forming bar as it stood
    step = data_mod._intraday_step_ns(BT)
    assert data_mod._late_batch_route(fake, released, BT, None, step, "[BAR-RELEASE]") is True
    assert fake.emitted == ["16:05", "16:10"]
    assert bar_tags.lookup(BT, _ns("2026-10-16T16:10:00Z")) is None


async def test_forming_event_flushes_and_fallback_keeps_order():
    fake = _enable(_fake([], mode="off"))
    _seeded(fake, BT, "2026-10-16T16:00:00Z")
    fake.now[0] = _ns("2026-10-16T16:12:00Z")
    _feed(fake, ev("16:05", "Closed", False))
    _feed(fake, ev("16:10", "Open", False))  # the seed's forming bar
    assert fake.emitted == ["16:05"]
    _feed(fake, ev("16:15"))  # 16:10 never closed: fallback, on time
    assert fake.emitted == ["16:05", "16:10"]


async def test_batch_flushes_on_stream_stop_and_reset():
    end_wm = _ns("2026-10-16T16:05:00Z")
    # reset (_cancel_bar_jobs: every reset path, incl. _reconnected and reconnect_bar_streams)
    fake = _enable(_fake([], mode="shadow"))
    fake.now[0] = _ns("2026-10-16T16:30:00Z")
    gate = _feed(fake, ev("16:05", "Closed"))
    assert fake.emitted == [] and gate.last_emitted_ns == end_wm
    data_mod._cancel_bar_jobs(fake, BT)
    assert fake.emitted == ["16:05"] and gate.last_emitted_ns == end_wm
    assert fake._late_batches == {} and _late_timers(fake.timers)[0].cancelled

    # _reconnected inside the stream
    fake = _enable(_fake([ev("16:05", "Closed"), {"_reconnected": True}], mode="shadow"))
    fake.now[0] = _ns("2026-10-16T16:30:00Z")
    seen = []
    orig = fake._bar_gates.setdefault(BT, BarCloseGate()).reset_connection
    fake._bar_gates[BT].reset_connection = lambda: (seen.append(list(fake.emitted)), orig())
    await C._stream_bars(fake, BT, "FCX26", "5", TradeStationBarUnit.MINUTE, None)
    assert seen == [[], ["16:05"]]  # start, then the reconnect: flushed before the reset

    # stream task cancelled (stop)
    async def stream_bars(**kw):
        yield ev("16:05", "Closed")
        await REAL_SLEEP(3600)

    fake = _enable(_fake([], mode="shadow"))
    fake._stream_client = types.SimpleNamespace(stream_bars=stream_bars)
    fake.now[0] = _ns("2026-10-16T16:30:00Z")
    task = asyncio.get_running_loop().create_task(
        C._stream_bars(fake, BT, "FCX26", "5", TradeStationBarUnit.MINUTE, None)
    )
    for _ in range(5):
        await REAL_SLEEP(0)
    assert fake.emitted == []
    task.cancel()
    await task  # _stream_bars swallows its own CancelledError
    assert fake.emitted == ["16:05"]
    assert fake._bar_gates[BT].last_emitted_ns == end_wm

    # unsubscribe and disconnect
    for path in ("unsubscribe", "disconnect"):
        fake = _enable(_fake([], mode="off"))
        fake.now[0] = _ns("2026-10-16T16:30:00Z")
        _feed(fake, ev("16:05", "Closed"))
        fake._bar_subscriptions = {}
        if path == "unsubscribe":
            await C._unsubscribe_bars(fake, types.SimpleNamespace(bar_type=BT))
        else:
            from unittest.mock import AsyncMock

            fake._quote_subscriptions = {}
            fake._trade_subscriptions = {}
            fake._quote_stream_tasks = {}
            fake._quote_stream_wants_quotes = set()
            fake._quote_stream_wants_trades = set()
            fake._last_quote = {}
            fake._last_trade = {}
            fake._client.close = AsyncMock()
            await C._disconnect(fake)
        assert fake.emitted == ["16:05"], path
        assert fake._late_batches == {} and fake._late_timers == {}


# -- excluded paths -------------------------------------------------------------------------


async def test_polling_and_day_bars_never_batched():
    fake = _enable(_fake([], mode="on"))
    fake.now[0] = _ns("2026-10-16T17:00:00Z")
    _seeded(fake, BT, "2026-10-16T16:00:00Z")
    _feed_bt(fake, BT, ev("16:05", "Closed"), live_stream=False)  # a poll, 55 min late
    assert fake.emitted == ["16:05"]
    fake.emitted.clear()
    fake.now[0] = _ns("2026-10-20T12:00:00Z")
    _feed_bt(fake, BTD, {**ev("16:05", "Closed"), "TimeStamp": "2026-10-19T00:00:00Z"})
    assert fake.emitted == ["00:00"]  # a DAY bar, emitted at once
    assert _late_timers(fake.timers) == [] and fake._late_batches == {}
    assert bar_tags.lookup(BT, _ns("2026-10-16T16:05:00Z")) is None
    assert _info(fake, "[BAR-LATE]") == []


# -- the tag store --------------------------------------------------------------------------


def test_tag_store_bounded_per_bar_type():
    t = bar_tags.BarTag(late=True, superseded=False, is_realtime=None, lag_s=1.0, batch=(1, 1))
    for i in range(100):
        bar_tags.tag(FX, i, t)
    bar_tags.tag(BT, 5, t)
    assert bar_tags.lookup(FX, 35) is None and bar_tags.lookup(FX, 36) == t
    assert bar_tags.lookup(FX, 99) == t and bar_tags.lookup(BT, 5) == t
    assert len(bar_tags._store[str(FX)]) == bar_tags._KEEP == 64


def test_lookup_miss_returns_none():
    assert bar_tags.API == 1
    assert bar_tags.lookup(FX, 1) is None
    bar_tags.tag(FX, 1, bar_tags.BarTag(True, False, None, 0.0, (1, 1)))
    assert bar_tags.lookup(FX, 2) is None and bar_tags.lookup(BT, 1) is None
    assert bar_tags.lookup(None, None) is None and bar_tags.lookup(FX, "x") is None


async def test_tag_key_is_the_real_bars_bar_type_and_ts_event():
    fake = _enable(_fake([], mode="shadow"))
    fake._parse_bars = lambda raws, bt, inst: parse_bars(raws, bt)
    got = []
    fake._handle_data = got.append
    _seeded(fake, FX, D + "09:40:00Z")
    fake.now[0] = _ns(D + "10:06:11.6Z")
    _feed_bt(fake, FX, fx("09:55", "Closed", False))
    _late_timers(fake.timers)[0].cb(*_late_timers(fake.timers)[0].args)
    (b,) = got
    tag = bar_tags.lookup(b.bar_type, b.ts_event)
    assert tag is not None and tag.batch == (1, 1) and tag.is_realtime is False


async def test_a_failing_tag_never_stops_the_bar(monkeypatch):
    fake = _enable(_fake([], mode="shadow"))
    fake.now[0] = _ns("2026-10-16T16:30:00Z")

    def boom(*a, **k):
        raise RuntimeError("boom")

    monkeypatch.setattr(bar_tags, "tag", boom)
    _feed(fake, ev("16:05", "Closed"), ev("16:10", "Closed"))
    data_mod._late_batch_flush(fake, BT)
    assert fake.emitted == ["16:05", "16:10"]
    assert any("[BAR-LATE-ERROR]" in str(c.args[0]) for c in fake._log.error.call_args_list)


async def test_batch_is_capped():
    fake = _enable(_fake([], mode="off"))
    fake.now[0] = _ns("2026-10-20T00:00:00Z")
    for i in range(70):
        e = ev("16:05", "Closed")
        e["TimeStamp"] = pd_ts(i)
        _feed(fake, e)
    assert len(fake.emitted) == data_mod._LATE_BATCH_MAX
    assert len(fake._late_batches[BT]) == 70 - data_mod._LATE_BATCH_MAX


def pd_ts(i):
    import pandas as pd

    t = pd.Timestamp("2026-10-16T16:05:00Z") + pd.Timedelta(minutes=5 * i)
    return t.strftime("%Y-%m-%dT%H:%M:%SZ")


# -- emissions byte-identical to batching off -----------------------------------------------


def _strip(lines):
    out = []
    for x in lines:
        if "[BAR-LATE]" in x:
            continue
        out.append(x.removesuffix(" IsRealtime=False"))
    return out


@pytest.mark.parametrize("mode", ["off", "shadow", "on"])
@pytest.mark.parametrize("name", sorted(GOLDEN))
async def test_scenarios_emit_the_same_with_batching(name, mode, monkeypatch):
    async def go(batching):
        h = Harness(mode, rest=rest_hole)
        if batching:
            _enable(h.fake)
        if name == "day":
            return await h.run(DAY_STEPS, bar_type=BTD, monkeypatch=monkeypatch)
        return await h.run(SCENARIOS[name], monkeypatch=monkeypatch)

    plain, batched = await go(False), await go(True)
    assert batched.fake.emitted == plain.fake.emitted
    assert set(batched.fake._last_bar_ts) == set(plain.fake._last_bar_ts)
    assert sorted(_strip(batched.lines())) == sorted(_strip(plain.lines()))
    assert batched.fake._late_batches == {}
