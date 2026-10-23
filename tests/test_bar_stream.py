"""_stream_bars / _request_bars / _poll_bars, calling the REAL methods (strategies LESSONS §207).

Events are TradeStation v3 shaped (BarStatus, IsRealtime, TimeStamp = bar END) as captured
live on 2026-10-16.
"""

from __future__ import annotations

import asyncio
import types
from unittest.mock import AsyncMock, MagicMock

import pandas as pd
import pytest
from nautilus_trader.model.data import BarType

from tradestation_nt_community import data as data_mod
from tradestation_nt_community.bar_gate import BarCloseGate, bar_end_ns
from tradestation_nt_community.common.enums import TradeStationBarUnit
from tradestation_nt_community.data import _STREAM_BARSBACK, TradeStationDataClient

C = TradeStationDataClient
BT = BarType.from_str("FCX26.TRADESTATION-5-MINUTE-LAST-EXTERNAL")


def ev(hhmm, status="Open", realtime=True):
    return {
        "TimeStamp": f"2026-10-16T{hhmm}:00Z",
        "BarStatus": status,
        "IsRealtime": realtime,
        "Open": 1,
        "High": 1,
        "Low": 1,
        "Close": 1,
        "TotalVolume": 1,
    }


def _create_task(coro):
    return asyncio.get_running_loop().create_task(coro)


def _fake(events, gates=None, mode="off"):
    async def stream_bars(**kw):
        fake.stream_kwargs = kw
        for e in events:
            yield e

    fake = types.SimpleNamespace(
        _log=MagicMock(),
        _extended_hours=False,
        _stream_client=types.SimpleNamespace(stream_bars=stream_bars),
        _bar_gates={} if gates is None else gates,
        _last_bar_ts={},
        emitted=[],
        _bar_release_mode=mode,
        _release_timers={},
        _release_targets={},
        _release_fired={},
        _backfill_tasks={},
        _verify_tasks={},
        timers=[],
        now=[0],
    )

    def call_later(delay, cb, *args):
        h = types.SimpleNamespace(cancelled=False, delay=delay, cb=cb, args=args)
        h.cancel = lambda: setattr(h, "cancelled", True)
        fake.timers.append(h)
        return h

    fake._loop = types.SimpleNamespace(call_later=call_later, create_task=_create_task)
    fake._clock = types.SimpleNamespace(timestamp_ns=lambda: fake.now[0])
    fake._client = types.SimpleNamespace(get_bars=AsyncMock(return_value=[]))
    fake._parse_bars = lambda raws, bt, inst: [r["TimeStamp"][11:16] for r in raws]
    fake._handle_data = fake.emitted.append
    for name in (
        "_emit_gated", "_emit_released", "_log_emit_kind", "_log_snapshot", "_arm_release",
        "_on_release_due", "_drain_diverged", "_verify_divergence", "_maybe_start_backfill",
        "_backfill", "_log_release_error",
    ):
        setattr(fake, name, types.MethodType(getattr(C, name), fake))
    return fake


async def _run(events, gates=None):
    fake = _fake(events, gates)
    await C._stream_bars(fake, BT, "FCX26", "5", TradeStationBarUnit.MINUTE, None)
    return fake


async def test_closed_event_emits_at_the_boundary():
    f = await _run([ev("16:10", realtime=False), ev("16:10"), ev("16:10", "Closed")])
    assert f.emitted == ["16:10"]
    assert BT in f._last_bar_ts  # §70 watchdog freshness still recorded per emission


async def test_open_updates_never_emit():
    f = await _run([ev("16:10", realtime=False), ev("16:10"), ev("16:10")])
    assert f.emitted == []


async def test_missing_closed_falls_back_to_the_next_bar_and_logs():
    f = await _run([ev("16:10"), ev("16:15")])
    assert f.emitted == ["16:10"]
    assert any("[BAR-CLOSE-FALLBACK]" in str(c.args[0]) for c in f._log.info.call_args_list)


async def test_initial_history_seed_is_not_replayed():
    f = await _run(
        [
            ev("16:00", "Closed", False),
            ev("16:05", "Closed", False),
            ev("16:10", "Open", False),
            ev("16:10", "Closed"),
        ]
    )
    assert f.emitted == ["16:10"]


async def test_warmup_watermark_drops_the_overlap_and_fills_the_gap():
    g = BarCloseGate()
    g.seed_from_history(bar_end_ns("2026-10-16T16:00:00Z"))
    f = await _run(
        [
            ev("16:00", "Closed", False),
            ev("16:05", "Closed", False),
            ev("16:10", "Open", False),
            ev("16:10", "Closed"),
        ],
        {BT: g},
    )
    assert f.emitted == ["16:05", "16:10"]


async def test_reconnect_replays_only_bars_closed_during_the_gap():
    f = await _run(
        [
            ev("16:05"),
            ev("16:05", "Closed"),
            {"_reconnected": True},
            ev("16:00", "Closed", False),
            ev("16:05", "Closed", False),
            ev("16:10", "Closed", False),
            ev("16:15", "Open", False),
            ev("16:15", "Closed"),
        ]
    )
    assert f.emitted == ["16:05", "16:10", "16:15"]


async def test_watermark_survives_a_watchdog_task_restart():
    gates = {}
    await _run([ev("16:05"), ev("16:05", "Closed")], gates)
    f = await _run([ev("16:05", "Closed", False), ev("16:10", "Open", False)], gates)
    assert f.emitted == []


async def test_after_hours_seed_of_a_delivered_bar_is_dropped():
    gates = {}
    await _run([ev("18:05"), ev("18:05", "Closed")], gates)
    f = await _run(
        [ev("18:05", "Open", False), {"_reconnected": True}, ev("18:05", "Closed", False)], gates
    )
    assert f.emitted == []


async def test_emitted_debug_line_keeps_the_report_format():
    f = await _run([ev("16:10", "Closed")])
    assert any(
        str(c.args[0]) == f"Bar emitted for {BT}: ts=2026-10-16T16:10:00Z"
        for c in f._log.debug.call_args_list
    )


async def test_stream_requests_enough_history_to_fill_a_gap():
    f = await _run([])
    assert f.stream_kwargs["barsback"] == _STREAM_BARSBACK == 10


async def test_request_bars_seeds_the_gate_from_the_last_closed_bar():
    bars = [types.SimpleNamespace(ts_event=1), types.SimpleNamespace(ts_event=2)]
    fake = types.SimpleNamespace(
        _cache=types.SimpleNamespace(instrument=lambda iid: object()),
        _historical=types.SimpleNamespace(get_bars=AsyncMock(return_value=bars)),
        _handle_bars=MagicMock(),
        _bar_gates={},
        _log=MagicMock(),
    )
    req = types.SimpleNamespace(bar_type=BT, start=None, end=None, limit=2, id="r", params={})
    await C._request_bars(fake, req)
    assert fake._bar_gates[BT].last_emitted_ns == 2
    fake._handle_bars.assert_called_once()


async def test_poll_emits_only_closed_bars_after_the_seeding_poll(monkeypatch):
    polls = iter(
        [
            [ev("16:00", "Closed", False), ev("16:05", "Open", False)],  # seeds the watermark
            [ev("16:05", "Closed", False), ev("16:10", "Open", False)],
            [ev("16:05", "Closed", False), ev("16:10", "Open", False)],  # nothing new closed
            [ev("16:10", "Closed", False), ev("16:15", "Open", False)],
        ]
    )

    async def get_bars(**kw):
        try:
            return next(polls)
        except StopIteration:
            raise asyncio.CancelledError

    async def no_sleep(_s):
        return None

    monkeypatch.setattr(asyncio, "sleep", no_sleep)
    fake = _fake([])
    fake._client = types.SimpleNamespace(get_bars=get_bars)
    await C._poll_bars(fake, BT, "FCX26", "5", TradeStationBarUnit.MINUTE, None, 300.0)
    assert fake.emitted == ["16:05", "16:10"]


async def test_reconnect_replay_is_tagged_gap_replay():
    """A bar replayed from a reconnect seed logs [BAR-GAP-REPLAY]; live bars do not (§207)."""
    f = await _run(
        [
            ev("16:05"),
            ev("16:05", "Closed"),
            {"_reconnected": True},
            ev("16:05", "Closed", False),
            ev("16:10", "Closed", False),
            ev("16:15", "Open", False),
            ev("16:15", "Closed"),
        ]
    )
    tagged = [
        str(c.args[0]) for c in f._log.info.call_args_list if "[BAR-GAP-REPLAY]" in str(c.args[0])
    ]
    assert f.emitted == ["16:05", "16:10", "16:15"]
    assert len(tagged) == 1 and "ts=2026-10-16T16:10:00Z" in tagged[0]


async def test_polled_bars_are_not_tagged_gap_replay(monkeypatch):
    polls = iter([[ev("16:00", "Closed", False)], [ev("16:05", "Closed", False)]])

    async def get_bars(**kw):
        try:
            return next(polls)
        except StopIteration:
            raise asyncio.CancelledError

    async def no_sleep(_s):
        return None

    monkeypatch.setattr(asyncio, "sleep", no_sleep)
    fake = _fake([])
    fake._client = types.SimpleNamespace(get_bars=get_bars)
    await C._poll_bars(fake, BT, "FCX26", "5", TradeStationBarUnit.MINUTE, None, 300.0)
    assert fake.emitted == ["16:05"]
    assert not any("[BAR-GAP-REPLAY]" in str(c.args[0]) for c in fake._log.info.call_args_list)


async def test_unparseable_event_never_reaches_the_gate():
    """A malformed Closed event must not consume the watermark: the bar's last good update is
    still released (fallback) when the next bar starts."""
    bad = {**ev("16:10", "Closed"), "Close": "bad"}
    f = _fake([ev("16:10"), bad, ev("16:15")])
    f._parse_bars = lambda raws, bt, inst: (
        [] if raws[0].get("Close") == "bad" else [r["TimeStamp"][11:16] for r in raws]
    )
    await C._stream_bars(f, BT, "FCX26", "5", TradeStationBarUnit.MINUTE, None)
    assert f.emitted == ["16:10"]
    assert any("[BAR-PARSE-SKIP]" in str(c.args[0]) for c in f._log.warning.call_args_list)


async def test_request_bars_answers_even_without_bars():
    """No history (API error or empty): answer anyway, or the requester waits forever."""
    fake = types.SimpleNamespace(
        _cache=types.SimpleNamespace(instrument=lambda iid: object()),
        _historical=types.SimpleNamespace(get_bars=AsyncMock(return_value=[])),
        _handle_bars=MagicMock(),
        _bar_gates={},
        _log=MagicMock(),
    )
    req = types.SimpleNamespace(bar_type=BT, start=None, end=None, limit=2, id="r", params={})
    await C._request_bars(fake, req)
    fake._handle_bars.assert_called_once()
    assert fake._handle_bars.call_args.args[1] == []
    assert BT not in fake._bar_gates


async def test_request_bars_answers_when_the_instrument_is_missing():
    fake = types.SimpleNamespace(
        _cache=types.SimpleNamespace(instrument=lambda iid: None),
        _instrument_provider=types.SimpleNamespace(load_async=AsyncMock()),
        _historical=types.SimpleNamespace(get_bars=AsyncMock()),
        _handle_bars=MagicMock(),
        _bar_gates={},
        _log=MagicMock(),
    )
    req = types.SimpleNamespace(bar_type=BT, start=None, end=None, limit=2, id="r", params={})
    await C._request_bars(fake, req)
    fake._handle_bars.assert_called_once()
    assert fake._handle_bars.call_args.args[1] == []
    fake._historical.get_bars.assert_not_awaited()


def _v3_bars():
    import json
    from pathlib import Path
    p = Path(__file__).parent / "resources" / "bars_response_v3.json"
    return json.loads(p.read_text())["Bars"]


def test_v3_fixture_carries_the_fields_the_gate_reads():
    bars = _v3_bars()
    assert all("BarStatus" in b and "IsRealtime" in b for b in bars)
    assert bars[-1]["BarStatus"] == "Open" and all(b["IsRealtime"] is False for b in bars)


async def test_first_poll_of_real_bars_only_seeds(monkeypatch):
    polls = iter([_v3_bars()])

    async def get_bars(**kw):
        try:
            return next(polls)
        except StopIteration:
            raise asyncio.CancelledError

    async def no_sleep(_s):
        return None

    monkeypatch.setattr(asyncio, "sleep", no_sleep)
    fake = _fake([])
    fake._client = types.SimpleNamespace(get_bars=get_bars)
    await C._poll_bars(fake, BT, "ESZ26", "5", TradeStationBarUnit.MINUTE, None, 300.0)
    assert fake.emitted == []
    assert fake._bar_gates[BT].last_emitted_ns == bar_end_ns(_v3_bars()[-2]["TimeStamp"])


# -- late thin-hour bars: release + backfill wiring (plan 2026-10-23 Task 2) -----------------

REAL_SLEEP = asyncio.sleep
BTD = BarType.from_str("FCX26.TRADESTATION-1-DAY-LAST-EXTERNAL")
DAY = "2026-10-16T"
S = 10**9


def _ns(t):
    return int(pd.Timestamp(t).value)


def bar(ts, status="Open", realtime=True, c=1, v=1):
    return {"TimeStamp": ts, "BarStatus": status, "IsRealtime": realtime,
            "Open": 1, "High": max(1, c), "Low": 1, "Close": c, "TotalVolume": v}


class Harness:
    """Replays timed events through the REAL _stream_bars on a fake wall clock: release
    timers fire at their target time, backfill / verification tasks run to completion."""

    def __init__(self, mode, rest=None):
        self.now = 0
        self.timers = []  # [target_ns, handle]
        self.wakes = set()  # wake times of tasks in asyncio.sleep
        self.rest = rest or (lambda now: [])
        self.get_bars_calls = []
        fake = _fake([], mode=mode)
        fake._parse_bars = lambda raws, bt, inst: [r["TimeStamp"][5:16] for r in raws]

        def call_later(delay, cb, *args):
            h = types.SimpleNamespace(cancelled=False, cb=cb, args=args)
            h.cancel = lambda: setattr(h, "cancelled", True)
            self.timers.append([self.now + int(delay * S), h])
            return h

        fake._loop = types.SimpleNamespace(call_later=call_later, create_task=_create_task)
        fake._clock = types.SimpleNamespace(timestamp_ns=lambda: self.now)

        async def get_bars(**kw):
            self.get_bars_calls.append((self.now, kw))
            out = self.rest(self.now)
            if asyncio.iscoroutine(out):
                out = await out
            if isinstance(out, Exception):
                raise out
            return out

        fake._client = types.SimpleNamespace(get_bars=get_bars)
        self.fake = fake

    async def spin(self):
        for _ in range(10):
            await REAL_SLEEP(0)

    async def _fire_due(self, t_ns):
        while True:
            due = sorted((x for x in self.timers if x[0] <= t_ns and not x[1].cancelled),
                         key=lambda x: x[0])
            if not due:
                return
            target, h = due[0]
            self.timers.remove(due[0])
            self.now = max(self.now, target)
            h.cb(*h.args)
            await self.spin()

    async def advance(self, t_ns):
        """Move the wall clock to ``t_ns``, stopping at every timer target and every sleeping
        task's wake time on the way, so tasks run and timers fire on time."""
        await self.spin()
        while self.now < t_ns:
            stops = [x[0] for x in self.timers if not x[1].cancelled and x[0] > self.now]
            stops += [w for w in self.wakes if w > self.now]
            nxt = min([t_ns] + stops)
            await self._fire_due(nxt)
            self.now = max(self.now, nxt)
            await self.spin()
        await self._fire_due(self.now)

    async def sleep(self, s):
        target = self.now + int(s * S)
        self.wakes.add(target)
        try:
            while self.now < target:
                await REAL_SLEEP(0)
        finally:
            self.wakes.discard(target)

    async def run(self, steps, bar_type=BT, monkeypatch=None):
        monkeypatch.setattr(asyncio, "sleep", self.sleep)

        async def stream_bars(**kw):
            for wall, e in steps:
                await self.advance(_ns(wall))
                yield e

        self.fake._stream_client = types.SimpleNamespace(stream_bars=stream_bars)
        await C._stream_bars(self.fake, bar_type, "FCX26", "5", TradeStationBarUnit.MINUTE, None)
        await self.advance(self.now + 900 * S)  # 15 min more of wall clock: timers and tasks
        monkeypatch.setattr(asyncio, "sleep", REAL_SLEEP)
        return self

    def lines(self, levels=("debug", "info", "warning", "error")):
        out = []
        for lvl in levels:
            out += [f"{lvl}:{c.args[0]}" for c in getattr(self.fake._log, lvl).call_args_list]
        return [x.replace(str(BT), "{BT5}").replace(str(BTD), "{BTD}") for x in out]


SCENARIOS = {
    "late": [  # B closes 150 s late with the same values
        (DAY + "16:04:30", bar(DAY + "16:05:00Z")),
        (DAY + "16:04:50", bar(DAY + "16:05:00Z")),
        (DAY + "16:07:30", bar(DAY + "16:05:00Z", "Closed")),
        (DAY + "16:08:00", bar(DAY + "16:10:00Z")),
        (DAY + "16:10:00.1", bar(DAY + "16:10:00Z", "Closed")),
    ],
    "merged": [  # B's late Closed carries the next period's trades
        (DAY + "16:04:30", bar(DAY + "16:05:00Z")),
        (DAY + "16:07:30", bar(DAY + "16:05:00Z", c=2, v=3)),
        (DAY + "16:07:31", bar(DAY + "16:05:00Z", "Closed", c=2, v=3)),
        (DAY + "16:10:00.1", bar(DAY + "16:10:00Z", "Closed")),
    ],
    "hole": [  # merge + hole: 16:10 is never sent, 16:15 arrives
        (DAY + "16:04:30", bar(DAY + "16:05:00Z")),
        (DAY + "16:09:00", bar(DAY + "16:05:00Z", c=2, v=3)),
        (DAY + "16:09:01", bar(DAY + "16:05:00Z", "Closed", c=2, v=3)),
        (DAY + "16:11:00", bar(DAY + "16:15:00Z")),
        (DAY + "16:14:00", bar(DAY + "16:15:00Z")),
        (DAY + "16:15:00.1", bar(DAY + "16:15:00Z", "Closed")),
        (DAY + "16:20:00.1", bar(DAY + "16:20:00Z", "Closed")),
    ],
    "fallback": [  # no Closed for B: released on C's first update
        (DAY + "16:04:30", bar(DAY + "16:05:00Z")),
        (DAY + "16:06:00", bar(DAY + "16:10:00Z")),
        (DAY + "16:10:00.1", bar(DAY + "16:10:00Z", "Closed")),
    ],
    "reconnect": [  # a reconnect seed replays a bar that closed during the gap
        (DAY + "16:04:30", bar(DAY + "16:05:00Z")),
        (DAY + "16:05:00.1", bar(DAY + "16:05:00Z", "Closed")),
        (DAY + "16:07:00", {"_reconnected": True}),
        (DAY + "16:12:00", bar(DAY + "16:05:00Z", "Closed", False)),
        (DAY + "16:12:00", bar(DAY + "16:10:00Z", "Closed", False)),
        (DAY + "16:12:00", bar(DAY + "16:15:00Z", "Open", False)),
        (DAY + "16:15:00.1", bar(DAY + "16:15:00Z", "Closed")),
    ],
}
DAY_STEPS = [
    ("2026-10-15T20:00:00", bar("2026-10-16T00:00:00Z")),
    ("2026-10-16T01:00:00", bar("2026-10-16T00:00:00Z", "Closed")),
    ("2026-10-18T22:00:00", bar("2026-10-19T00:00:00Z")),
    ("2026-10-19T20:00:00", bar("2026-10-20T00:00:00Z")),
]


def rest_hole(now):
    return [bar(DAY + "16:05:00Z", "Closed", False, c=2, v=3),
            bar(DAY + "16:10:00Z", "Closed", False, c=3, v=5),
            bar(DAY + "16:15:00Z", "Open", False)]


# Captured from the adapter BEFORE this change (base 201a70e) with the same harness: today's
# emissions and log lines, debug included.
_FB = "no Closed event - emitted on the next bar's first update"
GOLDEN = {
    "day": {"emitted": ["10-16T00:00", "10-19T00:00"],
            "lines": ["debug:Bar emitted for {BTD}: ts=2026-10-16T00:00:00Z",
                      "debug:Bar emitted for {BTD}: ts=2026-10-19T00:00:00Z",
                      "info:Bar SSE stream started for {BTD}",
                      "info:[BAR-CLOSE-FALLBACK] {BTD} ts=2026-10-19T00:00:00Z: " + _FB]},
    "fallback": {"emitted": ["10-16T16:05", "10-16T16:10"],
                 "lines": ["debug:Bar emitted for {BT5}: ts=2026-10-16T16:05:00Z",
                           "debug:Bar emitted for {BT5}: ts=2026-10-16T16:10:00Z",
                           "info:Bar SSE stream started for {BT5}",
                           "info:[BAR-CLOSE-FALLBACK] {BT5} ts=2026-10-16T16:05:00Z: " + _FB]},
    "hole": {"emitted": ["10-16T16:05", "10-16T16:15", "10-16T16:20"],
             "lines": ["debug:Bar emitted for {BT5}: ts=2026-10-16T16:05:00Z",
                       "debug:Bar emitted for {BT5}: ts=2026-10-16T16:15:00Z",
                       "debug:Bar emitted for {BT5}: ts=2026-10-16T16:20:00Z",
                       "info:Bar SSE stream started for {BT5}"]},
    "late": {"emitted": ["10-16T16:05", "10-16T16:10"],
             "lines": ["debug:Bar emitted for {BT5}: ts=2026-10-16T16:05:00Z",
                       "debug:Bar emitted for {BT5}: ts=2026-10-16T16:10:00Z",
                       "info:Bar SSE stream started for {BT5}"]},
    "merged": {"emitted": ["10-16T16:05", "10-16T16:10"],
               "lines": ["debug:Bar emitted for {BT5}: ts=2026-10-16T16:05:00Z",
                         "debug:Bar emitted for {BT5}: ts=2026-10-16T16:10:00Z",
                         "info:Bar SSE stream started for {BT5}"]},
    "reconnect": {"emitted": ["10-16T16:05", "10-16T16:10", "10-16T16:15"],
                  "lines": ["debug:Bar emitted for {BT5}: ts=2026-10-16T16:05:00Z",
                            "debug:Bar emitted for {BT5}: ts=2026-10-16T16:10:00Z",
                            "debug:Bar emitted for {BT5}: ts=2026-10-16T16:15:00Z",
                            "info:Bar SSE stream started for {BT5}",
                            ("info:[BAR-GAP-REPLAY] {BT5} ts=2026-10-16T16:10:00Z: closed while "
                             "the stream was down - replayed from the reconnect seed")]},
}


async def _scenario(name, mode, monkeypatch, rest=rest_hole):
    h = Harness(mode, rest=rest)
    if name == "day":
        return await h.run(DAY_STEPS, bar_type=BTD, monkeypatch=monkeypatch)
    return await h.run(SCENARIOS[name], monkeypatch=monkeypatch)


@pytest.mark.parametrize("name", sorted(GOLDEN))
async def test_shadow_mode_changes_no_emission(name, monkeypatch):
    off = await _scenario(name, "off", monkeypatch)
    assert off.fake.emitted == GOLDEN[name]["emitted"]
    assert off.lines() == GOLDEN[name]["lines"]  # off: today's behaviour, logs included
    assert off.get_bars_calls == [] and off.timers == []
    shadow = await _scenario(name, "shadow", monkeypatch)
    assert shadow.fake.emitted == GOLDEN[name]["emitted"]  # shadow: only log lines differ
    assert set(shadow.fake._last_bar_ts) == set(off.fake._last_bar_ts)
    shadow_only = set(shadow.lines(("info", "warning", "error"))) - set(GOLDEN[name]["lines"])
    assert not any("[BAR-RELEASE]" in x or "[BAR-BACKFILL]" in x for x in shadow_only)
    assert all("Bar emitted" not in x for x in shadow_only)


async def test_shadow_scenarios_actually_measure(monkeypatch):
    late = (await _scenario("late", "shadow", monkeypatch)).lines()
    assert sum("[BAR-RELEASE-SHADOW]" in x for x in late) == 1  # one per bar, never repeated
    merged = (await _scenario("merged", "shadow", monkeypatch)).lines()
    div = [x for x in merged if "[BAR-RELEASE-DIVERGED]" in x]
    assert div == [("info:[BAR-RELEASE-DIVERGED] {BT5} ts=2026-10-16T16:05:00Z "
                    "snapshot=1.0/1.0/1.0/1.0/1.0 closed=1/2/1/2/3 rest_eq_snapshot=False "
                    "rest=1/2/1/2/3")]
    hole = (await _scenario("hole", "shadow", monkeypatch)).lines()
    bf = [x for x in hole if "[BAR-BACKFILL-SHADOW]" in x]
    assert bf == [("info:[BAR-BACKFILL-SHADOW] {BT5} delay_s=60.0 "
                   "missing=[2026-10-16T16:10:00Z] status=[Closed] "
                   "history_has=[2026-10-16T16:10:00Z] vol=[5]")]
    snaps = [x for x in late if "[BAR-SNAPSHOT]" in x]
    assert snaps[0] == ("debug:[BAR-SNAPSHOT] {BT5} ts=2026-10-16T16:05:00Z closed_lag_s=150.000 "
                        "eq0=True eq2=True eq10=True eq30=True vol_closed=1 vol10=1")


@pytest.mark.parametrize("name", sorted(GOLDEN))
async def test_on_mode_never_emits_a_bar_twice(name, monkeypatch):
    on = await _scenario(name, "on", monkeypatch)
    assert len(on.fake.emitted) == len(set(on.fake.emitted))
    assert on.fake.emitted == sorted(on.fake.emitted)


async def test_backfill_emits_before_the_parked_next_bar(monkeypatch):
    h = Harness("on")

    async def slow_rest(now):  # the read takes until after 16:15's Closed came
        while h.now < _ns(DAY + "16:15:30"):
            await REAL_SLEEP(0)
        return rest_hole(h.now)

    h.rest = slow_rest
    await h.run(SCENARIOS["hole"], monkeypatch=monkeypatch)
    assert h.fake.emitted == ["10-16T16:05", "10-16T16:10", "10-16T16:15", "10-16T16:20"]
    info = h.lines(("info",))
    assert any(x.startswith("info:[BAR-BACKFILL] {BT5} ts=2026-10-16T16:10:00Z") for x in info)
    assert any(x.startswith("info:[BAR-RELEASE] {BT5} ts=2026-10-16T16:05:00Z") for x in info)
    backfill_reads = [t for t, kw in h.get_bars_calls if kw["barsback"] == 1 + _STREAM_BARSBACK]
    assert backfill_reads == [_ns(DAY + "16:12:00")]  # missing_end + _BACKFILL_DELAY_S


async def test_backfill_retries_once_when_history_is_still_open(monkeypatch):
    def rest(now):
        status = "Open" if now < _ns(DAY + "16:13:00") else "Closed"
        return [bar(DAY + "16:10:00Z", status, False, c=3, v=5)]

    h = await Harness("on", rest=rest).run(SCENARIOS["hole"], monkeypatch=monkeypatch)
    reads = [t for t, kw in h.get_bars_calls if kw["barsback"] == 1 + _STREAM_BARSBACK]
    assert reads == [_ns(DAY + "16:12:00"), _ns(DAY + "16:13:00")]
    assert h.fake.emitted == ["10-16T16:05", "10-16T16:10", "10-16T16:15", "10-16T16:20"]


async def test_backfill_skip_when_history_has_no_bar(monkeypatch):
    h = await Harness("on", rest=lambda now: []).run(SCENARIOS["hole"], monkeypatch=monkeypatch)
    assert h.fake.emitted == ["10-16T16:05", "10-16T16:15", "10-16T16:20"]
    assert "info:[BAR-BACKFILL-SKIP] {BT5} ts=2026-10-16T16:10:00Z: no history bar (no trades)" \
        in h.lines(("info",))
    # handled once: 16:15's later updates never re-trigger it
    assert len([1 for _t, kw in h.get_bars_calls if kw["barsback"] == 11]) == 1


async def test_backfill_failed_releases_parked_bars(monkeypatch):
    h = Harness("on")

    async def failing(now):
        while h.now < _ns(DAY + "16:15:30"):  # 16:15 Closed is parked meanwhile
            await REAL_SLEEP(0)
        return RuntimeError("boom")

    h.rest = failing
    await h.run(SCENARIOS["hole"], monkeypatch=monkeypatch)
    assert h.fake.emitted == ["10-16T16:05", "10-16T16:15", "10-16T16:20"]
    warn = h.lines(("warning",))
    assert warn == [("warning:[BAR-BACKFILL-FAILED] {BT5} missing=[2026-10-16T16:10:00Z]: "
                     "RuntimeError('boom')")]


async def test_backfill_shadow_fetches_and_logs_without_emitting(monkeypatch):
    def rest(now):  # Open at +60 s, Closed from +120 s
        status = "Open" if now < _ns(DAY + "16:12:00") else "Closed"
        return [bar(DAY + "16:10:00Z", status, False, c=3, v=5)]

    h = await Harness("shadow", rest=rest).run(SCENARIOS["hole"], monkeypatch=monkeypatch)
    assert h.fake.emitted == GOLDEN["hole"]["emitted"]
    bf = [x for x in h.lines(("info",)) if "[BAR-BACKFILL-SHADOW]" in x]
    assert [x.split(" delay_s=")[1].split(" ")[0] for x in bf] == ["60.0", "120.0"]
    assert "status=[Open] history_has=[] vol=[]" in bf[0]
    assert "status=[Closed] history_has=[2026-10-16T16:10:00Z] vol=[5]" in bf[1]


async def test_backfill_shadow_late_trigger_collapses_passed_delays(monkeypatch):
    steps = [
        (DAY + "16:04:30", bar(DAY + "16:05:00Z")),
        (DAY + "16:05:00.1", bar(DAY + "16:05:00Z", "Closed")),
        (DAY + "16:12:10", bar(DAY + "16:15:00Z")),  # first later update at missing_end + 130
    ]

    def rest(now):
        status = "Open" if now < _ns(DAY + "16:13:00") else "Closed"
        return [bar(DAY + "16:10:00Z", status, False)]

    h = await Harness("shadow", rest=rest).run(steps, monkeypatch=monkeypatch)
    bf = [x for x in h.lines(("info",)) if "[BAR-BACKFILL-SHADOW]" in x]
    assert [x.split(" delay_s=")[1].split(" ")[0] for x in bf] == ["130.0", "180.0"]


async def test_release_never_arms_for_day_bars(monkeypatch):
    for mode in ("shadow", "on"):
        h = await _scenario("day", mode, monkeypatch)
        assert h.timers == [] and h.get_bars_calls == []
        assert h.fake.emitted == GOLDEN["day"]["emitted"]
        assert h.lines() == GOLDEN["day"]["lines"]


async def test_poll_mode_never_backfills(monkeypatch):
    polls = iter(
        [
            [ev("16:00", "Closed", False), ev("16:05", "Open", False)],
            [ev("16:10", "Closed", False), ev("16:15", "Open", False)],  # 16:05 missing
            [ev("16:15", "Open", False)],  # late: no release either
        ]
    )

    async def get_bars(**kw):
        try:
            return next(polls)
        except StopIteration:
            raise asyncio.CancelledError

    async def no_sleep(_s):
        return None

    monkeypatch.setattr(asyncio, "sleep", no_sleep)
    fake = _fake([], mode="on")
    fake.now[0] = _ns("2026-10-16T17:00:00Z")
    fake._client = types.SimpleNamespace(get_bars=get_bars)
    await C._poll_bars(fake, BT, "FCX26", "5", TradeStationBarUnit.MINUTE, None, 300.0)
    assert fake.emitted == ["16:10"]
    assert fake.timers == [] and fake._backfill_tasks == {}


def _feed(fake, *events):
    gate = fake._bar_gates.setdefault(BT, BarCloseGate())
    for e in events:
        C._emit_gated(fake, gate, e, BT, None)
    return gate


async def test_release_on_emits_at_end_plus_grace_once():
    fake = _fake([], mode="on")
    end = _ns("2026-10-16T16:10:00Z")
    fake.now[0] = end - 30 * S
    gate = _feed(fake, ev("16:05", "Closed"), ev("16:10"))
    assert len(fake.timers) == 1 and fake.timers[0].delay == 40.0
    _feed(fake, ev("16:10"))  # another update of the same bar: no new timer
    assert len(fake.timers) == 1
    fake.now[0] = end + 10 * S
    h = fake.timers[0]
    h.cb(*h.args)
    assert fake.emitted == ["16:05", "16:10"]
    assert any(str(c.args[0]).startswith(f"[BAR-RELEASE] {BT} ts=2026-10-16T16:10:00Z")
               for c in fake._log.info.call_args_list)
    _feed(fake, ev("16:10"), ev("16:10", "Closed"))  # its late Closed
    h.cb(*h.args)  # a stale repeat of the callback
    assert fake.emitted == ["16:05", "16:10"]
    assert gate.last_emitted_ns == end


async def test_release_timer_fired_early_rearms():
    fake = _fake([], mode="on")
    end = _ns("2026-10-16T16:10:00Z")
    fake.now[0] = end
    _feed(fake, ev("16:05", "Closed"), ev("16:10"))
    fake.now[0] = end + 10 * S - 1000  # monotonic vs wall clock: a hair early
    fake.timers[0].cb(*fake.timers[0].args)
    assert fake.emitted == ["16:05"] and len(fake.timers) == 2
    fake.now[0] = end + 10 * S
    fake.timers[1].cb(*fake.timers[1].args)
    assert fake.emitted == ["16:05", "16:10"]


async def test_shadow_release_logs_and_keeps_pending_for_the_real_closed():
    fake = _fake([], mode="shadow")
    end = _ns("2026-10-16T16:10:00Z")
    fake.now[0] = end - S
    _feed(fake, ev("16:05", "Closed"), ev("16:10"))
    fake.now[0] = end + 10 * S
    fake.timers[0].cb(*fake.timers[0].args)
    shadow = [str(c.args[0]) for c in fake._log.info.call_args_list
              if "[BAR-RELEASE-SHADOW]" in str(c.args[0])]
    assert shadow == [(f"[BAR-RELEASE-SHADOW] {BT} would emit ts=2026-10-16T16:10:00Z "
                       f"O/H/L/C/V=1/1/1/1/1")]
    assert fake.emitted == ["16:05"]
    _feed(fake, ev("16:10"))  # a later update: not re-armed, not logged again
    assert len(fake.timers) == 1
    _feed(fake, ev("16:10", "Closed"))
    assert fake.emitted == ["16:05", "16:10"]


async def test_timer_cancelled_on_reconnect():
    fake = _fake(
        [ev("16:05", "Closed"), ev("16:10"), {"_reconnected": True},
         ev("16:10", "Open", False), ev("16:10", "Closed")],
        mode="on",
    )
    fake.now[0] = _ns("2026-10-16T16:09:00Z")
    await C._stream_bars(fake, BT, "FCX26", "5", TradeStationBarUnit.MINUTE, None)
    first = fake.timers[0]
    assert first.cancelled  # the reconnect cancelled it
    assert fake.emitted == ["16:05", "16:10"]
    fake.now[0] = _ns("2026-10-16T16:11:00Z")
    for h in fake.timers:  # stale timers fire after the Closed: harmless
        h.cb(*h.args)
    assert fake.emitted == ["16:05", "16:10"]


async def test_reconnect_aborts_a_backfill_and_the_seed_replays_in_order(monkeypatch):
    steps = [
        (DAY + "16:04:30", bar(DAY + "16:05:00Z")),
        (DAY + "16:05:00.1", bar(DAY + "16:05:00Z", "Closed")),
        (DAY + "16:11:00", bar(DAY + "16:15:00Z")),  # 16:10 missing: hold + backfill
        (DAY + "16:11:30", {"_reconnected": True}),  # before the backfill read
        (DAY + "16:11:31", bar(DAY + "16:05:00Z", "Closed", False)),
        (DAY + "16:11:31", bar(DAY + "16:10:00Z", "Closed", False)),
        (DAY + "16:11:31", bar(DAY + "16:15:00Z", "Open", False)),
        (DAY + "16:15:00.1", bar(DAY + "16:15:00Z", "Closed")),
    ]
    h = await Harness("on", rest=rest_hole).run(steps, monkeypatch=monkeypatch)
    assert h.fake.emitted == ["10-16T16:05", "10-16T16:10", "10-16T16:15"]
    assert h.get_bars_calls == []  # the cancelled backfill never read


async def test_unsubscribe_and_disconnect_cancel_timer_and_backfill():
    for path in ("unsubscribe", "disconnect"):
        fake = _fake([], mode="on")
        fake.now[0] = _ns("2026-10-16T16:11:00Z")
        gate = _feed(fake, ev("16:05", "Closed"), ev("16:15"))  # hole at 16:10
        task = fake._backfill_tasks[BT]
        timer = fake.timers[0]
        fake._bar_subscriptions = {}
        if path == "unsubscribe":
            await C._unsubscribe_bars(fake, types.SimpleNamespace(bar_type=BT))
        else:
            fake._quote_subscriptions = {}
            fake._trade_subscriptions = {}
            fake._quote_stream_tasks = {}
            fake._quote_stream_wants_quotes = set()
            fake._quote_stream_wants_trades = set()
            fake._last_quote = {}
            fake._last_trade = {}
            fake._client.close = AsyncMock()
            await C._disconnect(fake)
        await REAL_SLEEP(0)
        assert timer.cancelled and task.cancelled()
        assert fake._release_timers == {} and fake._backfill_tasks == {}
        assert gate._barrier_ns is None and gate.parked == []


async def test_reconnect_bar_streams_cancels_timer_and_backfill():
    fake = _fake([], mode="on")
    fake.now[0] = _ns("2026-10-16T16:11:00Z")
    _feed(fake, ev("16:05", "Closed"), ev("16:15"))
    task, timer = fake._backfill_tasks[BT], fake.timers[0]
    fake._use_streaming = True
    fake._stream_client = object()
    fake._client._ensure_authenticated = AsyncMock()
    fake._bar_subscriptions = {BT: asyncio.get_running_loop().create_task(REAL_SLEEP(3600))}
    fake._cache = types.SimpleNamespace(instrument=lambda iid: None)
    await C.reconnect_bar_streams(fake, stale_only=False)
    await REAL_SLEEP(0)
    assert timer.cancelled and task.cancelled() and fake._backfill_tasks == {}


def test_snapshot_offsets_match_the_gate_default():
    assert data_mod._SNAPSHOT_OFFSETS_NS == BarCloseGate().snapshot_offsets_ns
    assert data_mod._CLOSE_GRACE_NS in data_mod._SNAPSHOT_OFFSETS_NS
    assert data_mod._BACKFILL_SHADOW_DELAYS_S == (60.0, 120.0, 180.0, 300.0)
    assert (data_mod._BACKFILL_DELAY_S, data_mod._BACKFILL_RETRY_S,
            data_mod._BACKFILL_MAX_PERIODS) == (120.0, 60.0, 3)


# -- review fix pass ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "broken", ["_maybe_start_backfill", "_log_snapshot", "_drain_diverged", "_arm_release"]
)
@pytest.mark.parametrize("mode", ["shadow", "on"])
async def test_a_failing_release_helper_never_stops_the_stream(broken, mode, monkeypatch):
    events = [ev("16:05"), ev("16:05", "Closed"), ev("16:15"), ev("16:15", "Closed")]
    f = _fake(events, mode=mode)
    f.now[0] = _ns("2026-10-16T16:05:00.1Z")

    def boom(*a, **k):
        raise RuntimeError("boom")

    async def no_sleep(_s):
        return None

    setattr(f, broken, boom)
    await C._stream_bars(f, BT, "FCX26", "5", TradeStationBarUnit.MINUTE, None)
    monkeypatch.setattr(asyncio, "sleep", no_sleep)
    await asyncio.gather(*list(f._backfill_tasks.values()))  # let a live backfill finish
    monkeypatch.setattr(asyncio, "sleep", REAL_SLEEP)
    assert f.emitted == ["16:05", "16:15"]  # as today
    errors = [str(c.args[0]) for c in f._log.error.call_args_list]
    assert errors and all(e.startswith(f"[BAR-RELEASE-ERROR] {BT} ") for e in errors)
    gate = f._bar_gates[BT]
    assert gate._barrier_ns is None and gate.parked == [] and gate._backfill_active is None


async def test_a_reset_lets_a_seed_replayed_open_re_arm_the_release():
    fake = _fake([], mode="shadow")
    end = _ns("2026-10-16T16:10:00Z")
    fake.now[0] = end - S
    _feed(fake, ev("16:05", "Closed"), ev("16:10"))
    fake.now[0] = end + 10 * S
    fake.timers[0].cb(*fake.timers[0].args)  # shadow fired for this target
    data_mod._cancel_bar_jobs(fake, BT)  # a reset
    fake._bar_gates[BT].reset_connection()
    _feed(fake, ev("16:10", "Open", False))  # the new connection's seed
    assert len(fake.timers) == 2 and not fake.timers[1].cancelled


@pytest.mark.parametrize("mode", ["shadow", "on"])
async def test_cancelled_backfill_is_logged(mode):
    fake = _fake([], mode=mode)
    fake.now[0] = _ns("2026-10-16T16:16:00Z")
    _feed(fake, ev("16:05", "Closed"), ev("16:20"))  # 16:10 and 16:15 missing
    task = fake._backfill_tasks[BT]
    data_mod._cancel_bar_jobs(fake, BT)
    await REAL_SLEEP(0)
    assert task.cancelled()
    lines = [str(c.args[0]) for c in fake._log.info.call_args_list
             if "[BAR-BACKFILL-CANCELLED]" in str(c.args[0])]
    assert lines == [(f"[BAR-BACKFILL-CANCELLED] {BT} "
                      f"missing=[2026-10-16T16:10:00Z,2026-10-16T16:15:00Z] reason=reset")]
    data_mod._cancel_bar_jobs(fake, BT)  # nothing in flight: no second line
    assert sum("[BAR-BACKFILL-CANCELLED]" in str(c.args[0])
               for c in fake._log.info.call_args_list) == 1
