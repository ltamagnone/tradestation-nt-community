"""_stream_bars / _request_bars / _poll_bars, calling the REAL methods (strategies LESSONS §207).

Events are TradeStation v3 shaped (BarStatus, IsRealtime, TimeStamp = bar END) as captured
live on 2026-10-16.
"""

from __future__ import annotations

import asyncio
import types
from unittest.mock import AsyncMock, MagicMock

from nautilus_trader.model.data import BarType

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


def _fake(events, gates=None):
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
    )
    fake._parse_bars = lambda raws, bt, inst: [r["TimeStamp"][11:16] for r in raws]
    fake._handle_data = fake.emitted.append
    fake._emit_gated = lambda *a: C._emit_gated(fake, *a)
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
