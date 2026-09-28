"""reconnect_bar_streams(stale_only=True): per-bar-type staleness threshold.

Found 2026-09-26: the node's stream watchdog passes max_age_secs=1800. With a
single threshold, a pass triggered by one stale 15-min stream also tore down
every hourly/daily stream idle 30-60+ min — normal for those bar sizes. The
threshold is now max(max_age_secs, 2 x bar interval) per subscription.

These call the REAL method on a minimal fake `self` (no stub replica).
"""
from __future__ import annotations

import asyncio
import time
import types
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

from nautilus_trader.model.data import BarType

from tradestation_nt_community.data import TradeStationDataClient

H1 = BarType.from_str("GCZ26.TRADESTATION-1-HOUR-LAST-EXTERNAL")
M15 = BarType.from_str("ESZ26.TRADESTATION-15-MINUTE-LAST-EXTERNAL")
D1 = BarType.from_str("FDAXZ26.TRADESTATION-1-DAY-LAST-EXTERNAL")


def _iso_ago(minutes: float) -> str:
    return datetime.fromtimestamp(time.time() - minutes * 60, tz=timezone.utc).isoformat()


async def _run(ages_min: dict, max_age_secs: float = 1800, stale_only: bool = True,
               **kw):
    """Return (set of bar types reconnected, fake self) for assertion."""
    tasks = {bt: asyncio.get_running_loop().create_task(asyncio.sleep(3600)) for bt in ages_min}
    fake = types.SimpleNamespace(
        _use_streaming=True,
        _stream_client=object(),
        _client=types.SimpleNamespace(_ensure_authenticated=AsyncMock()),
        _bar_subscriptions=dict(tasks),
        _last_bar_ts={bt: _iso_ago(m) for bt, m in ages_min.items() if m is not None},
        _cache=types.SimpleNamespace(instrument=lambda iid: None),  # skip re-create
        _log=MagicMock(),
        _loop=asyncio.get_running_loop(),
    )
    await TradeStationDataClient.reconnect_bar_streams(
        fake, stale_only=stale_only, max_age_secs=max_age_secs, **kw)
    out = {bt for bt, t in tasks.items() if t.cancelled()}
    for t in tasks.values():
        t.cancel()
    return out, fake


async def test_hourly_stream_45_min_is_not_reconnected():
    assert (await _run({H1: 45}))[0] == set()


async def test_hourly_stream_125_min_is_reconnected():
    assert (await _run({H1: 125}))[0] == {H1}


async def test_15_min_stream_35_min_is_reconnected():
    assert (await _run({M15: 35}))[0] == {M15}


async def test_mixed_pass_reconnects_only_the_stale_one():
    assert (await _run({M15: 35, H1: 45, D1: 600}))[0] == {M15}


async def test_max_age_secs_is_a_floor():
    # 15-min bars: 2 x 15 min = 30 min < 7200 s floor → 60 min idle is fresh
    assert (await _run({M15: 60}, max_age_secs=7200))[0] == set()


async def test_stale_only_false_reconnects_all():
    assert (await _run({H1: 1, M15: 1}, stale_only=False))[0] == {H1, M15}


async def test_no_timestamp_still_reconnects():
    assert (await _run({H1: None}))[0] == {H1}


def test_bar_interval_secs():
    from tradestation_nt_community.data import _bar_interval_secs

    assert _bar_interval_secs(M15) == 900
    assert _bar_interval_secs(H1) == 3600
    assert _bar_interval_secs(D1) == 86400
    assert _bar_interval_secs(BarType.from_str("ESZ26.TRADESTATION-100-TICK-LAST-EXTERNAL")) is None
    assert _bar_interval_secs("garbage") is None
    assert _bar_interval_secs(None) is None


async def test_unparseable_bar_type_uses_max_age_secs():
    """40 min idle, max_age 30 min: with the fallback it IS reconnected. If an
    unknown interval were treated as e.g. 1 hour (threshold 2 h), it would not."""
    class _OddBarType:  # hashable; aggregation the adapter has no interval for
        spec = types.SimpleNamespace(step=100, aggregation="TICK")
        instrument_id = types.SimpleNamespace(symbol=types.SimpleNamespace(value="ESZ26"))

    bad = _OddBarType()
    assert (await _run({bad: 40}))[0] == {bad}


# ── bar_types scoping (fix round 2) ────────────────────────────────────────

async def test_bar_types_scopes_reconnect_to_listed_streams():
    # M15 idle 3 h is stale on its own merits but NOT listed → untouched.
    (out, _) = await _run({M15: 180, H1: 125}, bar_types=[H1])
    assert out == {H1}


async def test_bar_types_listed_but_fresh_is_not_reconnected():
    assert (await _run({H1: 45, M15: 180}, bar_types=[H1]))[0] == set()


async def test_bar_types_empty_touches_nothing():
    assert (await _run({M15: 180}, bar_types=[]))[0] == set()


async def test_bar_types_none_keeps_global_sweep():
    assert (await _run({M15: 180, H1: 125}, bar_types=None))[0] == {M15, H1}


async def test_token_is_refreshed_through_the_real_client_attribute():
    # 2026-09-28 stable.log: "... object has no attribute '_http_client'" on every reconnect
    _, fake = await _run({M15: 35})
    fake._client._ensure_authenticated.assert_awaited_once()
    assert not any("Token refresh" in str(c) for c in fake._log.warning.call_args_list)
