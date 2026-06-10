"""Tests for the streaming-mode status safety poll (§95, June 2026).

Why this exists: the order-fill SSE stream can go one-way-dead — heartbeats
keep flowing (so the 90s read timeout never fires) while FLL events are
silently dropped.  In streaming mode `_check_order_statuses()` previously ran
only on the reconnect sentinel, so missed fills stayed invisible for hours
(HGN26: 3h01m on 2026-06-09; GCQ26: 2h15m on 2026-06-10 — positions sat at
the broker with no exits and no ownership record).

The fix: a low-frequency HTTP status poll runs alongside the SSE stream.
`_check_order_statuses()` is already idempotent (skips unchanged statuses and
closed orders, returns immediately when nothing is tracked), so the poll is
pure safety net — worst case one no-op GET per interval.
"""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from tradestation_nt_community.config import TradeStationExecClientConfig
from tradestation_nt_community.execution import TradeStationExecutionClient


# ---------------------------------------------------------------------------
# Config plumbing
# ---------------------------------------------------------------------------

class TestConfigField:
    def test_default_poll_interval_is_60s(self):
        cfg = TradeStationExecClientConfig(account_id="SIM1")
        assert cfg.streaming_status_poll_secs == 60.0

    def test_interval_is_configurable(self):
        cfg = TradeStationExecClientConfig(account_id="SIM1", streaming_status_poll_secs=15.0)
        assert cfg.streaming_status_poll_secs == 15.0

    def test_zero_disables(self):
        cfg = TradeStationExecClientConfig(account_id="SIM1", streaming_status_poll_secs=0.0)
        assert cfg.streaming_status_poll_secs == 0.0


# ---------------------------------------------------------------------------
# The loop itself — run unbound against a stub self (no live Component)
# ---------------------------------------------------------------------------

def _stub_client(interval: float = 0.01) -> MagicMock:
    stub = MagicMock()
    stub._status_safety_poll_secs = interval
    stub._check_order_statuses = AsyncMock()
    stub._log = MagicMock()
    return stub


class TestStatusSafetyPollLoop:
    @pytest.mark.asyncio
    async def test_polls_check_order_statuses_periodically(self):
        stub = _stub_client(interval=0.01)
        task = asyncio.ensure_future(
            TradeStationExecutionClient._status_safety_poll_loop(stub)
        )
        await asyncio.sleep(0.06)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        assert stub._check_order_statuses.await_count >= 2

    @pytest.mark.asyncio
    async def test_loop_survives_check_exceptions(self):
        stub = _stub_client(interval=0.01)
        stub._check_order_statuses = AsyncMock(
            side_effect=[RuntimeError("boom"), None, None, None, None, None]
        )
        task = asyncio.ensure_future(
            TradeStationExecutionClient._status_safety_poll_loop(stub)
        )
        await asyncio.sleep(0.06)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        # Continued past the first exception
        assert stub._check_order_statuses.await_count >= 2

    @pytest.mark.asyncio
    async def test_cancellation_exits_cleanly(self):
        stub = _stub_client(interval=10.0)  # long sleep — cancel hits the sleep
        task = asyncio.ensure_future(
            TradeStationExecutionClient._status_safety_poll_loop(stub)
        )
        await asyncio.sleep(0.01)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
