"""Tests for Phase 0A order attribution.

Task 0A-1: order_attribution() pure function.
Task 0A-2: [ORDER-SUBMIT] log line emitted in _submit_order.
"""
from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from tradestation_nt_community._attribution import order_attribution
from tradestation_nt_community.execution import TradeStationExecutionClient
from tests.test_kit import make_market_order


# ---------------------------------------------------------------------------
# Task 0A-1: pure order_attribution() function
# ---------------------------------------------------------------------------

class TestOrderAttribution:
    def test_attribution_includes_tags_and_strategy(self):
        order = make_market_order(tags=["purpose=exit", "heal"])
        attr = order_attribution(order)
        assert attr["client_order_id"] == str(order.client_order_id)
        assert attr["strategy_id"] == str(order.strategy_id)
        assert attr["instrument_id"] == str(order.instrument_id)
        assert attr["side"] == order.side.name
        assert attr["tags"] == ["purpose=exit", "heal"]

    def test_attribution_handles_no_tags(self):
        order = make_market_order(tags=None)
        attr = order_attribution(order)
        assert attr["tags"] == []

    def test_attribution_includes_type(self):
        order = make_market_order(tags=None)
        attr = order_attribution(order)
        assert "type" in attr
        assert attr["type"] == order.order_type.name


# ---------------------------------------------------------------------------
# Task 0A-2: [ORDER-SUBMIT] log line in _submit_order
# ---------------------------------------------------------------------------
#
# Strategy: call _submit_order unbound against a stub self — the same idiom
# used by test_status_safety_poll.py and test_order_map_persistence.py in this
# repo.  NautilusTrader routes self._log.info() through its Rust backend, not
# stdlib logging, so caplog cannot capture it.  Asserting on
# _log.info.call_args_list is the correct approach for this codebase.
# ---------------------------------------------------------------------------

def _make_submit_order_stub() -> MagicMock:
    """Build a minimal stub self for calling _submit_order unbound."""
    stub = MagicMock()

    # _client.place_order must return a real dict (it's .get()'d for OrderID)
    stub._client.place_order = AsyncMock(return_value={"OrderID": "TS-123"})

    # _convert_order_to_ts_format must return a real dict (it's **-unpacked)
    stub._convert_order_to_ts_format = MagicMock(return_value={
        "AccountID": "SIM0000001F",
        "Symbol": "GCJ26",
        "Quantity": "1",
        "OrderType": "Market",
        "TradeAction": "BUY",
        "TimeInForce": {"Duration": "DAY"},
        "Route": "Intelligent",
    })

    # Order ID maps
    stub._ts_order_id_to_client_order_id = {}
    stub._client_order_id_to_ts_order_id = {}

    # Persistence (no-op)
    stub._persist_order_map = MagicMock()

    # Clock
    stub._clock.timestamp_ns = MagicMock(return_value=1_000_000_000)

    # Logger — real MagicMock so we can inspect .info calls
    stub._log = MagicMock()

    # generate_order_accepted / generate_order_rejected are auto-mocked
    return stub


def _build_submit_command(order):
    """Wrap an order in a minimal SubmitOrder command stub."""
    from nautilus_trader.execution.messages import SubmitOrder
    from nautilus_trader.core.uuid import UUID4
    from nautilus_trader.model.identifiers import TraderId, StrategyId

    cmd = MagicMock(spec=SubmitOrder)
    cmd.order = order
    cmd.trader_id = TraderId("T-001")
    cmd.strategy_id = StrategyId("S-001")
    return cmd


class TestSubmitOrderLogging:
    async def test_order_submit_log_contains_attribution(self):
        """_submit_order must emit an [ORDER-SUBMIT] info line with attribution fields."""
        order = make_market_order(tags=["purpose=exit", "heal"])
        stub = _make_submit_order_stub()
        command = _build_submit_command(order)

        await TradeStationExecutionClient._submit_order(stub, command)

        # Collect all info() call arg strings
        info_messages = [
            str(call.args[0]) if call.args else ""
            for call in stub._log.info.call_args_list
        ]

        submit_lines = [m for m in info_messages if "[ORDER-SUBMIT]" in m]
        assert submit_lines, (
            f"No [ORDER-SUBMIT] log line found. All info calls: {info_messages}"
        )

        # The line must be valid JSON after stripping the prefix
        line = submit_lines[0]
        json_part = line[len("[ORDER-SUBMIT] "):]
        payload = json.loads(json_part)

        assert payload["tags"] == ["purpose=exit", "heal"]
        assert payload["strategy_id"] == str(order.strategy_id)
        assert payload["client_order_id"] == str(order.client_order_id)
        assert payload["ts_order_id"] == "TS-123"

    async def test_order_submit_log_no_tags(self):
        """[ORDER-SUBMIT] line carries empty tags list when order has no tags."""
        order = make_market_order(tags=None)
        stub = _make_submit_order_stub()
        command = _build_submit_command(order)

        await TradeStationExecutionClient._submit_order(stub, command)

        info_messages = [
            str(call.args[0]) if call.args else ""
            for call in stub._log.info.call_args_list
        ]
        submit_lines = [m for m in info_messages if "[ORDER-SUBMIT]" in m]
        assert submit_lines, "No [ORDER-SUBMIT] log line found."

        payload = json.loads(submit_lines[0][len("[ORDER-SUBMIT] "):])
        assert payload["tags"] == []
