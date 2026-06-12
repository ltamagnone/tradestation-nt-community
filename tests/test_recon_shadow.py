"""Tests for Phase 2 Task 2-1: RECON-SHADOW shadow instrumentation (OBSERVE-ONLY).

The gap being measured:
  `generate_order_status_reports` reconciles broker orders. For orders not in
  `_ts_order_id_to_client_order_id` it mints a ClientOrderId(f"TS-{ts_order_id}")
  but never registers it. The safety poll `_check_order_statuses` then double-skips
  such orders: first because they're not in the map, second because they're not in
  the NautilusTrader cache. This task emits a structured [RECON-SHADOW] debug log
  for every order matching both conditions, so the gap frequency can be measured
  in production logs before any fix is attempted.

Design notes:
  - NautilusTrader routes self._log through a Rust backend; caplog does NOT work.
    Assertions inspect stub._log.debug.call_args_list directly.
  - The stub idiom follows test_order_attribution.py and test_status_safety_poll.py:
    call the real method unbound against a minimal MagicMock stub.
  - The log is emitted BEFORE instrument/open_only filters so that filled external
    orders (the highest-value gap case) are never silently dropped.
"""
from __future__ import annotations

import asyncio
import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from tradestation_nt_community.execution import TradeStationExecutionClient
from nautilus_trader.model.identifiers import ClientOrderId


_RESOURCES = Path(__file__).parent / "resources"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_orders_external() -> list[dict]:
    """Load the two-order fixture: one external (unmapped), one known."""
    return json.loads((_RESOURCES / "orders_external.json").read_text())


def _make_stub(orders: list[dict]) -> MagicMock:
    """Build a minimal stub self for calling generate_order_status_reports unbound.

    - _ts_order_id_to_client_order_id maps only TS-KNOWN-001 so TS-EXTERNAL-999
      is unmapped (condition a).
    - _cache.order() returns None for all orders (condition b).
    """
    stub = MagicMock()

    # HTTP client returns the fixture orders
    stub._client.get_orders = AsyncMock(return_value=orders)
    stub._account_id = "SIM0000001F"

    # Only the known order is in the map — external order is absent
    known_coid = ClientOrderId("O-KNOWN-001")
    stub._ts_order_id_to_client_order_id = {"TS-KNOWN-001": known_coid}

    # Cache returns None for every order (neither is in NT cache)
    stub._cache.order = MagicMock(return_value=None)

    # Logger — real MagicMock so we can inspect .debug call args
    stub._log = MagicMock()

    # _parse_order_status_report returns None (we don't need real reports here)
    stub._parse_order_status_report = MagicMock(return_value=None)

    # _parse_order_status is used by open_only filter — return a real enum value
    from nautilus_trader.model.enums import OrderStatus
    stub._parse_order_status = MagicMock(return_value=OrderStatus.SUBMITTED)

    return stub


def _shadow_log_calls(stub: MagicMock) -> list[str]:
    """Extract all debug call arg strings that contain '[RECON-SHADOW]'."""
    results = []
    for call in stub._log.debug.call_args_list:
        args, _ = call
        if args and "[RECON-SHADOW]" in str(args[0]):
            results.append(str(args[0]))
    return results


def _make_command(instrument_id=None, open_only: bool = False):
    """Build a minimal GenerateOrderStatusReports command stub."""
    cmd = MagicMock()
    cmd.instrument_id = instrument_id
    cmd.open_only = open_only
    return cmd


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestReconShadowLogging:
    """[RECON-SHADOW] log line emitted for external (unmapped+uncached) orders only."""

    @pytest.mark.asyncio
    async def test_shadow_log_emitted_for_external_order(self):
        """TS-EXTERNAL-999 is not in the map → [RECON-SHADOW] must be emitted."""
        orders = _load_orders_external()
        stub = _make_stub(orders)
        cmd = _make_command()

        await TradeStationExecutionClient.generate_order_status_reports(stub, cmd)

        shadow_calls = _shadow_log_calls(stub)
        assert len(shadow_calls) == 1, (
            f"Expected exactly 1 [RECON-SHADOW] call, got {len(shadow_calls)}. "
            f"All debug calls: {stub._log.debug.call_args_list}"
        )
        payload_str = shadow_calls[0]
        # The log payload must contain the external order's TS order ID
        assert "TS-EXTERNAL-999" in payload_str, (
            f"Expected 'TS-EXTERNAL-999' in shadow log payload: {payload_str}"
        )

    @pytest.mark.asyncio
    async def test_shadow_log_not_emitted_for_known_order(self):
        """TS-KNOWN-001 IS in the map → [RECON-SHADOW] must NOT be emitted for it."""
        orders = _load_orders_external()
        stub = _make_stub(orders)
        cmd = _make_command()

        await TradeStationExecutionClient.generate_order_status_reports(stub, cmd)

        shadow_calls = _shadow_log_calls(stub)
        for call_str in shadow_calls:
            assert "TS-KNOWN-001" not in call_str, (
                f"[RECON-SHADOW] must NOT mention the known order, but got: {call_str}"
            )

    @pytest.mark.asyncio
    async def test_shadow_log_payload_contains_required_fields(self):
        """The JSON payload must include event, ts_order_id, minted_coid, symbol, status."""
        orders = _load_orders_external()
        stub = _make_stub(orders)
        cmd = _make_command()

        await TradeStationExecutionClient.generate_order_status_reports(stub, cmd)

        shadow_calls = _shadow_log_calls(stub)
        assert shadow_calls, "Expected at least one [RECON-SHADOW] call"
        # Strip the "[RECON-SHADOW] " prefix and parse as JSON
        prefix = "[RECON-SHADOW] "
        payload_str = shadow_calls[0][shadow_calls[0].index(prefix) + len(prefix):]
        payload = json.loads(payload_str)

        assert payload["event"] == "adopted_unmapped_order"
        assert payload["ts_order_id"] == "TS-EXTERNAL-999"
        assert payload["minted_coid"] == "TS-TS-EXTERNAL-999"
        assert payload["symbol"] == "ESM26"
        assert payload["status"] == "FLL"

    @pytest.mark.asyncio
    async def test_shadow_log_emitted_before_open_only_filter(self):
        """[RECON-SHADOW] is emitted even when open_only=True would drop the FLL order.

        This is the critical placement test: the external order has Status=FLL,
        which is not in the open-only whitelist (ACCEPTED/SUBMITTED/PARTIALLY_FILLED).
        If the log were placed after the open_only filter it would silently miss
        exactly the highest-value gap case (externally-filled orders).
        """
        orders = _load_orders_external()
        stub = _make_stub(orders)
        # _parse_order_status must return FILLED for the FLL order so the filter drops it
        from nautilus_trader.model.enums import OrderStatus
        def _status_side_effect(status_str: str) -> OrderStatus:
            if status_str == "FLL":
                return OrderStatus.FILLED
            return OrderStatus.SUBMITTED
        stub._parse_order_status = MagicMock(side_effect=_status_side_effect)

        cmd = _make_command(open_only=True)

        await TradeStationExecutionClient.generate_order_status_reports(stub, cmd)

        shadow_calls = _shadow_log_calls(stub)
        assert len(shadow_calls) == 1, (
            f"[RECON-SHADOW] must fire before the open_only filter, "
            f"but got {len(shadow_calls)} calls. All debug: {stub._log.debug.call_args_list}"
        )
        assert "TS-EXTERNAL-999" in shadow_calls[0]

    @pytest.mark.asyncio
    async def test_check_order_statuses_not_modified(self):
        """Smoke-check: _check_order_statuses is not called during generate_order_status_reports.

        This confirms the fix is OBSERVE-ONLY and does not alter the safety-poll path.
        """
        orders = _load_orders_external()
        stub = _make_stub(orders)
        stub._check_order_statuses = AsyncMock()
        cmd = _make_command()

        await TradeStationExecutionClient.generate_order_status_reports(stub, cmd)

        stub._check_order_statuses.assert_not_called()
