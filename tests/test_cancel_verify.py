"""Post-cancel verification (§111 — JulyIssueFound QMU26 follow-up, 2026-07-13).

TS sim confirmed cancels for QMU26/RTYU26/CLQ26 (clean OrderCanceled in NT)
while the orders stayed alive on its own book, surfacing days later as true
orphans.  Any order reported cancelled is re-checked against the safety poll's
own order fetch; if still open >=2 min later the cancel is re-issued (max 3)."""
from __future__ import annotations

import time
from unittest.mock import AsyncMock, MagicMock

from tradestation_nt_community.execution import TradeStationExecutionClient


def _stub(pending: dict) -> MagicMock:
    stub = MagicMock()
    stub._cancel_verify_pending = pending
    stub._CANCEL_VERIFY_MIN_AGE_S = TradeStationExecutionClient._CANCEL_VERIFY_MIN_AGE_S
    stub._CANCEL_VERIFY_MAX_ATTEMPTS = TradeStationExecutionClient._CANCEL_VERIFY_MAX_ATTEMPTS
    stub._client = MagicMock()
    stub._client.cancel_order = AsyncMock()
    stub._log = MagicMock()
    return stub


def _open_order(oid="961146425", status="OPN"):
    return {"OrderID": oid, "Status": status}


class TestVerifyPendingCancels:
    async def test_still_open_after_min_age_reissues_cancel(self):
        old = time.monotonic() - 300
        stub = _stub({"961146425": (old, 0)})
        await TradeStationExecutionClient._verify_pending_cancels(stub, [_open_order()])
        stub._client.cancel_order.assert_awaited_once_with(order_id="961146425")
        assert stub._cancel_verify_pending["961146425"][1] == 1

    async def test_young_entry_not_checked_yet(self):
        stub = _stub({"961146425": (time.monotonic(), 0)})
        await TradeStationExecutionClient._verify_pending_cancels(stub, [_open_order()])
        stub._client.cancel_order.assert_not_awaited()

    async def test_gone_order_clears_entry(self):
        old = time.monotonic() - 300
        stub = _stub({"961146425": (old, 0)})
        await TradeStationExecutionClient._verify_pending_cancels(
            stub, [_open_order(status="CAN")])
        assert "961146425" not in stub._cancel_verify_pending
        stub._client.cancel_order.assert_not_awaited()

    async def test_gives_up_after_max_attempts(self):
        old = time.monotonic() - 300
        stub = _stub({"961146425": (old, 3)})
        await TradeStationExecutionClient._verify_pending_cancels(stub, [_open_order()])
        assert "961146425" not in stub._cancel_verify_pending
        stub._client.cancel_order.assert_not_awaited()
        stub._log.error.assert_called()      # loud give-up

    async def test_reissue_failure_is_swallowed_and_counted(self):
        old = time.monotonic() - 300
        stub = _stub({"961146425": (old, 1)})
        stub._client.cancel_order = AsyncMock(side_effect=Exception("boom"))
        await TradeStationExecutionClient._verify_pending_cancels(stub, [_open_order()])
        assert stub._cancel_verify_pending["961146425"][1] == 2
