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
    stub._CANCEL_STUCK_ALERT_S = TradeStationExecutionClient._CANCEL_STUCK_ALERT_S
    stub._client = MagicMock()
    stub._client.cancel_order = AsyncMock()
    stub._log = MagicMock()
    return stub


def _open_order(oid="961146425", status="OPN"):
    return {"OrderID": oid, "Status": status}


class TestVerifyPendingCancels:
    async def test_still_open_after_min_age_reissues_cancel(self):
        old = time.monotonic() - 300
        stub = _stub({"961146425": (old, 0, True, False)})
        await TradeStationExecutionClient._verify_pending_cancels(stub, [_open_order()])
        stub._client.cancel_order.assert_awaited_once_with(order_id="961146425")
        assert stub._cancel_verify_pending["961146425"][1] == 1

    async def test_young_entry_not_checked_yet(self):
        stub = _stub({"961146425": (time.monotonic(), 0, True, False)})
        await TradeStationExecutionClient._verify_pending_cancels(stub, [_open_order()])
        stub._client.cancel_order.assert_not_awaited()

    async def test_gone_order_clears_entry(self):
        old = time.monotonic() - 300
        stub = _stub({"961146425": (old, 0, True, False)})
        await TradeStationExecutionClient._verify_pending_cancels(
            stub, [_open_order(status="CAN")])
        assert "961146425" not in stub._cancel_verify_pending
        stub._client.cancel_order.assert_not_awaited()

    async def test_gives_up_after_max_attempts(self):
        old = time.monotonic() - 300
        stub = _stub({"961146425": (old, 3, True, False)})
        await TradeStationExecutionClient._verify_pending_cancels(stub, [_open_order()])
        assert "961146425" not in stub._cancel_verify_pending
        stub._client.cancel_order.assert_not_awaited()
        stub._log.error.assert_called()      # loud give-up

    async def test_reissue_failure_is_swallowed_and_counted(self):
        old = time.monotonic() - 300
        stub = _stub({"961146425": (old, 1, True, False)})
        stub._client.cancel_order = AsyncMock(side_effect=Exception("boom"))
        await TradeStationExecutionClient._verify_pending_cancels(stub, [_open_order()])
        assert stub._cancel_verify_pending["961146425"][1] == 2


class TestUnrequestedCancelIsNeverReissued:
    """2026-10-08: TS UCN reported as cancelled -> §111 re-cancelled 8 live exits.
    An observed-but-unrequested cancel must never become a real cancel."""

    async def test_unrequested_still_open_is_logged_not_cancelled(self):
        old = time.monotonic() - 300
        stub = _stub({"973736851": (old, 0, False, False)})
        await TradeStationExecutionClient._verify_pending_cancels(stub, [_open_order("973736851")])
        stub._client.cancel_order.assert_not_awaited()
        assert "973736851" not in stub._cancel_verify_pending
        assert "[CANCEL-MISMATCH]" in stub._log.error.call_args.args[0]

    async def test_requested_still_open_is_reissued(self):
        old = time.monotonic() - 300
        stub = _stub({"961146425": (old, 0, True, False)})
        await TradeStationExecutionClient._verify_pending_cancels(stub, [_open_order()])
        stub._client.cancel_order.assert_awaited_once_with(order_id="961146425")

    async def test_ucn_on_our_cancel_waits(self):
        old = time.monotonic() - 300
        stub = _stub({"961146425": (old, 0, True, False)})
        await TradeStationExecutionClient._verify_pending_cancels(
            stub, [_open_order(status="UCN")])
        # UCN = our cancel still pending: not gone, but not re-cancelled either
        stub._client.cancel_order.assert_not_awaited()
        assert "961146425" in stub._cancel_verify_pending

    def test_register_keeps_requested_flag(self):
        stub = MagicMock()
        stub._cancel_verify_pending = {}
        TradeStationExecutionClient._register_cancel_verify(stub, "1", requested=False)
        assert stub._cancel_verify_pending["1"][1:] == (0, False, False)
        TradeStationExecutionClient._register_cancel_verify(stub, "1", requested=True)
        assert stub._cancel_verify_pending["1"][2] is True   # requested wins


class TestStatusAwareVerify:
    """R11: pin the §149 requested/unrequested x UCN/LAT/DON matrix."""

    async def _run(self, status, requested):
        old = time.monotonic() - 300
        stub = _stub({"961146425": (old, 0, requested, False)})
        await TradeStationExecutionClient._verify_pending_cancels(
            stub, [_open_order(status=status)])
        return stub

    async def test_unrequested_ucn_kept_silent(self):
        stub = await self._run("UCN", False)
        stub._client.cancel_order.assert_not_awaited()
        assert "961146425" in stub._cancel_verify_pending
        stub._log.error.assert_not_called()

    async def test_requested_lat_not_recancelled(self):
        stub = await self._run("LAT", True)
        stub._client.cancel_order.assert_not_awaited()

    async def test_requested_don_is_recancelled(self):
        stub = await self._run("DON", True)
        stub._client.cancel_order.assert_awaited_once_with(order_id="961146425")

    async def test_unrequested_don_is_mismatch_not_cancelled(self):
        stub = await self._run("DON", False)
        stub._client.cancel_order.assert_not_awaited()
        assert "[CANCEL-MISMATCH]" in stub._log.error.call_args.args[0]
        assert "961146425" not in stub._cancel_verify_pending


class TestRequestedCancelStuck:
    """A1 (§150): a cancel WE requested sitting at UCN/LAT must alert, once."""

    def _stuck_errors(self, stub):
        return [c.args[0] for c in stub._log.error.call_args_list
                if "[CANCEL-STUCK]" in c.args[0]]

    async def test_601s_alerts_once_without_cancel(self):
        stub = _stub({"1": (time.monotonic() - 601, 0, True, False)})
        orders = [_open_order("1", "UCN")]
        await TradeStationExecutionClient._verify_pending_cancels(stub, orders)
        errs = self._stuck_errors(stub)
        assert len(errs) == 1
        assert "Order 1 " in errs[0] and "UCN" in errs[0] and "§150" in errs[0]
        assert "cancel requested 10 min ago" in errs[0]    # 601 s -> "10" (D6: minutes figure)
        stub._client.cancel_order.assert_not_awaited()
        assert stub._cancel_verify_pending["1"][3] is True
        await TradeStationExecutionClient._verify_pending_cancels(stub, orders)
        assert len(self._stuck_errors(stub)) == 1
        stub._client.cancel_order.assert_not_awaited()

    async def test_lat_also_alerts(self):
        stub = _stub({"1": (time.monotonic() - 700, 0, True, False)})
        await TradeStationExecutionClient._verify_pending_cancels(
            stub, [_open_order("1", "LAT")])
        assert len(self._stuck_errors(stub)) == 1

    async def test_599s_no_alert(self):
        stub = _stub({"1": (time.monotonic() - 599, 0, True, False)})
        await TradeStationExecutionClient._verify_pending_cancels(
            stub, [_open_order("1", "UCN")])
        assert self._stuck_errors(stub) == []
        assert stub._cancel_verify_pending["1"][3] is False

    async def test_unrequested_ucn_no_stuck(self):
        stub = _stub({"1": (time.monotonic() - 900, 0, False, False)})
        await TradeStationExecutionClient._verify_pending_cancels(
            stub, [_open_order("1", "UCN")])
        assert self._stuck_errors(stub) == []

    async def test_leaving_live_deletes_entry(self):
        stub = _stub({"1": (time.monotonic() - 900, 0, True, True)})
        await TradeStationExecutionClient._verify_pending_cancels(
            stub, [_open_order("1", "CAN")])
        assert "1" not in stub._cancel_verify_pending
