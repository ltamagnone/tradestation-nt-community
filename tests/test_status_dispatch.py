"""Adapter acts only on statuses the TS spec calls terminal (2026-10-08 UCN finding)."""
from unittest.mock import AsyncMock, MagicMock

import pytest
from nautilus_trader.model.enums import OrderStatus, TimeInForce
from nautilus_trader.model.identifiers import (ClientOrderId, InstrumentId, StrategyId,
                                                Symbol, Venue)

from tradestation_nt_community.execution import TradeStationExecutionClient


def _client(tif=TimeInForce.GTC, closed=False):
    cached = MagicMock()
    cached.is_closed = closed
    cached.status = OrderStatus.CANCELED if closed else OrderStatus.ACCEPTED
    cached.time_in_force = tif
    cached.strategy_id = StrategyId("S-001")
    cached.instrument_id = InstrumentId(Symbol("MNQZ26"), Venue("TRADESTATION"))
    m = MagicMock()
    m._ts_order_id_to_client_order_id = {"TS-1": ClientOrderId("O-EXIT")}
    m._order_last_status = {}
    m._pending_modify_trigger_price = {}
    m._cancel_verify_pending = {}
    m._status_watch = {}
    m._verify_pending_cancels = AsyncMock()
    m._recheck_watched_statuses = MagicMock()
    m._cache = MagicMock()
    m._cache.order.return_value = cached
    m._clock = MagicMock()
    m._clock.timestamp_ns.return_value = 0
    m._log = MagicMock()
    return m


async def _sse(m, status):
    await TradeStationExecutionClient._process_order_event(m, {"OrderID": "TS-1", "Status": status})


async def _poll(m, status):
    m._client = MagicMock()
    m._client.get_orders = AsyncMock(return_value=[{"OrderID": "TS-1", "Status": status}])
    await TradeStationExecutionClient._check_order_statuses(m)


@pytest.mark.parametrize("deliver", [_sse, _poll])
class TestDispatch:
    async def test_ucn_is_held_and_watched(self, deliver):
        m = _client()
        await deliver(m, "UCN")
        m.generate_order_canceled.assert_not_called()
        m._register_cancel_verify.assert_not_called()
        m._watch_status.assert_called_once()
        assert m._watch_status.call_args.args[0] == "TS-1"

    async def test_lat_and_bro_are_not_rejections(self, deliver):
        for st in ("LAT", "BRO"):
            m = _client()
            await deliver(m, st)
            m.generate_order_rejected.assert_not_called()
            m._watch_status.assert_called_once()

    async def test_don_on_day_order_is_a_pause(self, deliver):
        m = _client(tif=TimeInForce.DAY)
        await deliver(m, "DON")
        m.generate_order_canceled.assert_not_called()
        m._watch_status.assert_not_called()

    async def test_out_cancels_unrequested(self, deliver):
        m = _client()
        await deliver(m, "OUT")
        m.generate_order_canceled.assert_called_once()
        m._register_cancel_verify.assert_called_once_with("TS-1", requested=False)
        m._watch_status.assert_not_called()

    async def test_tsc_cancels_and_is_watched(self, deliver):
        m = _client()
        await deliver(m, "TSC")
        m.generate_order_canceled.assert_called_once()
        m._watch_status.assert_called_once()

    async def test_rej_rejects(self, deliver):
        m = _client()
        await deliver(m, "REJ")
        m.generate_order_rejected.assert_called_once()

    async def test_unknown_status_is_held_and_watched(self, deliver):
        m = _client()
        await deliver(m, "XYZ")
        m.generate_order_canceled.assert_not_called()
        m.generate_order_rejected.assert_not_called()
        m._watch_status.assert_called_once()
        m._log.warning.assert_called()

    async def test_flp_is_held_and_watched(self, deliver):
        # R6: FLP is in _st.FILLED but is not FLL; the adapter does not handle it.
        m = _client()
        await deliver(m, "FLP")
        m.generate_order_canceled.assert_not_called()
        m.generate_order_rejected.assert_not_called()
        m.generate_order_filled.assert_not_called()
        m._watch_status.assert_called_once()
        assert "[TS-STATUS-WATCH]" in m._log.warning.call_args.args[0]
        assert "Partial Fill UROut" in m._log.warning.call_args.args[0]

    async def test_ucn_on_already_closed_order_is_ignored(self, deliver):
        # Our own cancel: _cancel_order already emitted OrderCanceled, so NT has
        # the order closed when the UCN for it arrives.
        m = _client(closed=True)
        await deliver(m, "UCN")
        m._watch_status.assert_not_called()
        m.generate_order_canceled.assert_not_called()


@pytest.mark.parametrize("deliver", [_sse, _poll])
async def test_fill_on_cancelled_order_is_loud(deliver):
    m = _client(closed=True)  # NT: CANCELED
    await deliver(m, "FLL")
    m.generate_order_filled.assert_not_called()  # observe only
    msg = m._log.error.call_args.args[0]
    assert "[FILL-AFTER-CANCEL] TS order TS-1 (O-EXIT, MNQZ26) filled at the broker" in msg
