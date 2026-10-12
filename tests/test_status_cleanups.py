"""§150 status-handling cleanups: shared SSE/poll tail, watch-once, own-order guard."""
import logging
import time
from unittest.mock import MagicMock

import pytest
from nautilus_trader.model.identifiers import ClientOrderId, VenueOrderId

from tests.test_status_dispatch import _client, _poll, _sse
from tradestation_nt_community.execution import TradeStationExecutionClient as C


@pytest.mark.parametrize("deliver", [_sse, _poll])
async def test_cancel_pops_pending_modify_entry(deliver):
    m = _client()
    m._pending_modify_trigger_price = {ClientOrderId("O-EXIT"): 1.0}
    await deliver(m, "CAN")
    m.generate_order_canceled.assert_called_once()
    assert ClientOrderId("O-EXIT") not in m._pending_modify_trigger_price


@pytest.mark.parametrize("deliver", [_sse, _poll])
async def test_don_logs_session_pause_and_emits_nothing(deliver):
    m = _client()
    await deliver(m, "DON")
    assert any("session pause" in c.args[0] for c in m._log.info.call_args_list)
    m.generate_order_canceled.assert_not_called()
    m._watch_status.assert_not_called()


@pytest.mark.parametrize("deliver", [_sse, _poll])
@pytest.mark.parametrize("code,event", [("REJ", "generate_order_rejected"),
                                        ("CAN", "generate_order_canceled"),
                                        ("EXP", "generate_order_canceled")])
async def test_rej_can_exp_handled_directly_not_watched(deliver, code, event):
    m = _client()
    await deliver(m, code)
    getattr(m, event).assert_called_once()
    m._watch_status.assert_not_called()


@pytest.mark.parametrize("deliver", [_sse, _poll])
async def test_rjc_is_held_not_cancelled(deliver):
    # RJC = "Cancel Request Rejected": the order is still live.
    m = _client()
    await deliver(m, "RJC")
    m.generate_order_canceled.assert_not_called()
    m._watch_status.assert_called_once()


@pytest.mark.parametrize("deliver", [_sse, _poll])
async def test_csn_is_held_like_ucn(deliver):
    m = _client()
    await deliver(m, "CSN")
    m.generate_order_canceled.assert_not_called()
    m._watch_status.assert_called_once()


@pytest.mark.parametrize("deliver", [_sse, _poll])
@pytest.mark.parametrize("code", ["FLP", "XYZ"])
async def test_watch_warning_logged_exactly_once(deliver, code):
    m = _client()
    # use the real _watch_status against the stub's state
    m._STATUS_RECHECK_S = 300.0
    m._watch_status = lambda *a, **k: C._watch_status(m, *a, **k)
    await deliver(m, code)
    watch = [c for c in m._log.warning.call_args_list if "[TS-STATUS-WATCH]" in c.args[0]]
    assert len(watch) == 1
    # content, not just the count: the line must name the order, the client id and the code
    msg = watch[0].args[0]
    assert "TS order TS-1" in msg and f"got {code} we did not request" in msg
    assert "O-EXIT" in msg and "broker recheck in 300s" in msg


def _own_stub(cached_coid=None, order_in_cache=None):
    stub = MagicMock()
    stub._own_order_report_skip_logged = set()
    stub.__dict__["_own_order_report_skip_logged"] = set()
    stub._cache.order.side_effect = lambda c: order_in_cache if c == cached_coid else None
    stub._cache.client_order_id.return_value = cached_coid
    return stub


def test_own_guard_resolves_via_venue_order_id_when_coid_mapping_missing():
    coid = ClientOrderId("O-REAL")
    stub = _own_stub(coid, object())
    # caller minted TS-<id> because the ts->coid map had no entry
    assert C._is_own_cached_order(stub, ClientOrderId("TS-123"), "123") is True
    stub._cache.client_order_id.assert_called_once_with(VenueOrderId("123"))


def test_own_guard_false_for_truly_external_order():
    stub = _own_stub(None, None)
    assert C._is_own_cached_order(stub, ClientOrderId("TS-999"), "999") is False


def test_own_guard_unchanged_when_coid_cached():
    coid = ClientOrderId("O-1")
    stub = _own_stub(coid, object())
    assert C._is_own_cached_order(stub, coid, "1") is True
    stub._cache.client_order_id.assert_not_called()


def test_unmapped_duration_debug_line_does_not_raise(caplog):
    from nautilus_trader.model.enums import TimeInForce
    from tradestation_nt_community.parsing.execution import parse_time_in_force
    with caplog.at_level(logging.DEBUG):
        assert parse_time_in_force({"OrderID": "7", "Duration": "WEIRD"}) == TimeInForce.DAY
    assert any("Unmapped TS Duration" in r.getMessage() for r in caplog.records)
