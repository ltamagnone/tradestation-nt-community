import time
from unittest.mock import MagicMock

from nautilus_trader.model.identifiers import ClientOrderId

from tradestation_nt_community.execution import TradeStationExecutionClient as C


def _stub(watch):
    m = MagicMock()
    m._status_watch = watch
    m._STATUS_RECHECK_S = C._STATUS_RECHECK_S
    m._log = MagicMock()
    m._cache = MagicMock()
    m._cache.order.return_value = None
    return m


def _line(m):
    return m._log.warning.call_args.args[0]


def test_watch_status_records_once():
    m = _stub({})
    C._watch_status(m, "973736851", "O-1", "UCN")
    C._watch_status(m, "973736851", "O-1", "UCN")
    assert list(m._status_watch) == ["973736851"]
    assert m._status_watch["973736851"][1:] == ("UCN", "O-1")
    m._log.warning.assert_called_once()
    assert "[TS-STATUS-WATCH]" in _line(m)


def test_young_entry_waits():
    m = _stub({"9": (time.monotonic(), "UCN", "O-1")})
    C._recheck_watched_statuses(m, [{"OrderID": "9", "Status": "OPN"}])
    assert "9" in m._status_watch
    m._log.warning.assert_not_called()


def test_recheck_reports_still_live():
    m = _stub({"9": (time.monotonic() - 400, "UCN", "O-1")})
    C._recheck_watched_statuses(m, [{"OrderID": "9", "Status": "OPN"}])
    assert "9" not in m._status_watch
    line = _line(m)
    assert line.startswith("[TS-STATUS-RECHECK] TS order 9 (O-1, ?) got UCN unsolicited;")
    assert "broker status=OPN -> STILL LIVE - kept, NT state unchanged" in line


def test_recheck_reports_closed_when_now_terminal():
    m = _stub({"9": (time.monotonic() - 400, "UCN", "O-1")})
    C._recheck_watched_statuses(m, [{"OrderID": "9", "Status": "OUT"}])
    assert "broker status=OUT -> CLOSED at broker (canceled)" in _line(m)


def test_recheck_reports_not_found():
    m = _stub({"9": (time.monotonic() - 400, "UCN", "O-1")})
    C._recheck_watched_statuses(m, [])
    assert "broker status=- -> NOT FOUND in broker order list" in _line(m)
    assert "9" not in m._status_watch


def _stub_cached(watch, closed=False):
    m = _stub(watch)
    cached = MagicMock()
    cached.is_closed = closed
    cached.strategy_id = "S-001"
    cached.instrument_id = MagicMock()
    cached.instrument_id.symbol.value = "MNQZ26"
    m._cache.order.return_value = cached
    m._clock = MagicMock()
    m._clock.timestamp_ns.return_value = 123
    m._ts_order_id_to_client_order_id = {"9": ClientOrderId("O-1")}
    return m, cached


def test_recheck_canceled_at_broker_closes_open_nt_order():
    m, cached = _stub_cached({"9": (time.monotonic() - 400, "UCH", "O-1")})
    C._recheck_watched_statuses(m, [{"OrderID": "9", "Status": "OUT"}])
    m.generate_order_canceled.assert_called_once()
    kw = m.generate_order_canceled.call_args.kwargs
    assert kw["strategy_id"] == "S-001"
    assert kw["instrument_id"] is cached.instrument_id
    assert kw["ts_event"] == 123
    m._register_cancel_verify.assert_called_once_with("9", requested=False)
    line = _line(m)
    assert line.startswith("[TS-STATUS-RECHECK]")
    assert line.endswith("-> CLOSED at broker (canceled) - NT order closed")


def test_recheck_canceled_at_broker_nt_already_closed_no_event():
    m, _ = _stub_cached({"9": (time.monotonic() - 400, "UCH", "O-1")}, closed=True)
    C._recheck_watched_statuses(m, [{"OrderID": "9", "Status": "OUT"}])
    m.generate_order_canceled.assert_not_called()
    m._register_cancel_verify.assert_not_called()
    assert _line(m).endswith("-> CLOSED at broker (canceled)")


def test_recheck_still_live_emits_nothing():
    m, _ = _stub_cached({"9": (time.monotonic() - 400, "UCH", "O-1")})
    C._recheck_watched_statuses(m, [{"OrderID": "9", "Status": "OPN"}])
    m.generate_order_canceled.assert_not_called()
    m._register_cancel_verify.assert_not_called()


def test_recheck_missing_status_is_unknown_not_closed():
    m = _stub({"9": (time.monotonic() - 400, "UCN", "O-1")})
    C._recheck_watched_statuses(m, [{"OrderID": "9"}])
    assert "broker status=UNKNOWN -> no Status reported at broker" in _line(m)


import pytest


@pytest.mark.parametrize("idmap", [{"9": ClientOrderId("O-2")}, {}])
def test_recheck_remapped_id_does_not_close_nt(idmap):
    m, _ = _stub_cached({"9": (time.monotonic() - 400, "UCH", "O-1")})
    m._ts_order_id_to_client_order_id = idmap
    C._recheck_watched_statuses(m, [{"OrderID": "9", "Status": "OUT"}])
    m.generate_order_canceled.assert_not_called()
    m._register_cancel_verify.assert_not_called()
    assert _line(m).endswith("-> CLOSED at broker (canceled) - id remapped, NT order untouched")


def test_recheck_broker_still_held_code_does_not_close_nt():
    m, _ = _stub_cached({"9": (time.monotonic() - 400, "UCH", "O-1")})
    C._recheck_watched_statuses(m, [{"OrderID": "9", "Status": "UCH"}])
    m.generate_order_canceled.assert_not_called()
    m._register_cancel_verify.assert_not_called()
    assert _line(m).endswith(
        "-> CLOSED at broker (canceled) - still UCH, NT order kept open (held)")
