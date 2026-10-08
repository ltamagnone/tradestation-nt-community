import time
from unittest.mock import MagicMock

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
