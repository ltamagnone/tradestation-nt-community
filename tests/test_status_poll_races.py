"""§261c A-T4: poll/stream/REST races stay idempotent at a 30 s poll cadence, and the
[STATUS-POLL] observe line is throttled and never raises."""
from unittest.mock import MagicMock

from tradestation_nt_community.execution import (TradeStationExecutionClient,
                                                  _claim_cancel_emit)
from tests.test_status_dispatch import _client, _poll


class TestStalePollSnapshot:
    async def test_stale_open_after_stream_fill_emits_nothing_then_fll_no_second_fill(self):
        m = _client(closed=True)              # stream FLL already applied: NT order closed
        m._order_last_status["TS-1"] = "FLL"
        await _poll(m, "OPN")                 # stale snapshot taken before the fill
        for name in ("generate_order_filled", "generate_order_canceled",
                     "generate_order_rejected", "generate_order_accepted"):
            getattr(m, name).assert_not_called()
        await _poll(m, "FLL")                 # next poll sees the fill again
        m.generate_order_filled.assert_not_called()


class TestPollCancelVsRestCancel:
    async def test_rest_first_then_poll_can_emits_nothing_more(self):
        m = _client()
        m._emitted_cancel_coids = {}
        assert _claim_cancel_emit(m, m._ts_order_id_to_client_order_id["TS-1"])  # REST path
        await _poll(m, "CAN")
        m.generate_order_canceled.assert_not_called()

    async def test_poll_first_then_rest_claim_is_refused(self):
        m = _client()
        m._emitted_cancel_coids = {}
        await _poll(m, "CAN")
        assert m.generate_order_canceled.call_count == 1
        assert not _claim_cancel_emit(m, m._ts_order_id_to_client_order_id["TS-1"])


class TestStatusPollLine:
    def _self(self):
        s = MagicMock()
        s._last_status_poll_log_s = 0.0
        s._STATUS_POLL_LOG_EVERY_S = TradeStationExecutionClient._STATUS_POLL_LOG_EVERY_S
        s._status_safety_poll_secs = 30.0
        return s

    def test_once_per_ten_minutes(self, monkeypatch):
        import tradestation_nt_community.execution as ex
        s = self._self()
        t = [1000.0]
        monkeypatch.setattr(ex, "_mono", lambda: t[0])
        for dt in (0, 30, 300, 599):
            t[0] = 1000.0 + dt
            TradeStationExecutionClient._log_status_poll(s, 100, 80.0)
        assert s._log.info.call_count == 1
        t[0] = 1000.0 + 601
        TradeStationExecutionClient._log_status_poll(s, 100, 80.0)
        assert s._log.info.call_count == 2
        assert "[STATUS-POLL] orders=100 read_ms=80" in s._log.info.call_args.args[0]

    def test_never_raises(self):
        s = self._self()
        s._log.info.side_effect = RuntimeError("log broke")
        TradeStationExecutionClient._log_status_poll(s, 1, 1.0)
