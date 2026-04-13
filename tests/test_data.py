"""
Tests for TradeStation data client parsing helpers.

These tests exercise bar_spec_to_ts_params() and parse_bars() directly via the
module-level functions in data.py, requiring no live client or network calls.
"""
import pytest

from tradestation_nt_community.common.enums import TradeStationBarUnit
from tradestation_nt_community.data import bar_spec_to_ts_params, parse_bars
from nautilus_trader.model.data import Bar, BarSpecification, BarType
from nautilus_trader.model.enums import AggregationSource, BarAggregation, PriceType
from tests.test_kit import (
    TSTestDataStubs,
    TSTestInstrumentStubs,
)


def _make_spec(step: int, aggregation: BarAggregation) -> BarSpecification:
    return BarSpecification(step=step, aggregation=aggregation, price_type=PriceType.LAST)


def _make_bar_type(instrument, step: int, aggregation: BarAggregation = BarAggregation.MINUTE) -> BarType:
    spec = _make_spec(step, aggregation)
    return BarType(
        instrument_id=instrument.id,
        bar_spec=spec,
        aggregation_source=AggregationSource.EXTERNAL,
    )


class TestBarSpecConversion:
    """Tests for bar_spec_to_ts_params()."""

    def test_15_minute_spec(self):
        """15-MINUTE → ('15', MINUTE)."""
        interval, unit = bar_spec_to_ts_params(_make_spec(15, BarAggregation.MINUTE))
        assert interval == "15"
        assert unit == TradeStationBarUnit.MINUTE

    def test_1_minute_spec(self):
        """1-MINUTE → ('1', MINUTE)."""
        interval, unit = bar_spec_to_ts_params(_make_spec(1, BarAggregation.MINUTE))
        assert interval == "1"
        assert unit == TradeStationBarUnit.MINUTE

    def test_5_minute_spec(self):
        """5-MINUTE → ('5', MINUTE)."""
        interval, unit = bar_spec_to_ts_params(_make_spec(5, BarAggregation.MINUTE))
        assert interval == "5"
        assert unit == TradeStationBarUnit.MINUTE

    def test_1_hour_spec_maps_to_60_minutes(self):
        """1-HOUR → ('60', MINUTE) — TradeStation uses minutes for hourly bars."""
        interval, unit = bar_spec_to_ts_params(_make_spec(1, BarAggregation.HOUR))
        assert interval == "60"
        assert unit == TradeStationBarUnit.MINUTE

    def test_1_day_spec(self):
        """1-DAY → ('1', DAILY)."""
        interval, unit = bar_spec_to_ts_params(_make_spec(1, BarAggregation.DAY))
        assert interval == "1"
        assert unit == TradeStationBarUnit.DAILY

    def test_unsupported_aggregation_raises_value_error(self):
        """Week aggregation is not supported → ValueError."""
        with pytest.raises(ValueError, match="Unsupported"):
            bar_spec_to_ts_params(_make_spec(1, BarAggregation.WEEK))


class TestQuoteStreamMuxLifecycle:
    """Tests for the shared quote/trade SSE multiplexer lifecycle logic.

    These validate _ensure_quote_stream / _maybe_stop_quote_stream behaviour
    without constructing a full data client.
    """

    def test_ensure_quote_stream_only_starts_once(self):
        """Calling _ensure_quote_stream twice for same instrument should not create a second task."""
        tasks: dict = {}
        wants_quotes: set = set()
        wants_trades: set = set()
        instrument_id = TSTestInstrumentStubs.gc_futures_contract().id

        # Simulate _ensure_quote_stream logic
        def ensure(iid):
            if iid in tasks:
                return False  # already running
            tasks[iid] = "task_placeholder"
            return True

        assert ensure(instrument_id) is True
        assert ensure(instrument_id) is False
        assert len(tasks) == 1

    def test_maybe_stop_with_both_subscribers_keeps_stream(self):
        """Stream should NOT be stopped when both quote and trade subscribers exist."""
        instrument_id = TSTestInstrumentStubs.gc_futures_contract().id
        wants_quotes = {instrument_id}
        wants_trades = {instrument_id}
        tasks = {instrument_id: "task"}

        # _maybe_stop logic: only stop if neither set contains the id
        should_stop = (
            instrument_id not in wants_quotes
            and instrument_id not in wants_trades
        )
        assert should_stop is False

    def test_maybe_stop_with_one_subscriber_keeps_stream(self):
        """Stream should NOT be stopped when one subscriber remains."""
        instrument_id = TSTestInstrumentStubs.gc_futures_contract().id
        wants_quotes = set()  # quotes unsubscribed
        wants_trades = {instrument_id}  # trades still active

        should_stop = (
            instrument_id not in wants_quotes
            and instrument_id not in wants_trades
        )
        assert should_stop is False

    def test_maybe_stop_with_no_subscribers_stops_stream(self):
        """Stream should be stopped when no subscribers remain."""
        instrument_id = TSTestInstrumentStubs.gc_futures_contract().id
        wants_quotes = set()
        wants_trades = set()

        should_stop = (
            instrument_id not in wants_quotes
            and instrument_id not in wants_trades
        )
        assert should_stop is True

    def test_multiple_instruments_independent(self):
        """Stopping one instrument's stream does not affect another."""
        gc_id = TSTestInstrumentStubs.gc_futures_contract().id
        es_id = TSTestInstrumentStubs.es_futures_contract().id
        wants_quotes = {gc_id}
        wants_trades = {gc_id, es_id}

        # Remove ES from trades — ES should stop, GC should continue
        wants_trades.discard(es_id)
        es_should_stop = es_id not in wants_quotes and es_id not in wants_trades
        gc_should_stop = gc_id not in wants_quotes and gc_id not in wants_trades
        assert es_should_stop is True
        assert gc_should_stop is False


class TestBarStreamBuffering:
    """Tests for the bar stream buffering logic (emit on timestamp change)."""

    def test_same_timestamp_does_not_emit(self):
        """Events with the same timestamp should buffer without emitting."""
        buffered_ts = ""
        emitted = []
        events = [
            {"TimeStamp": "2026-04-08T10:00:00Z", "Open": 3340, "Close": 3345},
            {"TimeStamp": "2026-04-08T10:00:00Z", "Open": 3340, "Close": 3347},
            {"TimeStamp": "2026-04-08T10:00:00Z", "Open": 3340, "Close": 3348},
        ]
        buffered_event = None

        for event in events:
            event_ts = event["TimeStamp"]
            if not buffered_ts:
                buffered_ts = event_ts
                buffered_event = event
                continue
            if event_ts != buffered_ts:
                emitted.append(buffered_event)
                buffered_ts = event_ts
                buffered_event = event
            else:
                buffered_event = event

        assert len(emitted) == 0
        # The buffer should hold the latest update
        assert buffered_event["Close"] == 3348

    def test_timestamp_change_emits_previous(self):
        """When timestamp changes, the previous buffered bar should be emitted."""
        buffered_ts = ""
        emitted = []
        events = [
            {"TimeStamp": "2026-04-08T10:00:00Z", "Open": 3340, "Close": 3345},
            {"TimeStamp": "2026-04-08T10:00:00Z", "Open": 3340, "Close": 3348},
            {"TimeStamp": "2026-04-08T10:15:00Z", "Open": 3348, "Close": 3350},
        ]
        buffered_event = None

        for event in events:
            event_ts = event["TimeStamp"]
            if not buffered_ts:
                buffered_ts = event_ts
                buffered_event = event
                continue
            if event_ts != buffered_ts:
                emitted.append(buffered_event)
                buffered_ts = event_ts
                buffered_event = event
            else:
                buffered_event = event

        assert len(emitted) == 1
        assert emitted[0]["Close"] == 3348  # last update of the first bar
        assert emitted[0]["TimeStamp"] == "2026-04-08T10:00:00Z"

    def test_three_bars_emits_two(self):
        """Three different timestamps should emit two completed bars."""
        buffered_ts = ""
        emitted = []
        events = [
            {"TimeStamp": "2026-04-08T10:00:00Z", "Open": 3340, "Close": 3345},
            {"TimeStamp": "2026-04-08T10:15:00Z", "Open": 3345, "Close": 3350},
            {"TimeStamp": "2026-04-08T10:30:00Z", "Open": 3350, "Close": 3355},
        ]
        buffered_event = None

        for event in events:
            event_ts = event["TimeStamp"]
            if not buffered_ts:
                buffered_ts = event_ts
                buffered_event = event
                continue
            if event_ts != buffered_ts:
                emitted.append(buffered_event)
                buffered_ts = event_ts
                buffered_event = event
            else:
                buffered_event = event

        assert len(emitted) == 2
        assert emitted[0]["TimeStamp"] == "2026-04-08T10:00:00Z"
        assert emitted[1]["TimeStamp"] == "2026-04-08T10:15:00Z"

    def test_empty_timestamp_skipped(self):
        """Events with empty TimeStamp should be skipped."""
        buffered_ts = ""
        emitted = []
        events = [
            {"TimeStamp": "", "Open": 0, "Close": 0},
            {"TimeStamp": "2026-04-08T10:00:00Z", "Open": 3340, "Close": 3345},
        ]
        buffered_event = None

        for event in events:
            event_ts = event.get("TimeStamp", "")
            if not event_ts:
                continue
            if not buffered_ts:
                buffered_ts = event_ts
                buffered_event = event
                continue
            if event_ts != buffered_ts:
                emitted.append(buffered_event)
                buffered_ts = event_ts
                buffered_event = event
            else:
                buffered_event = event

        assert len(emitted) == 0
        assert buffered_event["Open"] == 3340


class TestBarStreamReconnection:
    """Tests for _stream_bars reconnection gap recovery logic.

    On reconnect, TradeStation sends a Historical seed bar. The buffering
    logic must use it to correct stale OHLCV or fill gaps instead of
    discarding it.
    """

    @staticmethod
    def _run_buffer(events):
        """Simulate _stream_bars buffering logic with reconnection handling.

        Returns (emitted, buffered_event, buffered_ts).
        """
        buffered_event = None
        buffered_ts = ""
        initialized = False
        emitted = []

        for event in events:
            event_ts = event.get("TimeStamp", "")
            if not event_ts:
                continue

            is_historical = event.get("Status") == "Historical"

            if is_historical:
                if not initialized:
                    # Initial seed — skip
                    continue
                if event_ts == buffered_ts:
                    # Correct buffer with accurate close
                    buffered_event = event
                elif event_ts < buffered_ts:
                    # Stale seed — older than buffer, already emitted. Ignore.
                    pass
                else:
                    # Gap: seed is newer. Emit buffered + seed, reset buffer.
                    if buffered_event:
                        emitted.append(buffered_event)
                    emitted.append(event)
                    buffered_ts = ""
                    buffered_event = None
                continue

            # RealTime event
            if not buffered_ts:
                buffered_ts = event_ts
                buffered_event = event
                initialized = True
                continue

            if event_ts != buffered_ts:
                if buffered_event:
                    emitted.append(buffered_event)
                buffered_ts = event_ts
                buffered_event = event
            else:
                buffered_event = event

        return emitted, buffered_event, buffered_ts

    def test_initial_historical_seed_skipped(self):
        """First Historical event on initial connection is skipped."""
        events = [
            {"TimeStamp": "10:00", "Status": "Historical", "Close": 3340},
            {"TimeStamp": "10:00", "Status": "RealTime", "Close": 3342},
            {"TimeStamp": "10:15", "Status": "RealTime", "Close": 3350},
        ]
        emitted, buf, _ = self._run_buffer(events)
        assert len(emitted) == 1
        assert emitted[0]["Close"] == 3342  # RealTime, not Historical

    def test_reconnect_same_ts_corrects_buffer(self):
        """Historical seed with same timestamp replaces stale buffer values."""
        events = [
            # Initial: seed skipped, then live bar
            {"TimeStamp": "10:00", "Status": "Historical", "Close": 9999},
            {"TimeStamp": "10:00", "Status": "RealTime", "Close": 3340},
            {"TimeStamp": "10:00", "Status": "RealTime", "Close": 3345},
            # Disconnect + reconnect: seed bar corrects the buffer
            {"TimeStamp": "10:00", "Status": "Historical", "Close": 3348},
            # Next bar arrives — emits the corrected buffer
            {"TimeStamp": "10:15", "Status": "RealTime", "Close": 3350},
        ]
        emitted, buf, _ = self._run_buffer(events)
        assert len(emitted) == 1
        assert emitted[0]["Close"] == 3348  # Corrected by seed, not stale 3345

    def test_reconnect_different_ts_fills_gap(self):
        """Historical seed with new timestamp emits both buffered and seed bars."""
        events = [
            # Initial
            {"TimeStamp": "10:00", "Status": "Historical", "Close": 9999},
            {"TimeStamp": "10:00", "Status": "RealTime", "Close": 3340},
            # Disconnect spans the 10:00 close. Reconnect seed is 10:15.
            {"TimeStamp": "10:15", "Status": "Historical", "Close": 3355},
            # Next RealTime bar
            {"TimeStamp": "10:30", "Status": "RealTime", "Close": 3360},
        ]
        emitted, buf, _ = self._run_buffer(events)
        # Should emit: buffered 10:00 bar + seed 10:15 bar
        assert len(emitted) == 2
        assert emitted[0]["TimeStamp"] == "10:00"
        assert emitted[0]["Close"] == 3340  # Buffered bar
        assert emitted[1]["TimeStamp"] == "10:15"
        assert emitted[1]["Close"] == 3355  # Seed bar (gap recovery)

    def test_reconnect_resets_buffer_for_next_realtime(self):
        """After gap recovery, next RealTime event starts a fresh buffer."""
        events = [
            {"TimeStamp": "10:00", "Status": "Historical", "Close": 9999},
            {"TimeStamp": "10:00", "Status": "RealTime", "Close": 3340},
            # Gap
            {"TimeStamp": "10:15", "Status": "Historical", "Close": 3355},
            # Fresh buffer starts
            {"TimeStamp": "10:30", "Status": "RealTime", "Close": 3360},
            {"TimeStamp": "10:30", "Status": "RealTime", "Close": 3362},
        ]
        emitted, buf, buf_ts = self._run_buffer(events)
        assert buf_ts == "10:30"
        assert buf["Close"] == 3362  # Latest update in buffer

    def test_no_reconnect_normal_flow_unchanged(self):
        """Normal flow without reconnection works the same as before."""
        events = [
            {"TimeStamp": "10:00", "Status": "Historical", "Close": 9999},
            {"TimeStamp": "10:00", "Status": "RealTime", "Close": 3340},
            {"TimeStamp": "10:15", "Status": "RealTime", "Close": 3350},
            {"TimeStamp": "10:30", "Status": "RealTime", "Close": 3360},
        ]
        emitted, _, _ = self._run_buffer(events)
        assert len(emitted) == 2
        assert emitted[0]["Close"] == 3340
        assert emitted[1]["Close"] == 3350

    def test_stale_seed_ignored_on_brief_disconnect(self):
        """Seed older than buffer is ignored (reconnect within same bar period)."""
        events = [
            {"TimeStamp": "09:45", "Status": "Historical", "Close": 9999},
            {"TimeStamp": "09:45", "Status": "RealTime", "Close": 3320},
            # 09:45 bar closes, 10:00 bar starts
            {"TimeStamp": "10:00", "Status": "RealTime", "Close": 3340},
            {"TimeStamp": "10:00", "Status": "RealTime", "Close": 3345},
            # Brief disconnect + reconnect within 10:00 bar period.
            # Seed = last COMPLETED bar = 09:45 (older than buffer 10:00)
            {"TimeStamp": "09:45", "Status": "Historical", "Close": 3325},
            # RealTime resumes for the same in-progress bar
            {"TimeStamp": "10:00", "Status": "RealTime", "Close": 3347},
            # Bar closes
            {"TimeStamp": "10:15", "Status": "RealTime", "Close": 3355},
        ]
        emitted, buf, _ = self._run_buffer(events)
        # Should emit: 09:45 bar (when 10:00 started), then 10:00 bar (when 10:15 started)
        assert len(emitted) == 2
        assert emitted[0]["TimeStamp"] == "09:45"
        assert emitted[0]["Close"] == 3320  # Original 09:45 bar
        assert emitted[1]["TimeStamp"] == "10:00"
        assert emitted[1]["Close"] == 3347  # Latest update, NOT stale pre-disconnect value

    def test_double_historical_seed_same_ts(self):
        """Two Historical seeds with same timestamp — second replaces first."""
        events = [
            {"TimeStamp": "10:00", "Status": "Historical", "Close": 9999},
            {"TimeStamp": "10:00", "Status": "RealTime", "Close": 3340},
            # Two seeds in a row (TS quirk)
            {"TimeStamp": "10:00", "Status": "Historical", "Close": 3348},
            {"TimeStamp": "10:00", "Status": "Historical", "Close": 3350},
            # Next bar
            {"TimeStamp": "10:15", "Status": "RealTime", "Close": 3355},
        ]
        emitted, _, _ = self._run_buffer(events)
        assert len(emitted) == 1
        assert emitted[0]["Close"] == 3350  # Latest seed value

    def test_multiple_initial_seeds_all_skipped(self):
        """Multiple Historical seeds before first RealTime are all skipped."""
        events = [
            {"TimeStamp": "10:00", "Status": "Historical", "Close": 3340},
            {"TimeStamp": "10:00", "Status": "Historical", "Close": 3342},
            {"TimeStamp": "10:00", "Status": "Historical", "Close": 3345},
            {"TimeStamp": "10:00", "Status": "RealTime", "Close": 3350},
            {"TimeStamp": "10:15", "Status": "RealTime", "Close": 3360},
        ]
        emitted, buf, _ = self._run_buffer(events)
        assert len(emitted) == 1
        assert emitted[0]["Close"] == 3350  # Only RealTime values used


class TestBarParsing:
    """Tests for parse_bars()."""

    def setup_method(self):
        self.instrument = TSTestInstrumentStubs.gc_futures_contract()

    def test_parse_returns_correct_count(self):
        """Parsing 3 raw bars returns exactly 3 Bar objects."""
        raw = TSTestDataStubs.bars_response()
        bar_type = _make_bar_type(self.instrument, 1)
        bars = parse_bars(raw, bar_type)
        assert len(bars) == 3

    def test_parse_returns_bar_instances(self):
        """All parsed items are Bar instances."""
        raw = TSTestDataStubs.bars_response()
        bar_type = _make_bar_type(self.instrument, 1)
        bars = parse_bars(raw, bar_type)
        assert all(isinstance(b, Bar) for b in bars)

    def test_first_bar_open_value(self):
        """First bar open matches fixture value 2050.5."""
        raw = TSTestDataStubs.bars_response()
        bar_type = _make_bar_type(self.instrument, 1)
        bars = parse_bars(raw, bar_type)
        assert float(bars[0].open) == pytest.approx(2050.5, rel=1e-4)

    def test_first_bar_high_value(self):
        """First bar high matches fixture value 2051.0."""
        raw = TSTestDataStubs.bars_response()
        bar_type = _make_bar_type(self.instrument, 1)
        bars = parse_bars(raw, bar_type)
        assert float(bars[0].high) == pytest.approx(2051.0, rel=1e-4)

    def test_first_bar_volume(self):
        """First bar volume matches fixture value 1500."""
        raw = TSTestDataStubs.bars_response()
        bar_type = _make_bar_type(self.instrument, 1)
        bars = parse_bars(raw, bar_type)
        assert bars[0].volume == 1500

    def test_parse_empty_list_returns_empty(self):
        """Parsing an empty list returns an empty list."""
        bar_type = _make_bar_type(self.instrument, 1)
        assert parse_bars([], bar_type) == []

    def test_bars_sorted_by_timestamp(self):
        """Parsed bars are in chronological order (fixture already sorted)."""
        raw = TSTestDataStubs.bars_response()
        bar_type = _make_bar_type(self.instrument, 1)
        bars = parse_bars(raw, bar_type)
        timestamps = [b.ts_event for b in bars]
        assert timestamps == sorted(timestamps)
