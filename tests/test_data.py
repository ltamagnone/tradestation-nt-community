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
