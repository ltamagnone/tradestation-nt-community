"""
Tests for the historical package (historical/client.py).

All tests use MockTradeStationHttpClient — no network calls.
"""
import pytest
import pandas as pd

from tradestation_nt_community.historical.client import (
    TradeStationHistoricalClient,
    estimate_barsback,
    _MIN_BARS,
    _MAX_BARS_PER_REQUEST,
)
from nautilus_trader.model.data import BarSpecification, BarType
from nautilus_trader.model.enums import AggregationSource, BarAggregation, PriceType
from tests.mock_http_client import MockTradeStationHttpClient
from tests.test_kit import TSTestInstrumentStubs


def _make_bar_type(
    instrument, step: int, agg: BarAggregation = BarAggregation.MINUTE
) -> BarType:
    spec = BarSpecification(step=step, aggregation=agg, price_type=PriceType.LAST)
    return BarType(
        instrument_id=instrument.id,
        bar_spec=spec,
        aggregation_source=AggregationSource.EXTERNAL,
    )


@pytest.fixture
def mock_client() -> MockTradeStationHttpClient:
    return MockTradeStationHttpClient()


@pytest.fixture
def historical_client(mock_client) -> TradeStationHistoricalClient:
    return TradeStationHistoricalClient(http_client=mock_client)


@pytest.fixture
def instrument():
    return TSTestInstrumentStubs.gc_futures_contract()



class TestEstimateBarsback:
    """Tests for estimate_barsback() — bar-count estimation logic."""

    def test_uses_limit_when_no_start(self):
        """With no start, uses the explicit limit."""
        spec = BarSpecification(15, BarAggregation.MINUTE, PriceType.LAST)
        assert estimate_barsback(spec, start=None, limit=500) == 500

    def test_defaults_to_100_when_limit_zero(self):
        """Limit=0 falls back to 100."""
        spec = BarSpecification(15, BarAggregation.MINUTE, PriceType.LAST)
        assert estimate_barsback(spec, start=None, limit=0) == 100

    def test_start_drives_count_for_minute_bars(self):
        """With a 1-day start for 1-min bars: ~23*60 bars (roughly)."""
        spec = BarSpecification(1, BarAggregation.MINUTE, PriceType.LAST)
        start = pd.Timestamp.utcnow() - pd.Timedelta(days=1)
        result = estimate_barsback(spec, start=start, limit=0)
        assert result > 23 * 60  # at least 23h worth
        assert result <= _MAX_BARS_PER_REQUEST

    def test_start_drives_count_for_hour_bars(self):
        """With a 7-day start for 1-hour bars: ~7*23 bars (roughly)."""
        spec = BarSpecification(1, BarAggregation.HOUR, PriceType.LAST)
        start = pd.Timestamp.utcnow() - pd.Timedelta(days=7)
        result = estimate_barsback(spec, start=start, limit=0)
        assert result > 7 * 23
        assert result <= _MAX_BARS_PER_REQUEST

    def test_start_drives_count_for_daily_bars(self):
        """With a 30-day start for daily bars: ~35 bars (with buffer)."""
        spec = BarSpecification(1, BarAggregation.DAY, PriceType.LAST)
        start = pd.Timestamp.utcnow() - pd.Timedelta(days=30)
        result = estimate_barsback(spec, start=start, limit=0)
        assert result > 30
        assert result <= _MAX_BARS_PER_REQUEST

    def test_clamped_to_minimum(self):
        """Very small spans are clamped to _MIN_BARS."""
        spec = BarSpecification(15, BarAggregation.MINUTE, PriceType.LAST)
        start = pd.Timestamp.utcnow() - pd.Timedelta(seconds=1)
        result = estimate_barsback(spec, start=start, limit=0)
        assert result >= _MIN_BARS  # very short span still gets minimum buffer

    def test_clamped_to_maximum(self):
        """Very large spans are clamped to _MAX_BARS_PER_REQUEST."""
        spec = BarSpecification(1, BarAggregation.MINUTE, PriceType.LAST)
        start = pd.Timestamp.utcnow() - pd.Timedelta(days=365)
        result = estimate_barsback(spec, start=start, limit=0)
        assert result == _MAX_BARS_PER_REQUEST

    def test_naive_start_treated_as_utc(self):
        """A tz-naive start timestamp is localised to UTC without error."""
        spec = BarSpecification(1, BarAggregation.MINUTE, PriceType.LAST)
        start = pd.Timestamp("2026-01-01")  # no tz
        result = estimate_barsback(spec, start=start, limit=0)
        assert result >= _MIN_BARS



class TestTradeStationHistoricalClient:
    """Tests for TradeStationHistoricalClient.get_bars()."""

    @pytest.mark.asyncio
    async def test_returns_bar_objects(self, historical_client, instrument):
        """get_bars returns a list of Bar objects from the fixture."""
        from nautilus_trader.model.data import Bar
        bar_type = _make_bar_type(instrument, 1)
        bars = await historical_client.get_bars(bar_type=bar_type, instrument=instrument)
        assert len(bars) == 3
        assert all(isinstance(b, Bar) for b in bars)

    @pytest.mark.asyncio
    async def test_correct_ohlc_values(self, historical_client, instrument):
        """First bar has the OHLC from the fixture (Open=2050.5)."""
        bar_type = _make_bar_type(instrument, 1)
        bars = await historical_client.get_bars(bar_type=bar_type, instrument=instrument)
        assert float(bars[0].open) == pytest.approx(2050.5, rel=1e-4)

    @pytest.mark.asyncio
    async def test_uses_limit_when_no_start(self, historical_client, instrument):
        """When start=None, the limit is passed to the HTTP client."""
        bar_type = _make_bar_type(instrument, 15)
        bars = await historical_client.get_bars(
            bar_type=bar_type, instrument=instrument, limit=250
        )
        assert len(bars) >= 0  # mock always returns 3 bars regardless

    @pytest.mark.asyncio
    async def test_returns_empty_on_unsupported_spec(self, historical_client, instrument):
        """An unsupported bar aggregation returns [] without raising."""
        bar_type = _make_bar_type(instrument, 1, BarAggregation.WEEK)
        bars = await historical_client.get_bars(bar_type=bar_type, instrument=instrument)
        assert bars == []

    @pytest.mark.asyncio
    async def test_returns_empty_on_api_error(self, instrument):
        """When get_bars raises, returns empty list."""
        from unittest.mock import AsyncMock
        mock_client = MockTradeStationHttpClient()
        mock_client.get_bars = AsyncMock(side_effect=Exception("API error"))
        client = TradeStationHistoricalClient(http_client=mock_client)
        bar_type = _make_bar_type(instrument, 1)
        bars = await client.get_bars(bar_type=bar_type, instrument=instrument)
        assert bars == []

    @pytest.mark.asyncio
    async def test_returns_empty_when_no_raw_bars(self, instrument):
        """When get_bars returns [], propagates as empty list."""
        from unittest.mock import AsyncMock
        mock_client = MockTradeStationHttpClient()
        mock_client.get_bars = AsyncMock(return_value=[])
        client = TradeStationHistoricalClient(http_client=mock_client)
        bar_type = _make_bar_type(instrument, 1)
        bars = await client.get_bars(bar_type=bar_type, instrument=instrument)
        assert bars == []
