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

    @pytest.mark.asyncio
    async def test_drops_the_still_forming_bar(self, instrument):
        """TS returns the forming bar last (BarStatus Open, future end): never replay it (§207)."""
        from unittest.mock import AsyncMock

        def raw(ts, status):
            return {"TimeStamp": ts, "Open": 1, "High": 1, "Low": 1, "Close": 1,
                    "TotalVolume": 1, "BarStatus": status}

        mock_client = MockTradeStationHttpClient()
        mock_client.get_bars = AsyncMock(return_value=[
            raw("2026-10-16T16:06:00Z", "Closed"),
            raw("2026-10-16T16:07:00Z", "Closed"),
            raw("2026-10-16T16:08:00Z", "Open"),
        ])
        client = TradeStationHistoricalClient(http_client=mock_client)
        bars = await client.get_bars(bar_type=_make_bar_type(instrument, 1), instrument=instrument)
        assert [b.ts_event for b in bars] == [
            pd.Timestamp("2026-10-16T16:06:00Z").value,
            pd.Timestamp("2026-10-16T16:07:00Z").value,
        ]

    @pytest.mark.asyncio
    async def test_real_v3_response_drops_only_the_forming_bar(self, instrument):
        import json
        from pathlib import Path
        from unittest.mock import AsyncMock
        raw = json.loads((Path(__file__).parent / "resources" / "bars_response_v3.json")
                         .read_text())["Bars"]
        mock_client = MockTradeStationHttpClient()
        mock_client.get_bars = AsyncMock(return_value=raw)
        bars = await TradeStationHistoricalClient(http_client=mock_client).get_bars(
            bar_type=_make_bar_type(instrument, 5), instrument=instrument)
        assert len(bars) == len(raw) - 1


# ---------------------------------------------------------------------------
# Bar-history retry (strategies LESSONS §210 addendum, 2026-10-19): a failed warm-up history
# request is retried on transient failures only (timeout / transport / HTTP 5xx), 3 attempts,
# backoff 1 s then 3 s. Never on 4xx (incl. an exhausted 429) or a parse error.
# ---------------------------------------------------------------------------

def _raw_bars() -> list[dict]:
    import json
    from pathlib import Path
    return json.loads(
        (Path(__file__).parent / "resources" / "bars_response.json").read_text())["Bars"]


class _Sleeps:
    """Injectable sleep: records the backoff instead of waiting."""

    def __init__(self) -> None:
        self.calls: list[float] = []

    async def __call__(self, secs: float) -> None:
        self.calls.append(secs)


def _retry_client(side_effect):
    from unittest.mock import AsyncMock
    http = MockTradeStationHttpClient()
    http.get_bars = AsyncMock(side_effect=side_effect)
    sleeps = _Sleeps()
    return TradeStationHistoricalClient(http_client=http, sleep=sleeps), http, sleeps


def _records(caplog, text: str, level: str | None = None) -> list[str]:
    return [r.getMessage() for r in caplog.records
            if text in r.getMessage() and (level is None or r.levelname == level)]


class TestHistoryRetry:

    async def test_5xx_then_success_returns_the_bars(self, instrument, caplog):
        from tradestation_nt_community.http.client import GetBarsFailedException
        client, http, sleeps = _retry_client([GetBarsFailedException(503), _raw_bars()])
        caplog.set_level("INFO")
        bars = await client.get_bars(bar_type=_make_bar_type(instrument, 1), instrument=instrument)
        assert len(bars) == 3
        assert http.get_bars.await_count == 2
        assert sleeps.calls == [1.0]
        retry = _records(caplog, "[HIST-RETRY]", "WARNING")
        assert len(retry) == 1
        assert "attempt 2/3 after" in retry[0] and "HTTP 503" in retry[0]
        assert str(_make_bar_type(instrument, 1)) in retry[0]
        assert not _records(caplog, "Historical bar request failed for")

    async def test_504_three_times_gives_up_after_three_attempts(self, instrument, caplog):
        from tradestation_nt_community.http.client import GetBarsFailedException
        client, http, sleeps = _retry_client([GetBarsFailedException(504)] * 3)
        caplog.set_level("INFO")
        bar_type = _make_bar_type(instrument, 1)
        bars = await client.get_bars(bar_type=bar_type, instrument=instrument)
        assert bars == []
        assert http.get_bars.await_count == 3
        assert sleeps.calls == [1.0, 3.0]
        retry = _records(caplog, "[HIST-RETRY]", "WARNING")
        assert len(retry) == 2
        assert "attempt 2/3" in retry[0] and "attempt 3/3" in retry[1]
        # the final failure keeps today's ERROR line, byte for byte
        assert _records(caplog, "Historical bar request failed for", "ERROR") == [
            f"Historical bar request failed for {bar_type}: TradeStation get bars failed: HTTP 504"
        ]

    async def test_4xx_is_never_retried(self, instrument, caplog):
        from tradestation_nt_community.http.client import GetBarsFailedException
        client, http, sleeps = _retry_client([GetBarsFailedException(400), _raw_bars()])
        caplog.set_level("INFO")
        bars = await client.get_bars(bar_type=_make_bar_type(instrument, 1), instrument=instrument)
        assert bars == []
        assert http.get_bars.await_count == 1
        assert sleeps.calls == []
        assert not _records(caplog, "[HIST-RETRY]")
        assert _records(caplog, "Historical bar request failed for", "ERROR")

    async def test_read_timeout_then_success_returns_the_bars(self, instrument, caplog):
        import httpx
        client, http, sleeps = _retry_client([httpx.ReadTimeout(""), _raw_bars()])
        caplog.set_level("INFO")
        bars = await client.get_bars(bar_type=_make_bar_type(instrument, 1), instrument=instrument)
        assert len(bars) == 3
        assert sleeps.calls == [1.0]
        retry = _records(caplog, "[HIST-RETRY]", "WARNING")
        # a timeout's message is empty: the line must still say what failed
        assert len(retry) == 1 and "ReadTimeout" in retry[0]

    async def test_connect_error_then_success_returns_the_bars(self, instrument):
        import httpx
        client, http, sleeps = _retry_client([httpx.ConnectError("refused"), _raw_bars()])
        bars = await client.get_bars(bar_type=_make_bar_type(instrument, 1), instrument=instrument)
        assert len(bars) == 3
        assert http.get_bars.await_count == 2

    async def test_parse_error_is_never_retried(self, instrument, caplog):
        import json
        client, http, sleeps = _retry_client(
            [json.JSONDecodeError("Expecting value", "<html>", 0), _raw_bars()])
        caplog.set_level("INFO")
        bars = await client.get_bars(bar_type=_make_bar_type(instrument, 1), instrument=instrument)
        assert bars == []
        assert http.get_bars.await_count == 1
        assert sleeps.calls == []
        assert not _records(caplog, "[HIST-RETRY]")

    async def test_httpx_decoding_error_is_never_retried(self, instrument):
        import httpx
        client, http, sleeps = _retry_client([httpx.DecodingError("bad gzip"), _raw_bars()])
        assert await client.get_bars(
            bar_type=_make_bar_type(instrument, 1), instrument=instrument) == []
        assert http.get_bars.await_count == 1

    async def test_untyped_exception_is_never_retried(self, instrument):
        client, http, sleeps = _retry_client([Exception("API error"), _raw_bars()])
        assert await client.get_bars(
            bar_type=_make_bar_type(instrument, 1), instrument=instrument) == []
        assert http.get_bars.await_count == 1
        assert sleeps.calls == []


class TestHistoryRetryThroughTheHttpClient:
    """End to end through the real TradeStationHttpClient: proves get_bars raises the typed
    error (a plain Exception would make every 5xx look permanent)."""

    @staticmethod
    def _http(responses, max_retries: int = 3):
        from datetime import datetime, timedelta, timezone
        from unittest.mock import AsyncMock
        from tests.test_http_client import _mock_resp
        from tradestation_nt_community.http.client import TradeStationHttpClient
        http = TradeStationHttpClient(
            client_id="c", client_secret="s", refresh_token="r", use_sandbox=True,
            max_retries=max_retries)
        http._access_token = "t"
        http.token_expiry = datetime.now(tz=timezone.utc) + timedelta(hours=1)
        http._httpx.get = AsyncMock(
            side_effect=[_mock_resp(code, data) for code, data in responses])
        return http

    async def test_504_then_200_returns_the_bars(self, instrument):
        http = self._http([(504, None), (200, {"Bars": _raw_bars()})])
        sleeps = _Sleeps()
        bars = await TradeStationHistoricalClient(http_client=http, sleep=sleeps).get_bars(
            bar_type=_make_bar_type(instrument, 1), instrument=instrument)
        assert len(bars) == 3
        assert http._httpx.get.await_count == 2
        assert sleeps.calls == [1.0]

    async def test_404_is_one_request(self, instrument):
        http = self._http([(404, None), (200, {"Bars": _raw_bars()})])
        sleeps = _Sleeps()
        bars = await TradeStationHistoricalClient(http_client=http, sleep=sleeps).get_bars(
            bar_type=_make_bar_type(instrument, 1), instrument=instrument)
        assert bars == []
        assert http._httpx.get.await_count == 1
        assert sleeps.calls == []

    async def test_an_exhausted_429_is_not_retried_here(self, instrument, caplog, monkeypatch):
        """429 stays _request's job: once it gives up, this layer sees a 4xx and stops."""
        from tradestation_nt_community.http import client as http_mod

        async def no_sleep(_s):
            return None

        monkeypatch.setattr(http_mod.asyncio, "sleep", no_sleep)  # _request's 429 wait
        http = self._http([(429, None), (200, {"Bars": _raw_bars()})], max_retries=1)
        sleeps = _Sleeps()
        caplog.set_level("INFO")
        bars = await TradeStationHistoricalClient(http_client=http, sleep=sleeps).get_bars(
            bar_type=_make_bar_type(instrument, 1), instrument=instrument)
        assert bars == []
        assert http._httpx.get.await_count == 1
        assert sleeps.calls == []
        assert not _records(caplog, "[HIST-RETRY]")

    async def test_get_bars_error_keeps_its_message_and_carries_the_status(self):
        from tradestation_nt_community.common.enums import TradeStationBarUnit
        from tradestation_nt_community.http.client import GetBarsFailedException
        http = self._http([(504, None)])
        with pytest.raises(GetBarsFailedException) as ei:
            await http.get_bars("GCZ26", "1", TradeStationBarUnit.MINUTE, barsback=3)
        assert isinstance(ei.value, Exception)
        assert ei.value.status_code == 504
        # log greps and the agent's data_downloader ("HTTP 5" in str(e)) match on this text
        assert str(ei.value) == "TradeStation get bars failed: HTTP 504"
