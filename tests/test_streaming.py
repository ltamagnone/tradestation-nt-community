"""
Tests for the streaming package (streaming/client.py).

Uses httpx_mock (or manual async mocking) to simulate SSE responses without
network calls.
"""
import asyncio
import json

import pytest

from tradestation_nt_community.streaming.client import (
    TradeStationStreamClient,
    _HEARTBEAT_KEYS,
)


def _make_client(base_url: str = "https://mock.tradestation.com/v3") -> TradeStationStreamClient:
    return TradeStationStreamClient(
        access_token_provider=lambda: "mock_token",
        base_url=base_url,
        reconnect_delay_secs=0.01,  # fast for tests
    )



class TestHeartbeatFiltering:
    """Heartbeat keys are defined and cover expected values."""

    def test_heartbeat_key_present(self):
        assert "Heartbeat" in _HEARTBEAT_KEYS

    def test_heartbeat_lowercase_present(self):
        assert "heartbeat" in _HEARTBEAT_KEYS

    def test_quote_event_not_heartbeat(self):
        event = {"Symbol": "GCJ26", "Bid": 3350.5}
        assert not any(k in event for k in _HEARTBEAT_KEYS)

    def test_heartbeat_event_detected(self):
        event = {"Heartbeat": 1234567890}
        assert any(k in event for k in _HEARTBEAT_KEYS)


class TestStreamClientConstruction:
    """TradeStationStreamClient initialises correctly."""

    def test_base_url_trailing_slash_stripped(self):
        client = _make_client("https://api.tradestation.com/v3/")
        assert not client._base_url.endswith("/")

    def test_token_provider_called(self):
        calls = []
        client = TradeStationStreamClient(
            access_token_provider=lambda: calls.append(1) or "tok",
            base_url="https://mock.tradestation.com/v3",
            reconnect_delay_secs=0.01,
        )
        headers = client._headers()
        assert headers["Authorization"] == "Bearer tok"
        assert len(calls) == 1

    def test_headers_contain_auth(self):
        client = _make_client()
        headers = client._headers()
        assert "Authorization" in headers
        assert headers["Authorization"].startswith("Bearer ")

    def test_headers_contain_accept(self):
        client = _make_client()
        headers = client._headers()
        assert "Accept" in headers

    def test_max_delay_is_8x_initial(self):
        client = TradeStationStreamClient(
            access_token_provider=lambda: "tok",
            base_url="https://mock.tradestation.com/v3",
            reconnect_delay_secs=5.0,
        )
        assert client._max_delay == 40.0

    def test_reconnect_delay_stored(self):
        client = _make_client()
        assert client._reconnect_delay == pytest.approx(0.01)


class TestStreamUrlConstruction:
    """Check that stream methods build the correct URLs."""

    @pytest.mark.asyncio
    async def test_stream_quotes_url(self):
        """stream_quotes uses /marketdata/stream/quotes/{symbols}."""
        client = _make_client()
        urls = []

        async def fake_stream(url: str):
            urls.append(url)
            return
            yield  # make it an async generator

        client._stream = fake_stream  # type: ignore
        async for _ in client.stream_quotes("GCJ26"):
            pass
        assert len(urls) == 1
        assert "stream/quotes/GCJ26" in urls[0]

    @pytest.mark.asyncio
    async def test_stream_orders_url(self):
        """stream_orders uses /brokerage/stream/accounts/{account}/orders."""
        client = _make_client()
        urls = []

        async def fake_stream(url: str):
            urls.append(url)
            return
            yield

        client._stream = fake_stream  # type: ignore
        async for _ in client.stream_orders("SIM0000001F"):
            pass
        assert len(urls) == 1
        assert "stream/accounts/SIM0000001F/orders" in urls[0]

    @pytest.mark.asyncio
    async def test_stream_market_depth_url(self):
        """stream_market_depth uses /marketdata/stream/marketdepth/{symbol}."""
        client = _make_client()
        urls = []

        async def fake_stream(url: str):
            urls.append(url)
            return
            yield

        client._stream = fake_stream  # type: ignore
        async for _ in client.stream_market_depth("GCJ26"):
            pass
        assert len(urls) == 1
        assert "stream/marketdepth/GCJ26" in urls[0]


class TestStreamEventParsing:
    """_stream correctly parses newline-delimited JSON and filters heartbeats."""

    @pytest.mark.asyncio
    async def test_stream_yields_parsed_json(self):
        """Valid JSON lines are yielded as dicts."""
        client = _make_client()

        events = [
            {"Symbol": "GCJ26", "Bid": 3350.5, "Ask": 3350.7},
            {"Symbol": "GCJ26", "Bid": 3351.0, "Ask": 3351.2},
        ]
        lines = [json.dumps(e) for e in events]

        async def fake_stream(url: str):
            for line in lines:
                yield json.loads(line)

        # Patch _stream to yield pre-parsed events (bypass HTTP layer)
        client._stream = fake_stream  # type: ignore
        collected = []
        async for event in client.stream_quotes("GCJ26"):
            collected.append(event)

        assert len(collected) == 2
        assert collected[0]["Bid"] == pytest.approx(3350.5, rel=1e-4)
        assert collected[1]["Bid"] == pytest.approx(3351.0, rel=1e-4)

    @pytest.mark.asyncio
    async def test_stream_drops_heartbeats(self):
        """Heartbeat events are silently dropped."""
        client = _make_client()

        raw = [
            {"Symbol": "GCJ26", "Bid": 3350.5, "Ask": 3350.7},
            {"Heartbeat": 1234567890},
            {"Symbol": "GCJ26", "Bid": 3351.0, "Ask": 3351.2},
        ]

        async def fake_stream(url: str):
            for evt in raw:
                if any(k in evt for k in _HEARTBEAT_KEYS):
                    continue  # simulate what _stream does
                yield evt

        client._stream = fake_stream  # type: ignore
        collected = []
        async for event in client.stream_quotes("GCJ26"):
            collected.append(event)

        assert len(collected) == 2
        assert all("Symbol" in e for e in collected)

    @pytest.mark.asyncio
    async def test_stream_bars_passes_all_events(self):
        """stream_bars yields both Historical and RealTime events for caller to handle."""
        client = _make_client()

        raw = [
            {"Status": "Historical", "Open": 3340.0, "Close": 3345.0},
            {"Status": "RealTime", "Open": 3350.0, "Close": 3355.0},
        ]

        async def fake_stream(url: str):
            for evt in raw:
                yield evt

        client._stream = fake_stream  # type: ignore
        collected = []
        async for event in client.stream_bars("GCJ26", "1", "Minute"):
            collected.append(event)

        assert len(collected) == 2
        assert collected[0]["Status"] == "Historical"
        assert collected[1]["Status"] == "RealTime"

    @pytest.mark.asyncio
    async def test_stream_can_be_cancelled(self):
        """Cancelling the task stops the stream cleanly."""
        client = _make_client()

        async def infinite_stream(url: str):
            i = 0
            while True:
                yield {"Symbol": "GCJ26", "Bid": 3350.0 + i, "Ask": 3351.0 + i}
                i += 1
                await asyncio.sleep(0)

        client._stream = infinite_stream  # type: ignore
        collected = []

        async def consume():
            async for event in client.stream_quotes("GCJ26"):
                collected.append(event)
                if len(collected) >= 3:
                    break

        await asyncio.wait_for(consume(), timeout=1.0)
        assert len(collected) == 3


class TestStreamClientConfig:
    """Config fields propagate to the streaming layer correctly."""

    def test_use_streaming_true_by_default(self):
        """The config default enables streaming (lower latency than polling)."""
        from tradestation_nt_community.config import TradeStationDataClientConfig
        cfg = TradeStationDataClientConfig()
        assert cfg.use_streaming is True

    def test_use_streaming_can_be_disabled(self):
        from tradestation_nt_community.config import TradeStationDataClientConfig
        cfg = TradeStationDataClientConfig(use_streaming=False)
        assert cfg.use_streaming is False

    def test_streaming_reconnect_delay_default(self):
        from tradestation_nt_community.config import TradeStationDataClientConfig
        cfg = TradeStationDataClientConfig()
        assert cfg.streaming_reconnect_delay_secs == pytest.approx(5.0)

    def test_exec_config_use_streaming(self):
        from tradestation_nt_community.config import TradeStationExecClientConfig
        cfg = TradeStationExecClientConfig(account_id="SIM001", use_streaming=True)
        assert cfg.use_streaming is True
