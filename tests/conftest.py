"""
Shared fixtures for TradeStation adapter integration tests.
"""
from unittest.mock import AsyncMock, MagicMock

import pytest

from nautilus_tradestation.constants import TRADESTATION_VENUE
from nautilus_tradestation.providers import TradeStationInstrumentProvider
from nautilus_trader.model.identifiers import Venue
from tests.mock_http_client import MockTradeStationHttpClient
from tests.test_kit import TSTestInstrumentStubs


@pytest.fixture
def venue() -> Venue:
    """Return the TradeStation venue."""
    return TRADESTATION_VENUE


@pytest.fixture
def mock_http_client() -> MagicMock:
    """Return a mock HTTP client for provider tests.

    test_providers.py sets .return_value / .side_effect on individual methods.
    Since the client is now async, get_symbol_details must be an AsyncMock so
    ``await client.get_symbol_details(...)`` works in the provider's load_async.
    """
    client = MagicMock()
    client.get_symbol_details = AsyncMock()
    return client


@pytest.fixture
def stub_http_client() -> MockTradeStationHttpClient:
    """Return the concrete MockTradeStationHttpClient with pre-loaded stub data."""
    return MockTradeStationHttpClient()


@pytest.fixture
def instrument_provider(mock_http_client) -> TradeStationInstrumentProvider:
    """Create a TradeStationInstrumentProvider backed by the mock client."""
    return TradeStationInstrumentProvider(client=mock_http_client)


@pytest.fixture
def instrument():
    """Return a GC futures contract for testing."""
    return TSTestInstrumentStubs.gc_futures_contract()


@pytest.fixture
def data_client():
    """Placeholder — not used in provider tests."""
    return None


@pytest.fixture
def exec_client():
    """Placeholder — not used in provider tests."""
    return None


@pytest.fixture
def account_state():
    """Placeholder — not used in provider tests."""
    return None
