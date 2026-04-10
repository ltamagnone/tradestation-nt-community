import json
from pathlib import Path

import pytest

from nautilus_tradestation.constants import TRADESTATION_VENUE
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Symbol


# Load sample data from resources
RESOURCES_DIR = Path(__file__).parent / "resources"


def load_sample_data(filename: str) -> dict:
    """
    Load sample JSON data from resources directory.
    """
    with open(RESOURCES_DIR / filename) as f:
        return json.load(f)


# Sample API responses
GOLD_FUTURE_RESPONSE = load_sample_data("symbol_detail_future.json")
EQUITY_RESPONSE = load_sample_data("symbol_detail_equity.json")


@pytest.mark.asyncio
async def test_load_futures_contract_parses_all_fields_correctly(
    instrument_provider,
    mock_http_client,
):
    """
    Test that a futures contract is correctly parsed from TradeStation API response.

    Verifies:
    - Instrument ID generation
    - Price/quantity precision extraction
    - Expiration date parsing
    - Currency conversion
    - Multiplier calculation
    - Underlying symbol extraction
    """
    # Arrange: Mock HTTP client to return unwrapped symbol data
    # (get_symbol_details extracts from Symbols array)
    symbol = "GCG25"
    mock_http_client.get_symbol_details.return_value = GOLD_FUTURE_RESPONSE

    # Act: Load the instrument
    instrument_id = InstrumentId(Symbol(symbol), TRADESTATION_VENUE)
    await instrument_provider.load_async(instrument_id)
    instrument = instrument_provider.find(instrument_id)

    # Assert: Verify all fields were parsed correctly
    assert instrument is not None
    assert instrument.id.symbol.value == "GCG25"
    assert instrument.price_precision == 1
    assert instrument.price_increment.as_double() == 0.1
    assert instrument.multiplier.as_double() == 100
    assert instrument.quote_currency.code == "USD"
    assert instrument.underlying == "GC"
    # Verify expiration timestamp is correct
    assert instrument.expiration_ns > 0
    assert instrument.activation_ns > 0


@pytest.mark.asyncio
async def test_load_equity_parses_all_fields_correctly(
    instrument_provider,
    mock_http_client,
):
    """
    Test that an equity is correctly parsed from TradeStation API response.

    Verifies:
    - Equity instrument creation
    - Price precision (2 decimals for stocks)
    - Quantity precision
    - Currency handling
    """
    # Arrange: Mock HTTP client to return unwrapped equity data
    symbol = "AAPL"
    mock_http_client.get_symbol_details.return_value = EQUITY_RESPONSE

    # Act: Load the instrument
    instrument_id = InstrumentId(Symbol(symbol), TRADESTATION_VENUE)
    await instrument_provider.load_async(instrument_id)
    instrument = instrument_provider.find(instrument_id)

    # Assert: Verify equity fields
    assert instrument is not None
    assert instrument.id.symbol.value == "AAPL"
    assert instrument.price_precision == 2
    assert instrument.price_increment.as_double() == 0.01
    assert instrument.quote_currency.code == "USD"


@pytest.mark.asyncio
async def test_load_invalid_symbol_returns_none(
    instrument_provider,
    mock_http_client,
):
    """
    Test that requesting an invalid symbol returns None gracefully.

    This tests error handling when the API returns an empty Symbols array.
    """
    # Arrange: Mock HTTP client to return empty dict (symbol not found)
    mock_http_client.get_symbol_details.return_value = {}

    # Act: Try to load invalid symbol
    instrument_id = InstrumentId(Symbol("INVALID"), TRADESTATION_VENUE)
    await instrument_provider.load_async(instrument_id)
    instrument = instrument_provider.find(instrument_id)

    # Assert: Should return None for invalid symbol
    assert instrument is None


@pytest.mark.asyncio
async def test_load_symbol_with_missing_fields_uses_defaults(
    instrument_provider,
    mock_http_client,
):
    """
    Test that missing optional fields use sensible defaults.

    This ensures robustness when API responses are incomplete.
    """
    # Arrange: Create minimal response with some fields missing
    minimal_response = {
        "Symbol": "TEST",
        "AssetType": "STOCK",
        # Missing: PriceFormat, QuantityFormat, Currency, etc.
    }
    mock_http_client.get_symbol_details.return_value = minimal_response

    # Act: Load the instrument
    instrument_id = InstrumentId(Symbol("TEST"), TRADESTATION_VENUE)
    await instrument_provider.load_async(instrument_id)
    instrument = instrument_provider.find(instrument_id)

    # Assert: Should create instrument with default values
    assert instrument is not None
    assert instrument.id.symbol.value == "TEST"
    # Default precision should be 2
    assert instrument.price_precision == 2
    # Default currency should be USD
    assert instrument.quote_currency.code == "USD"


@pytest.mark.asyncio
async def test_load_futures_with_different_point_values(
    instrument_provider,
    mock_http_client,
):
    """
    Test that different futures contracts with different multipliers are handled.

    Tests contracts like ES (multiplier 50) vs GC (multiplier 100).
    """
    # Arrange: Create futures response with different point value
    es_response = {
        "Symbol": "ESH25",
        "Description": "E-mini S&P 500 Mar 25",
        "AssetType": "FUTURE",
        "Currency": "USD",
        "ContractExpireDate": "2025-03-20T00:00:00Z",
        "PriceFormat": {
            "Decimals": 2,
            "Increment": "0.25",
            "PointValue": 50,  # ES has $50 per point
        },
        "QuantityFormat": {
            "Decimals": 0,
            "Increment": "1",
        },
        "Underlying": "ES",
    }
    mock_http_client.get_symbol_details.return_value = es_response

    # Act: Load the instrument
    instrument_id = InstrumentId(Symbol("ESH25"), TRADESTATION_VENUE)
    await instrument_provider.load_async(instrument_id)
    instrument = instrument_provider.find(instrument_id)

    # Assert: Verify multiplier is correct
    assert instrument is not None
    assert instrument.multiplier.as_double() == 50
    assert instrument.price_precision == 2
    assert instrument.price_increment.as_double() == 0.25


@pytest.mark.asyncio
async def test_load_async_caches_instrument(
    instrument_provider,
    mock_http_client,
):
    """
    Test that loaded instruments are cached and subsequent calls don't hit the API.

    This verifies the caching behavior of the instrument provider.
    """
    # Arrange
    symbol = "GCG25"
    mock_http_client.get_symbol_details.return_value = GOLD_FUTURE_RESPONSE

    # Act: Load the same instrument twice
    instrument_id = InstrumentId(Symbol(symbol), TRADESTATION_VENUE)
    await instrument_provider.load_async(instrument_id)
    first_instrument = instrument_provider.find(instrument_id)

    await instrument_provider.load_async(instrument_id)
    second_instrument = instrument_provider.find(instrument_id)

    # Assert: Should return the same cached instance
    assert first_instrument is second_instrument
    # HTTP client should only be called once due to caching
    assert mock_http_client.get_symbol_details.call_count == 1


@pytest.mark.asyncio
async def test_list_all_returns_all_loaded_instruments(
    instrument_provider,
    mock_http_client,
):
    """
    Test that list_all returns all instruments that have been loaded.
    """
    # Arrange: Load multiple instruments
    mock_http_client.get_symbol_details.side_effect = [
        GOLD_FUTURE_RESPONSE,
        EQUITY_RESPONSE,
    ]

    # Act: Load two different instruments
    await instrument_provider.load_async(
        InstrumentId(Symbol("GCG25"), TRADESTATION_VENUE),
    )
    await instrument_provider.load_async(
        InstrumentId(Symbol("AAPL"), TRADESTATION_VENUE),
    )

    all_instruments = instrument_provider.list_all()

    # Assert: Should return both instruments
    assert len(all_instruments) == 2
    symbols = {instr.id.symbol.value for instr in all_instruments}
    assert "GCG25" in symbols
    assert "AAPL" in symbols


@pytest.mark.asyncio
async def test_find_returns_none_for_not_loaded_instrument(
    instrument_provider,
):
    """
    Test that find returns None for instruments that haven't been loaded.
    """
    # Arrange: Don't load any instruments

    # Act: Try to find an instrument that was never loaded
    instrument_id = InstrumentId(Symbol("NOTLOADED"), TRADESTATION_VENUE)
    instrument = instrument_provider.find(instrument_id)

    # Assert: Should return None
    assert instrument is None


@pytest.mark.asyncio
async def test_load_option_contract_parses_correctly(
    instrument_provider,
    mock_http_client,
):
    """
    Test that an option contract is correctly parsed through the provider.

    The mock HTTP client routes OCC-format symbols (containing a space) to
    the symbol_detail_option.json fixture.
    """
    from nautilus_trader.model.instruments import OptionContract
    from nautilus_trader.model.enums import OptionKind

    OPTION_RESPONSE = load_sample_data("symbol_detail_option.json")
    mock_http_client.get_symbol_details.return_value = OPTION_RESPONSE

    symbol = "AAPL 250321C00175000"
    instrument_id = InstrumentId(Symbol(symbol), TRADESTATION_VENUE)
    await instrument_provider.load_async(instrument_id)
    instrument = instrument_provider.find(instrument_id)

    assert instrument is not None
    assert isinstance(instrument, OptionContract)
    assert instrument.option_kind == OptionKind.CALL
    assert float(instrument.strike_price) == pytest.approx(175.0, rel=1e-4)
    assert instrument.underlying == "AAPL"
    assert instrument.expiration_ns > 0
