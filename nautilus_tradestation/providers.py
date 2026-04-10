"""
TradeStation instrument provider implementation.
"""

from nautilus_tradestation.constants import TRADESTATION_VENUE
from nautilus_tradestation.http.client import TradeStationHttpClient
from nautilus_tradestation.parsing.instruments import parse_instrument
from nautilus_trader.common.providers import InstrumentProvider
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.instruments import Equity
from nautilus_trader.model.instruments import FuturesContract
from nautilus_trader.model.instruments import OptionContract


class TradeStationInstrumentProvider(InstrumentProvider):
    """
    Provide instrument loading and management for TradeStation.

    Parameters
    ----------
    client : TradeStationHttpClient
        The TradeStation HTTP client.
    filters : dict[str, Any], optional
        Optional filters for instruments (e.g., {'asset_type': 'FUTURE'}).

    """

    def __init__(
        self,
        client: TradeStationHttpClient,
        filters: dict | None = None,
    ) -> None:
        super().__init__()
        self._client = client
        self._filters = filters or {}
        self._log_warnings = True

    async def load_all_async(self, filters: dict | None = None) -> None:
        """
        Load all instruments from TradeStation.

        For TradeStation, this is not practical as there are thousands of symbols.
        Instead, instruments should be loaded on-demand via `load_ids_async()`.

        Parameters
        ----------
        filters : dict, optional
            Additional filters (currently not used).

        """
        # TradeStation has too many symbols to load all at once
        # Instruments should be loaded on-demand using load_ids_async()
        self._log.warning(
            "load_all_async not supported for TradeStation. "
            "Use load_ids_async() or load_async() to load specific instruments.",
        )

    async def load_ids_async(
        self,
        instrument_ids: list[InstrumentId],
        filters: dict | None = None,
    ) -> None:
        """
        Load specific instruments by their IDs.

        Parameters
        ----------
        instrument_ids : list[InstrumentId]
            The instrument IDs to load.
        filters : dict, optional
            Additional filters (currently not used).

        """
        for instrument_id in instrument_ids:
            try:
                # Check if already cached
                if self.find(instrument_id) is not None:
                    self._log.debug(f"Instrument already loaded: {instrument_id}")
                    continue

                # Extract symbol from instrument ID
                symbol_str = instrument_id.symbol.value

                # Get symbol details from TradeStation
                symbol_data = await self._client.get_symbol_details(symbol_str)

                # Parse and create instrument
                instrument = self._parse_instrument(symbol_str, symbol_data)

                if instrument:
                    self.add(instrument)
                    self._log.info(f"Loaded instrument: {instrument_id}")
                else:
                    self._log.warning(f"Failed to parse instrument: {instrument_id}")

            except Exception as e:
                self._log.error(f"Error loading instrument {instrument_id}: {e}")

    async def load_async(self, instrument_id: InstrumentId, filters: dict | None = None) -> None:
        """
        Load a single instrument.

        Parameters
        ----------
        instrument_id : InstrumentId
            The instrument ID to load.
        filters : dict, optional
            Additional filters (currently not used).

        """
        await self.load_ids_async([instrument_id], filters)

    def _parse_instrument(
        self,
        symbol: str,
        data: dict,
    ) -> Equity | FuturesContract | OptionContract | None:
        return parse_instrument(symbol, data, TRADESTATION_VENUE)
