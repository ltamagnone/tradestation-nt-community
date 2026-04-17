"""
TradeStation instrument provider implementation.
"""
import asyncio

from tradestation_nt_community.constants import TRADESTATION_VENUE
from tradestation_nt_community.http.client import TradeStationHttpClient
from tradestation_nt_community.parsing.instruments import parse_instrument
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
        Load specific instruments by their IDs concurrently.

        All non-cached instruments are fetched in parallel via
        ``asyncio.gather``. Individual failures are logged and do not abort
        the remaining loads.

        Parameters
        ----------
        instrument_ids : list[InstrumentId]
            The instrument IDs to load.
        filters : dict, optional
            Additional filters (currently not used).

        """
        to_load = [iid for iid in instrument_ids if self.find(iid) is None]
        if not to_load:
            return

        results = await asyncio.gather(
            *[self._load_single(iid) for iid in to_load],
            return_exceptions=True,
        )
        for iid, result in zip(to_load, results):
            if isinstance(result, Exception):
                self._log.error(f"Error loading instrument {iid}: {result}")

    async def _load_single(self, instrument_id: InstrumentId) -> None:
        """Fetch and register one instrument; raises on any failure."""
        symbol_str = instrument_id.symbol.value
        symbol_data = await self._client.get_symbol_details(symbol_str)
        instrument = self._parse_instrument(symbol_str, symbol_data)
        if instrument:
            self.add(instrument)
            self._log.info(f"Loaded instrument: {instrument_id}")
        else:
            self._log.warning(f"Failed to parse instrument: {instrument_id}")

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
