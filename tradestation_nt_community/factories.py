"""
TradeStation client factory implementations.
"""

import asyncio
from functools import lru_cache

from tradestation_nt_community.config import TradeStationDataClientConfig
from tradestation_nt_community.config import TradeStationExecClientConfig
from tradestation_nt_community.data import TradeStationDataClient
from tradestation_nt_community.execution import TradeStationExecutionClient
from tradestation_nt_community.http.client import TradeStationHttpClient
from tradestation_nt_community.providers import TradeStationInstrumentProvider
from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import MessageBus
from nautilus_trader.live.factories import LiveDataClientFactory
from nautilus_trader.live.factories import LiveExecClientFactory


@lru_cache(1)
def get_cached_tradestation_http_client(
    client_id: str | None = None,
    client_secret: str | None = None,
    refresh_token: str | None = None,
    use_sandbox: bool = False,
    base_url: str | None = None,
    allow_custom_base_url: bool = False,
) -> TradeStationHttpClient:
    """
    Create or return a cached TradeStation HTTP client.

    This caching prevents creating multiple HTTP clients with duplicate connections.

    Parameters
    ----------
    client_id : str, optional
        The TradeStation API client ID.
    client_secret : str, optional
        The TradeStation API client secret.
    refresh_token : str, optional
        The OAuth refresh token.
    use_sandbox : bool, default False
        If True, use sandbox API.
    base_url : str, optional
        Override base URL.
    allow_custom_base_url : bool, default False
        If True, skip hostname validation on base_url.

    Return -------
    TradeStationHttpClient
        The cached HTTP client.

    """
    return TradeStationHttpClient(
        client_id=client_id,
        client_secret=client_secret,
        refresh_token=refresh_token,
        use_sandbox=use_sandbox,
        base_url=base_url,
        allow_custom_base_url=allow_custom_base_url,
    )


@lru_cache(1)
def get_cached_tradestation_instrument_provider(
    client_id: str | None = None,
    client_secret: str | None = None,
    refresh_token: str | None = None,
    use_sandbox: bool = False,
    base_url: str | None = None,
    allow_custom_base_url: bool = False,
) -> TradeStationInstrumentProvider:
    """
    Create or return a cached TradeStation instrument provider.

    Parameters
    ----------
    client_id : str, optional
        The TradeStation API client ID.
    client_secret : str, optional
        The TradeStation API client secret.
    refresh_token : str, optional
        The OAuth refresh token.
    use_sandbox : bool, default False
        If True, use sandbox API.
    base_url : str, optional
        Override base URL.
    allow_custom_base_url : bool, default False
        If True, skip hostname validation on base_url.

    Return -------
    TradeStationInstrumentProvider
        The cached instrument provider.

    """
    http_client = get_cached_tradestation_http_client(
        client_id=client_id,
        client_secret=client_secret,
        refresh_token=refresh_token,
        use_sandbox=use_sandbox,
        base_url=base_url,
        allow_custom_base_url=allow_custom_base_url,
    )

    return TradeStationInstrumentProvider(client=http_client)


class TradeStationLiveDataClientFactory(LiveDataClientFactory):
    """
    Factory for creating TradeStation data clients.
    """

    @staticmethod
    def create(
        loop: asyncio.AbstractEventLoop,
        name: str,
        config: TradeStationDataClientConfig,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
    ) -> TradeStationDataClient:
        """
        Create a new TradeStation data client.

        Parameters
        ----------
        loop : asyncio.AbstractEventLoop
            The event loop for the client.
        name : str
            The client name.
        config : TradeStationDataClientConfig
            The configuration for the client.
        msgbus : MessageBus
            The message bus for the client.
        cache : Cache
            The cache for the client.
        clock : LiveClock
            The clock for the client.

        Return -------
        TradeStationDataClient
            The created data client.

        """
        # Get or create HTTP client (cached)
        http_client = get_cached_tradestation_http_client(
            client_id=config.client_id,
            client_secret=config.client_secret,
            refresh_token=config.refresh_token,
            use_sandbox=config.use_sandbox,
            base_url=config.base_url_http,
            allow_custom_base_url=config.allow_custom_base_url,
        )

        # Get or create instrument provider (cached)
        instrument_provider = get_cached_tradestation_instrument_provider(
            client_id=config.client_id,
            client_secret=config.client_secret,
            refresh_token=config.refresh_token,
            use_sandbox=config.use_sandbox,
            base_url=config.base_url_http,
            allow_custom_base_url=config.allow_custom_base_url,
        )

        # Create and return data client
        return TradeStationDataClient(
            loop=loop,
            client=http_client,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            instrument_provider=instrument_provider,
            instrument_ids=config.instrument_ids,
            use_streaming=config.use_streaming,
            streaming_reconnect_delay_secs=config.streaming_reconnect_delay_secs,
            extended_hours=config.extended_hours,
        )


class TradeStationLiveExecClientFactory(LiveExecClientFactory):
    """
    Factory for creating TradeStation execution clients.
    """

    @staticmethod
    def create(
        loop: asyncio.AbstractEventLoop,
        name: str,
        config: TradeStationExecClientConfig,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
    ) -> TradeStationExecutionClient:
        """
        Create a new TradeStation execution client.

        Parameters
        ----------
        loop : asyncio.AbstractEventLoop
            The event loop for the client.
        name : str
            The client name.
        config : TradeStationExecClientConfig
            The configuration for the client.
        msgbus : MessageBus
            The message bus for the client.
        cache : Cache
            The cache for the client.
        clock : LiveClock
            The clock for the client.

        Return -------
        TradeStationExecutionClient
            The created execution client.

        """
        # Get or create HTTP client (cached)
        http_client = get_cached_tradestation_http_client(
            client_id=config.client_id,
            client_secret=config.client_secret,
            refresh_token=config.refresh_token,
            use_sandbox=config.use_sandbox,
            base_url=config.base_url_http,
            allow_custom_base_url=config.allow_custom_base_url,
        )

        # Get or create instrument provider (cached)
        instrument_provider = get_cached_tradestation_instrument_provider(
            client_id=config.client_id,
            client_secret=config.client_secret,
            refresh_token=config.refresh_token,
            use_sandbox=config.use_sandbox,
            base_url=config.base_url_http,
            allow_custom_base_url=config.allow_custom_base_url,
        )

        # Create and return execution client
        return TradeStationExecutionClient(
            loop=loop,
            client=http_client,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            instrument_provider=instrument_provider,
            account_id=config.account_id,
            base_url_ws=config.base_url_ws,
            use_streaming=config.use_streaming,
            streaming_reconnect_delay_secs=config.streaming_reconnect_delay_secs,
            extended_hours=config.extended_hours,
        )
