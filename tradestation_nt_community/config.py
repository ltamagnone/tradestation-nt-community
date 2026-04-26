"""
TradeStation adapter configuration.
"""

from nautilus_trader.config import LiveDataClientConfig
from nautilus_trader.config import LiveExecClientConfig


class TradeStationDataClientConfig(LiveDataClientConfig, frozen=True):
    """
    Configuration for ``TradeStationDataClient`` instances.

    Parameters
    ----------
    client_id : str, optional
        The TradeStation API client ID.
        If ``None`` then will source from the `TRADESTATION_CLIENT_ID` environment variable.
    client_secret : str, optional
        The TradeStation API client secret.
        If ``None`` then will source from the `TRADESTATION_CLIENT_SECRET` environment variable.
    refresh_token : str, optional
        The TradeStation OAuth refresh token.
        If ``None`` then will source from the `TRADESTATION_REFRESH_TOKEN` environment variable.
    use_sandbox : bool, default False
        If True, use the TradeStation sandbox/simulation API.
        If False, use the production API.
    account_id : str, optional
        The TradeStation account ID for account-specific operations.
    base_url_http : str, optional
        Override the default HTTP API base URL.
    recv_window_ms : int, default 5000
        The API request signature receive window in milliseconds.
    max_retries : int, optional
        The maximum number of retry attempts for failed requests.
    retry_delay_initial_ms : int, default 1000
        The initial delay (milliseconds) between retry attempts (exponential backoff).
    retry_delay_max_ms : int, default 60000
        The maximum delay (milliseconds) between retry attempts.

    """

    client_id: str | None = None
    client_secret: str | None = None
    refresh_token: str | None = None
    use_sandbox: bool = False
    account_id: str | None = None
    base_url_http: str | None = None
    allow_custom_base_url: bool = False  # Skip hostname validation on base_url_http
    recv_window_ms: int = 5000
    max_retries: int | None = None
    retry_delay_initial_ms: int = 1000
    retry_delay_max_ms: int = 60000
    instrument_ids: tuple[str, ...] = ()  # Pre-load these instruments during _connect()
    use_streaming: bool = True  # Use SSE streaming for bars (lower latency than polling)
    streaming_reconnect_delay_secs: float = 5.0  # Initial SSE reconnect delay (doubles on failure)
    extended_hours: bool = False  # If True, use USEQPreAndPost session template for equity bar streams


class TradeStationExecClientConfig(LiveExecClientConfig, frozen=True, kw_only=True):
    """
    Configuration for ``TradeStationExecutionClient`` instances.

    Parameters
    ----------
    account_id : str
        The TradeStation account ID for trading operations (required for execution).
    client_id : str, optional
        The TradeStation API client ID.
        If ``None`` then will source from the `TRADESTATION_CLIENT_ID` environment variable.
    client_secret : str, optional
        The TradeStation API client secret.
        If ``None`` then will source from the `TRADESTATION_CLIENT_SECRET` environment variable.
    refresh_token : str, optional
        The TradeStation OAuth refresh token.
        If ``None`` then will source from the `TRADESTATION_REFRESH_TOKEN` environment variable.
    use_sandbox : bool, default False
        If True, use the TradeStation sandbox/simulation API.
        If False, use the production API.
    base_url_http : str, optional
        Override the default HTTP API base URL.
    base_url_ws : str, optional
        Override the default WebSocket API base URL (for future streaming implementation).
    recv_window_ms : int, default 5000
        The API request signature receive window in milliseconds.
    max_retries : int, optional
        The maximum number of retry attempts for failed requests.
    retry_delay_initial_ms : int, default 1000
        The initial delay (milliseconds) between retry attempts (exponential backoff).
    retry_delay_max_ms : int, default 60000
        The maximum delay (milliseconds) between retry attempts.

    """

    account_id: str  # Required for execution
    client_id: str | None = None
    client_secret: str | None = None
    refresh_token: str | None = None
    use_sandbox: bool = False
    base_url_http: str | None = None
    allow_custom_base_url: bool = False  # Skip hostname validation on base_url_http
    base_url_ws: str | None = None
    recv_window_ms: int = 5000
    max_retries: int | None = None
    retry_delay_initial_ms: int = 1000
    retry_delay_max_ms: int = 60000
    use_streaming: bool = True  # Use SSE order stream for fill notifications (lower latency than polling)
    streaming_reconnect_delay_secs: float = 5.0  # Initial SSE reconnect delay
    extended_hours: bool = False  # If True, use DYP duration for equity orders (pre/post market)
    order_map_path: str | None = None  # Path to persist order-ID map across restarts (T-2)
