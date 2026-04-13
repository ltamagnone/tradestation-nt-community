"""
TradeStation adapter for NautilusTrader.

Provides live market data and order execution for TradeStation via their REST and
SSE streaming APIs.  Production-tested with 20+ concurrent futures strategies.

Supported:
- Async HTTP client (httpx) — no blocking calls on the event loop
- Instrument loading: Equity, FuturesContract, OptionContract
- Historical bar data (REST) via TradeStationHistoricalClient
- Live bar subscriptions (HTTP polling or SSE streaming)
- Live quote tick and trade tick subscriptions (polling or SSE streaming)
- Order submission: Market, Limit, StopMarket, StopLimit
- OCO and bracket order groups (POST /orderexecution/ordergroups)
- Fill reports, position status reports, and mass reconciliation
- OAuth 2.0 with automatic token refresh
"""

from tradestation_nt_community.config import TradeStationDataClientConfig
from tradestation_nt_community.historical.client import TradeStationHistoricalClient
from tradestation_nt_community.config import TradeStationExecClientConfig
from tradestation_nt_community.constants import TRADESTATION
from tradestation_nt_community.constants import TRADESTATION_CLIENT_ID
from tradestation_nt_community.constants import TRADESTATION_VENUE
from tradestation_nt_community.data import TradeStationDataClient
from tradestation_nt_community.factories import TradeStationLiveDataClientFactory
from tradestation_nt_community.factories import TradeStationLiveExecClientFactory
from tradestation_nt_community.http.client import TradeStationHttpClient
from tradestation_nt_community.providers import TradeStationInstrumentProvider


__all__ = [
    "TRADESTATION",
    "TRADESTATION_CLIENT_ID",
    "TRADESTATION_VENUE",
    "TradeStationDataClient",
    "TradeStationDataClientConfig",
    "TradeStationExecClientConfig",
    "TradeStationHistoricalClient",
    "TradeStationHttpClient",
    "TradeStationInstrumentProvider",
    "TradeStationLiveDataClientFactory",
    "TradeStationLiveExecClientFactory",
]
