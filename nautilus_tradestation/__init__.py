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

from nautilus_tradestation.config import TradeStationDataClientConfig
from nautilus_tradestation.historical.client import TradeStationHistoricalClient
from nautilus_tradestation.config import TradeStationExecClientConfig
from nautilus_tradestation.constants import TRADESTATION
from nautilus_tradestation.constants import TRADESTATION_CLIENT_ID
from nautilus_tradestation.constants import TRADESTATION_VENUE
from nautilus_tradestation.data import TradeStationDataClient
from nautilus_tradestation.factories import TradeStationLiveDataClientFactory
from nautilus_tradestation.factories import TradeStationLiveExecClientFactory
from nautilus_tradestation.http.client import TradeStationHttpClient
from nautilus_tradestation.providers import TradeStationInstrumentProvider


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
