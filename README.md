# nautilus-tradestation

Community-contributed [TradeStation](https://www.tradestation.com/) adapter for [NautilusTrader](https://github.com/nautechsystems/nautilus_trader).

Production-tested with 20+ concurrent futures strategies in paper trading.

## Features

- **Market data:** Historical bars (REST), live bar subscriptions (HTTP polling or SSE streaming), quote ticks, trade ticks
- **Execution:** Market, Limit, StopMarket, StopLimit orders; OCO and bracket order groups
- **Instruments:** Equity, FuturesContract, OptionContract loading
- **Reconciliation:** Fill reports, order status reports, position status reports, mass reconciliation
- **Authentication:** OAuth 2.0 with automatic token refresh
- **Transport:** Async HTTP via httpx (no blocking calls on the event loop), SSE streaming with reconnect and exponential back-off

## Installation

```bash
pip install nautilus-tradestation
```

Or from source:

```bash
git clone https://github.com/ltamagno/nautilus-tradestation.git
cd nautilus-tradestation
pip install -e .
```

Requires `nautilus_trader >= 1.200` and Python 3.11+.

## Quick Start

```python
from nautilus_tradestation.config import TradeStationDataClientConfig
from nautilus_tradestation.config import TradeStationExecClientConfig
from nautilus_tradestation.factories import TradeStationLiveDataClientFactory
from nautilus_tradestation.factories import TradeStationLiveExecClientFactory

data_config = TradeStationDataClientConfig(
    client_id="YOUR_CLIENT_ID",       # or set TRADESTATION_CLIENT_ID env var
    client_secret="YOUR_SECRET",      # or set TRADESTATION_CLIENT_SECRET env var
    refresh_token="YOUR_TOKEN",       # or set TRADESTATION_REFRESH_TOKEN env var
    use_sandbox=True,                 # True for sim, False for production
    account_id="SIM12345F",
    instrument_ids=("ESM26.TRADESTATION", "GCQ26.TRADESTATION"),
    use_streaming=True,               # SSE streaming (recommended) vs HTTP polling
)

exec_config = TradeStationExecClientConfig(
    account_id="SIM12345F",
    client_id="YOUR_CLIENT_ID",
    client_secret="YOUR_SECRET",
    refresh_token="YOUR_TOKEN",
    use_sandbox=True,
    use_streaming=True,
)
```

Register the factories with the NautilusTrader `TradingNodeConfig`:

```python
from nautilus_trader.config import TradingNodeConfig

config = TradingNodeConfig(
    data_clients={"TRADESTATION": data_config},
    exec_clients={"TRADESTATION": exec_config},
)
```

See `examples/` for complete runnable scripts.

## Environment Variables

| Variable | Description |
|----------|-------------|
| `TRADESTATION_CLIENT_ID` | TradeStation API client ID |
| `TRADESTATION_CLIENT_SECRET` | TradeStation API client secret |
| `TRADESTATION_REFRESH_TOKEN` | OAuth refresh token |

## Configuration Options

### Data Client

| Option | Default | Description |
|--------|---------|-------------|
| `use_sandbox` | `False` | Use TradeStation sandbox/simulation API |
| `instrument_ids` | `()` | Pre-load these instruments during connect |
| `use_streaming` | `False` | SSE streaming for quotes/ticks (recommended) |
| `streaming_reconnect_delay_secs` | `5.0` | Initial SSE reconnect delay (doubles on failure, caps at 8x) |

### Execution Client

| Option | Default | Description |
|--------|---------|-------------|
| `account_id` | required | TradeStation account ID for trading |
| `use_streaming` | `False` | SSE order stream for real-time fill detection |

## Supported Instruments

- **Futures** (e.g., `ESM26`, `GCQ26`, `NQM26`)
- **Equities** (e.g., `AAPL`, `MSFT`)
- **Options** (OCC format, e.g., `AAPL 250321C00175000`)

## Supported Order Types

| Order Type | TradeStation Mapping |
|------------|---------------------|
| `MARKET` | `Market` |
| `LIMIT` | `Limit` |
| `STOP_MARKET` | `StopMarket` |
| `STOP_LIMIT` | `StopLimit` |

**Note:** TradeStation rejects FOK (Fill-or-Kill) orders. Use `TimeInForce.DAY` instead.

## Architecture

```
nautilus_tradestation/
  config.py          -- Data + Execution client configs
  constants.py       -- TRADESTATION venue and client ID constants
  data.py            -- LiveMarketDataClient (bar polling/streaming, quote/trade ticks)
  execution.py       -- LiveExecutionClient (order submit/cancel/modify/reconcile)
  factories.py       -- LiveDataClientFactory / LiveExecClientFactory
  providers.py       -- InstrumentProvider (on-demand symbol loading)
  types.py           -- Instrument type union alias
  common/enums.py    -- TradeStationBarUnit, OrderStatus, OrderType, etc.
  historical/client.py -- Historical bar request client (barsback estimation)
  http/client.py     -- Async HTTP client (httpx, OAuth, all REST endpoints)
  parsing/           -- Pure parsing functions (no client state)
    data.py          -- bar_spec_to_ts_params(), parse_bars(), parse_quote_tick()
    execution.py     -- parse_order_status(), convert_order_to_ts_format(), etc.
    instruments.py   -- parse_instrument() for Equity/Futures/Options
  streaming/client.py -- SSE streaming client (quotes, bars, orders, market depth)
```

## Running Tests

```bash
pip install -e ".[dev]"
pytest tests/ -q
```

## Known Constraints

- `60-MINUTE` bar spec is invalid in NautilusTrader -- use `1-HOUR` instead
- TradeStation rejects FOK orders -- use DAY for all orders
- `reduce_only` is silently ignored by the adapter (safe to include)
- No partial fill (FLP) handling -- only full fills (FLL) trigger events
- Options: instrument loading only (order submission for options not implemented)

## Related

- [NautilusTrader](https://github.com/nautechsystems/nautilus_trader) -- the trading platform
- [TradeStation API docs](https://api.tradestation.com/docs/) -- official REST/SSE API reference
- [RFC issue](https://github.com/nautechsystems/nautilus_trader/issues/3516) -- original contribution proposal
