# TradeStation

Founded in 1982, TradeStation is a US broker and trading platform offering equities,
futures, options, and forex through both a desktop platform and a REST/SSE API.

NautilusTrader provides an async adapter for TradeStation's REST API that supports
live market data, order execution, and account management for futures and equities.

## Installation

This adapter is included in the main NautilusTrader package:

```bash
pip install nautilus_trader
```

No additional dependencies are required (uses `httpx` which is already bundled).

## Getting Started

### 1. Create a TradeStation Developer App

1. Sign in at [developer.tradestation.com](https://developer.tradestation.com/)
2. Create a new application to obtain your **Client ID** and **Client Secret**
3. Complete the OAuth 2.0 flow to obtain a **Refresh Token**

:::info
The refresh token grants long-lived access without repeated logins. Rotate it if
you suspect it has been compromised. TradeStation refresh tokens are valid for
approximately 90 days of inactivity.
:::

### 2. Set Credentials

Supply credentials via environment variables (recommended):

```bash
export TRADESTATION_CLIENT_ID="your_client_id"
export TRADESTATION_CLIENT_SECRET="your_client_secret"
export TRADESTATION_REFRESH_TOKEN="your_refresh_token"
export TRADESTATION_ACCOUNT_ID="your_account_id"
```

Or pass them directly in the configuration objects.

### 3. Sandbox vs Production

TradeStation provides a **sandbox** (simulation) environment for paper trading:

| Environment | Base URL |
|---|---|
| Sandbox | `https://sim-api.tradestation.com/v3` |
| Production | `https://api.tradestation.com/v3` |

Set `use_sandbox=True` for paper trading (recommended for strategy development).

## Configuration

### Data Client

```python
from tradestation_nt_community.config import TradeStationDataClientConfig

config = TradeStationDataClientConfig(
    client_id="YOUR_CLIENT_ID",        # or env var TRADESTATION_CLIENT_ID
    client_secret="YOUR_SECRET",        # or env var TRADESTATION_CLIENT_SECRET
    refresh_token="YOUR_REFRESH_TOKEN", # or env var TRADESTATION_REFRESH_TOKEN
    use_sandbox=True,                   # False for production API
    instrument_ids=(                    # Pre-load instruments at connect time
        "GCJ26.TRADESTATION",
        "ESM26.TRADESTATION",
    ),
    use_streaming=False,                # True to use SSE instead of polling
    streaming_reconnect_delay_secs=5.0, # SSE reconnect back-off initial delay
)
```

### Execution Client

```python
from tradestation_nt_community.config import TradeStationExecClientConfig

config = TradeStationExecClientConfig(
    account_id="SIM0000001F",           # Required — your TradeStation account ID
    client_id="YOUR_CLIENT_ID",
    client_secret="YOUR_SECRET",
    refresh_token="YOUR_REFRESH_TOKEN",
    use_sandbox=True,
    use_streaming=False,                # True for real-time fill detection via SSE
)
```

## Supported Features

### Instruments

| Asset type | NT class | Notes |
|---|---|---|
| `STOCK` | `Equity` | US equities and ETFs |
| `FUTURE` | `FuturesContract` | CME, NYMEX, CBOT, EUREX |
| `OPTION` | `OptionContract` | OCC format (`AAPL 250321C00175000`) — loading only, no execution |

### Bar Subscriptions

Bars are delivered via HTTP polling by default. Set `use_streaming=True` for SSE delivery.

| Bar spec | Polling interval |
|---|---|
| `MINUTE` | 60 seconds |
| `HOUR` / `DAY` | 300 seconds |

```python
# Subscribe to 15-minute Gold bars
strategy.subscribe_bars(BarType.from_str("GCJ26.TRADESTATION-15-MINUTE-LAST-EXTERNAL"))
```

### Quote and Trade Ticks

```python
# Subscribe to real-time quotes (polling every 1s, or immediate with streaming)
strategy.subscribe_quote_ticks(InstrumentId.from_str("GCJ26.TRADESTATION"))

# Subscribe to last-price trade ticks
strategy.subscribe_trade_ticks(InstrumentId.from_str("GCJ26.TRADESTATION"))
```

:::note
Quote and trade tick subscriptions share a single `GET /marketdata/stream/quotes` SSE
connection per instrument via a shared multiplexer. Each event is dispatched to the
quote handler and/or trade handler based on which subscriptions are active.
:::

### Order Types

| NT order type | TradeStation type |
|---|---|
| `MarketOrder` | `Market` |
| `LimitOrder` | `Limit` |
| `StopMarketOrder` | `StopMarket` |
| `StopLimitOrder` | `StopLimit` |

### Time in Force

| NT TIF | TradeStation duration |
|---|---|
| `DAY` | `DAY` |
| `GTC` | `GTC` |
| `IOC` | `IOC` |
| `FOK` | `FOK` |

:::warning
TradeStation rejects FOK orders in some account types. Use `DAY` for market orders
that need immediate fill — they execute instantly at market price.
:::

### Order Groups (OCO / Bracket)

`SubmitOrderList` commands with `ContingencyType` fields are submitted atomically
to `POST /v3/orderexecution/ordergroups`:

| NT pattern | TradeStation type | Use case |
|---|---|---|
| All `OCO` | `"OCO"` | Two exits cancel each other (stop loss + take profit) |
| One `OTO` + ≥2 `OCO` | `"BRK"` | Entry triggers OCO exit group (bracket) |
| Other | individual | Submitted as separate orders |

### SSE Streaming (opt-in)

Enable real-time data by setting `use_streaming=True`:

```python
config = TradeStationDataClientConfig(use_streaming=True, use_sandbox=True, ...)
```

| What | Endpoint |
|---|---|
| Quotes + ticks | `GET /marketdata/stream/quotes/{symbols}` |
| Real-time bars | `GET /marketdata/stream/barcharts/{symbol}` (buffers partial updates, emits on bar close) |
| Order fills | `GET /brokerage/stream/accounts/{account}/orders` |
| Market depth | `GET /marketdata/stream/marketdepth/{symbol}` (available via `stream_market_depth()`) |

The stream client reconnects with exponential back-off (initial × 2 per failure, capped at 8×).
Heartbeat events are silently dropped.

## Symbol Conventions

### Futures Month Codes

TradeStation uses standard CME month codes:

```
F=Jan  G=Feb  H=Mar  J=Apr  K=May  M=Jun
N=Jul  Q=Aug  U=Sep  V=Oct  X=Nov  Z=Dec
```

Example: `GCJ26` = Gold April 2026.

### Continuous Contracts

TradeStation continuous contract symbols use the `@` prefix (e.g., `@GC` for continuous Gold).
These are for historical data download only — live subscriptions require the specific contract
symbol (e.g., `GCJ26`).

### Option Symbols

TradeStation uses the OCC 21-character format:
`<underlying><space><YYMMDD><C|P><8-digit-strike×1000>`

Example: `AAPL 250321C00175000` = AAPL call, expiry 2025-03-21, strike $175.00.

## Known Limitations

| Limitation | Details |
|---|---|
| No `60-MINUTE` bar spec | `BarSpecification(60, MINUTE)` is rejected by NautilusTrader — use `1-HOUR-LAST` instead |
| FOK orders rejected | TradeStation rejects FOK in most contexts — use `DAY` |
| `reduce_only` ignored | Flag is silently accepted but has no broker-side effect |
| Fill polling latency | Without streaming, fills are detected within 5 seconds (poll interval) |
| Partial fill events (FLP) | Tracked in order status reports; fill reports only include `FLL` (fully filled) orders |
| No options execution | `OptionContract` instruments load correctly; order submission for options is not implemented |
| Shared SSE connection per instrument | Quote tick and trade tick subscriptions share a single SSE connection per instrument via a multiplexer |

## Troubleshooting

### Authentication Fails

**Error:** `TradeStation authentication failed`

1. Verify environment variables are set: `echo $TRADESTATION_CLIENT_ID`
2. Check the refresh token is still valid (rotate via the developer portal if expired)
3. Ensure you're using the correct `use_sandbox` setting for your credentials

### Strategy Never Starts (Portfolio Not Initialized)

EUR-denominated instruments (FDAX, FESX) require a EUR/USD exchange rate at startup.
If the rate is not available, the kernel's portfolio initialization times out.

**Fix in `run_paper_trading.py`:** `_inject_eur_usd_xrate()` fetches the EC contract
close price and injects it as a mark rate. The kernel is also patched to start
strategies regardless of portfolio initialization status.

### Bars Never Arrive

1. Confirm the instrument is in cache before subscribing: pre-load via `instrument_ids` in config
2. Check the bar spec uses `MINUTE` aggregation (not `HOUR`): `"15-MINUTE-LAST"` not `"1-HOUR-LAST"`

### Orders Missing After Restart

On restart, orders submitted in a prior session may not be in the NT cache. The adapter
fetches order history (`get_orders`) during reconciliation to rebuild order status.
Set `reconciliation_lookback_mins=1440` (24 hours) in your `TradingNodeConfig` to maximize coverage.

### SSE Stream Keeps Reconnecting

1. Check network stability and TradeStation API status
2. Review logs for `SSE stream error` messages
3. Adjust `streaming_reconnect_delay_secs` — default 5s doubles on each failure (capped at 40s)
4. If persistent, fall back to polling: set `use_streaming=False`

### Bars Arrive Late with Streaming Enabled

Bar streaming buffers in-progress updates and emits only on bar close (when timestamp
changes). For illiquid instruments, the next tick may arrive seconds after bar close.
This is normal — the bar is emitted as soon as the exchange confirms the bar period ended.

## Resources

- [TradeStation API Docs](https://api.tradestation.com/docs/)
- [Developer Portal](https://developer.tradestation.com/)
- [NautilusTrader Documentation](https://nautilustrader.io/docs/)
