# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Community-contributed TradeStation adapter for [NautilusTrader](https://github.com/nautechsystems/nautilus_trader). Provides live market data (bars, quotes, trades) and order execution (market, limit, stop, OCO/bracket groups) via TradeStation's REST and SSE streaming APIs. Production-tested with 20+ concurrent futures strategies in paper trading. Status: Beta (v0.1.0).

## Development Commands

```bash
# Install from source with dev dependencies
pip install -e ".[dev]"

# Run all tests
pytest tests/ -q

# Run a single test file
pytest tests/test_parsing.py -v

# Run a specific test
pytest tests/test_parsing.py::TestParseQuoteTick::test_parse_quote_tick -v

# Lint
ruff check tradestation_nt_community/ tests/
ruff format --check tradestation_nt_community/ tests/
```

- **Build system:** Hatchling (pyproject.toml only, no setup.py)
- **Python:** 3.11+
- **Test framework:** pytest with `asyncio_mode = "auto"` (async tests run without decorators)
- **Linter:** ruff, line-length 100, target py311
- **No CI/CD, pre-commit hooks, tox, or Makefile configured**

## Architecture

The adapter bridges NautilusTrader's event-driven engine to TradeStation's REST/SSE API. Key design principles:

- **Async throughout** -- all I/O uses `httpx.AsyncClient`; no blocking calls on the event loop
- **Pure parsing layer** -- `parsing/` contains stateless functions that convert between TradeStation API dicts and NautilusTrader domain objects; easy to test in isolation
- **Factory + caching pattern** -- `factories.py` uses `@lru_cache(1)` on `get_cached_tradestation_http_client()` and `get_cached_tradestation_instrument_provider()` to ensure a single HTTP client and provider instance per process

### Data flow

```
TradingNode
  -> TradeStationLiveDataClientFactory.create()
     -> TradeStationDataClient (data.py)
        -> TradeStationHttpClient (http/client.py)      # REST: historical bars, symbol details
        -> TradeStationStreamClient (streaming/client.py) # SSE: live quotes, bars, orders
        -> parsing/data.py                                # dict -> Bar/QuoteTick/TradeTick
        -> TradeStationHistoricalClient (historical/)     # bar-count estimation + fetch
```

Execution follows the same pattern through `TradeStationExecutionClient` -> HTTP client -> `parsing/execution.py`.

### Key modules

| Module | Role |
|--------|------|
| `config.py` | `TradeStationDataClientConfig` / `TradeStationExecClientConfig` -- NautilusTrader config objects |
| `data.py` | `TradeStationDataClient` -- bar/tick subscriptions via polling or SSE streaming |
| `execution.py` | `TradeStationExecutionClient` -- order submit/cancel/modify, OCO/bracket groups, reconciliation |
| `http/client.py` | `TradeStationHttpClient` -- OAuth 2.0 token management, all REST endpoints |
| `streaming/client.py` | `TradeStationStreamClient` -- SSE with exponential-backoff reconnect |
| `parsing/` | Pure functions: `data.py` (bars/ticks), `execution.py` (orders/fills), `instruments.py` (symbol -> Equity/Futures/Option) |
| `providers.py` | `TradeStationInstrumentProvider` -- on-demand instrument loading (load-all not supported) |
| `historical/client.py` | `TradeStationHistoricalClient` -- `estimate_barsback()` converts date ranges to bar counts |

### Data client modes

The data client supports two modes configured via `use_streaming`:
- **HTTP polling (default):** Periodic REST requests at intervals (60s for MINUTE bars, 300s for HOUR/DAY, 1s for quotes)
- **SSE streaming (recommended):** Real-time delivery via Server-Sent Events; quote and trade tick subscriptions share a single SSE connection per instrument

### Order groups

- **OCO:** All orders have `ContingencyType.OCO` -> submitted as `"Type": "OCO"`
- **Bracket:** One OTO + two or more OCO legs -> submitted as `"Type": "BRK"`
- Both submitted atomically to `POST /orderexecution/ordergroups`

## Testing

Tests use a `MockTradeStationHttpClient` (in `tests/mock_http_client.py`) that returns fixture data from `tests/resources/*.json`. No live API calls are needed. Test stubs and factory helpers are in `tests/test_kit.py`.

Core dependencies for the adapter: `nautilus_trader >= 1.200`, `httpx >= 0.27`, `pandas`.

## Known Constraints

- `60-MINUTE` bar spec is invalid in NautilusTrader -- use `1-HOUR`
- TradeStation rejects FOK orders -- use `TimeInForce.DAY`
- `reduce_only` is silently ignored (no broker-side effect)
- Only full fills (FLL) trigger fill reports; no partial fill (FLP) handling
- Options: instrument loading works, but order submission is not implemented
- TradeStation uses SSE (not WebSocket) for streaming
- Instruments should be pre-loaded via `instrument_ids` config for reliable subscriptions
