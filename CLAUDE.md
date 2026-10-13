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

> **Test interpreter:** run the adapter suite with the system `pytest` (`/usr/bin/python3` + `.[dev]` extras) — full green is **352 passed**. Do NOT use the consumer's vendored nautilus venv; it pins a different httpx that fails `tests/test_streaming.py::TestAuthErrorRefresh` (environment artifact, not a regression).

## Observability (June 2026)

Two structured, greppable log lines aid post-incident reconstruction (both additive, no behavior change):
- **`[ORDER-SUBMIT]`** — emitted by `execution._submit_order` for every order we submit, carrying the order attribution (`client_order_id`, `strategy_id`, `instrument_id`, side, type, `tags`, `ts_order_id`) from `_attribution.order_attribution`. TradeStation has no round-tripping tag field, so attribution is kept on our side.
- **`[RECON-SHADOW]`** (observe-only) — emitted by `generate_order_status_reports` for any broker order absent from `_ts_order_id_to_client_order_id` **and** the NT cache: the "adopted/external order" gap where `_check_order_statuses` (the §95 safety poll) double-skips, so a dropped fill is invisible. This **measures** the gap (it does not fix it — no map registration, no behavior change); a later phase uses the soak data to close it.

## Known Constraints

- `60-MINUTE` bar spec is invalid in NautilusTrader -- use `1-HOUR`
- TradeStation rejects FOK orders -- use `TimeInForce.DAY`
- `reduce_only` is silently ignored (no broker-side effect)
- Fills carry the EXECUTED qty (`Legs[*].ExecQuantity` summed, as a delta vs `order.filled_qty`) and ONE price resolver for SSE/poll/FLP/in-flight: AveragePrice/FilledPrice, `Legs[0].ExecutionPrice`, then the order's own stop/limit price (WARNING `[FILL-PRICE-FALLBACK]`); all missing -> ERROR `[FILL-NO-PRICE]`, `_order_last_status` not advanced (next poll retries), an FLP does not cancel its remainder, the in-flight report returns None. Status reports use `ClosedDateTime` for `ts_last`. A PENDING_UPDATE order with a pending amend is not answered in-flight: the 5xx-modify recovery runs inline. Event stamps use `ClosedDateTime`, else now (F3). FLP is terminal: fill for the delta, then OrderCanceled for the remainder (`[FLP-TERMINAL]`); FLP without a qty stays `[FLP-NO-QTY]` (runner pending path). FPR is only tracked: `[PARTIAL-FILL]` WARNING once, `partial_orders()` -> {coid: (exec_qty, since_ns)}, no NT event; cleared on any terminal status.
- F4: `generate_order_status_report` for an own cached order reads the broker (`_inflight_status_report`): open/held codes -> ACCEPTED, FLL -> FILLED, FLP -> PARTIALLY_FILLED, REJ -> REJECTED, CAN/OUT/EXP -> CANCELED/EXPIRED only if `_cancel_was_requested(coid)` (cancel attempts + §111 registry), else ACCEPTED + `[CANCEL-MISMATCH]`; broker read failure or order absent -> None (NT retries; runner sets `inflight_check_retries=20`).
- `DON` ('Queued', Open category) is a session pause for every time-in-force, never a cancel (§149)
- `[UNCACHED-FILL]` WARNING = a mapped TS order filled but NT's cache has no order (placed before a node restart — startup reconciliation rebuilds 0 orders), so the strategy never sees that fill; HEAL CASE-A / PERIODIC-RECON must reconcile it. Fills whose `ClosedDateTime` predates this adapter instance (already delivered by the previous process) log at INFO only (40b9a55).
- Options: instrument loading works, but order submission is not implemented
- TradeStation uses SSE (not WebSocket) for streaming
- Instruments should be pre-loaded via `instrument_ids` config for reliable subscriptions

## Order status categories (§149)

- Source of truth: `tradestation_nt_community/common/order_status.py`, copied verbatim from the TS OpenAPI spec (never import status sets from elsewhere).
- Four categories: Open (incl. ACK, DON, UCN, LAT, BRO), Canceled, Rejected, Filled.
- Held (no NT event; `[TS-STATUS-WATCH]`, then `[TS-STATUS-RECHECK]` for WATCH codes): Open-category codes (incl. DON, UCN, LAT, BRO, RJC), plus the Canceled-category WATCH codes TSC/SCN/RJR/UCH (R24: held, assume live), plus FLP and unknown codes. Orders whose NT status is PENDING_CANCEL are NOT watched (R25: our own cancel is in flight; the UCN echo is expected).
- Acted on as cancels immediately: the remaining Canceled-category codes (CAN, EXP, OUT). The held TSC/SCN/RJR/UCH are closed only by the recheck: when the broker then shows a terminal Canceled status and the NT order is still open, it emits `generate_order_canceled` and registers `_register_cancel_verify(requested=False)` (never re-cancelled); the verdict gains ` - NT order closed`. A broker order with no Status gives `broker status=UNKNOWN -> no Status reported at broker` (§150). The old adapter never logged these codes, so their real frequency is unknown.
- `parse_order_status` also feeds NT reconciliation reports (mass status, in-flight check): Open, unknown and TSC/SCN/RJR/UCH codes map to ACCEPTED (assume live, agreeing with the live paths); other Canceled codes (CAN, OUT) to CANCELED, EXP to EXPIRED.
- UCN = Cancel Sent (Open) -- never a cancel. DON = Queued (Open) -- a pause for every TIF.
- `CSN` (deprecated spec code, "Cancel Sent") is treated exactly like UCN (open, WATCH, CANCEL_PENDING, not CANCELLABLE). `UPD` in `common/enums.py` is not a spec code and is unused. The SSE and poll paths share one status tail, `_apply_status_tail`; `_watch_status` logs `[TS-STATUS-WATCH]` once per parked order. EXP stays a cancel event: the adapter has no `generate_order_expired` path. `_is_own_cached_order` also resolves via `cache.client_order_id(VenueOrderId)` when the ts->coid mapping is missing. `tests/test_order_status.py` checks the sets against the real OpenAPI JSON (`TS_OPENAPI_SPEC` or the sibling strategies repo; skipped if absent).
- `CANCEL_PENDING = {UCN, CSN, LAT}` (`common/order_status.py`): a cancel is already in flight or too late; never re-cancel, never treat as gone. Used by `_verify_pending_cancels`.
- §111 re-cancels only cancels we requested; an unrequested cancel still live at the broker logs `[CANCEL-MISMATCH]` (ERROR) and is not re-cancelled.
- Log tags: `[TS-STATUS-WATCH]`, `[TS-STATUS-RECHECK]` (5-min broker recheck), `[CANCEL-MISMATCH]`, `[CANCEL-STUCK]` (ERROR once per order: a cancel we requested still UCN/LAT after 600 s; never re-cancelled, §150), `[FILL-AFTER-CANCEL]` (broker fill on an order NT closed as CANCELED/EXPIRED; observe-only).
- Do not add status sets elsewhere -- import `common/order_status`.
- Uncached fills and v3 order shape (strategies LESSONS §151/§153): `set_uncached_fill_callback` hands every uncached FLL/FLP (kind live/downtime/unknown) to the runner via `loop.call_soon` (callback must not block the loop); legacy group-less order-map entries are stamped when seen in the own NT cache. The parser reads the TS v3 order shape (`Legs[]`), and `_is_own_cached_order` + `filter_unclaimed_external_orders` keep EXTERNAL orders out of NT reconciliation.
