"""
TradeStation SSE streaming client.

TradeStation uses HTTP Server-Sent Events (SSE) rather than WebSocket for
real-time data. Each streaming endpoint returns a long-lived HTTP GET whose
body is a sequence of newline-delimited JSON objects.

Endpoints
---------
- GET /v3/marketdata/stream/quotes/{symbols}       → real-time quotes
- GET /v3/marketdata/stream/barcharts/{symbol}     → real-time bars
- GET /v3/brokerage/stream/accounts/{account}/orders → order status changes
- GET /v3/marketdata/stream/marketdepth/{symbol}   → level-2 order book

Usage
-----
::

    stream_client = TradeStationStreamClient(
        access_token_provider=lambda: http_client.access_token,
        base_url="https://sim-api.tradestation.com/v3",
        reconnect_delay_secs=5.0,
    )

    async for event in stream_client.stream_quotes("GCJ26"):
        print(event)  # dict with Bid, Ask, Last, etc.

    async for event in stream_client.stream_orders("SIM0000001F"):
        print(event)  # dict with OrderID, Status, etc.

"""
import asyncio
import json
import logging
from collections.abc import AsyncIterator
from typing import Callable

import httpx


_log = logging.getLogger(__name__)

# Heartbeat lines that TradeStation sends to keep the connection alive.
_HEARTBEAT_KEYS = {"Heartbeat", "heartbeat"}


class TradeStationStreamClient:
    """
    Async SSE client for TradeStation streaming endpoints.

    Wraps httpx's async streaming API. Parses newline-delimited JSON events,
    silently drops heartbeats, and reconnects after errors with exponential
    back-off (capped at ``reconnect_delay_secs * 8``).

    Parameters
    ----------
    access_token_provider : Callable[[], str | None]
        Zero-argument callable that returns the current OAuth access token.
        Called on each (re)connection so tokens refresh transparently.
    base_url : str
        TradeStation API base URL (sandbox or production).
    reconnect_delay_secs : float, default 5.0
        Initial delay before reconnecting after a stream error. Doubles on
        each successive failure, capped at 8× the initial value.

    """

    def __init__(
        self,
        access_token_provider: Callable[[], str | None],
        base_url: str,
        reconnect_delay_secs: float = 5.0,
    ) -> None:
        self._token_provider = access_token_provider
        self._base_url = base_url.rstrip("/")
        self._reconnect_delay = reconnect_delay_secs
        self._max_delay = reconnect_delay_secs * 8

    def _headers(self) -> dict[str, str]:
        token = self._token_provider()
        return {
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.tradestation.streams.v2+json",
        }

    async def _stream(
        self, url: str, params: dict[str, str] | None = None,
    ) -> AsyncIterator[dict]:
        """Core SSE reader — yields parsed JSON dicts, reconnects on error.

        After the *first* successful connection, emits ``{"_reconnected": True}``
        at the start of every subsequent connection so callers can run a catch-up
        HTTP poll to recover events that were lost during the gap.

        A 90-second read timeout is applied at the httpx transport level. If no
        bytes arrive (including heartbeats) within that window, the connection is
        assumed to be a zombie and is closed immediately — no backoff sleep.
        The reconnect sentinel is then emitted so the catch-up poll fires normally.
        """
        delay = self._reconnect_delay
        first_connect = True
        while True:
            try:
                async with httpx.AsyncClient(
                    timeout=httpx.Timeout(connect=30.0, read=90.0, write=None, pool=30.0),
                ) as client:
                    async with client.stream(
                        "GET", url, headers=self._headers(), params=params,
                    ) as resp:
                        if resp.status_code != 200:
                            body = await resp.aread()
                            _log.error(
                                f"SSE stream {url} returned {resp.status_code}: "
                                f"{body.decode(errors='replace')[:200]}"
                            )
                            await asyncio.sleep(delay)
                            delay = min(delay * 2, self._max_delay)
                            continue

                        _log.info(f"SSE stream connected: {url}")
                        delay = self._reconnect_delay  # reset on successful connect

                        if not first_connect:
                            # Signal the caller that a reconnect occurred so it can
                            # perform a one-shot HTTP poll to recover missed events.
                            yield {"_reconnected": True}
                        first_connect = False

                        async for line in resp.aiter_lines():
                            line = line.strip()
                            if not line:
                                continue
                            try:
                                event = json.loads(line)
                            except json.JSONDecodeError:
                                _log.debug(f"Non-JSON SSE line (skipped): {line[:120]}")
                                continue

                            # Drop heartbeats silently
                            if any(k in event for k in _HEARTBEAT_KEYS):
                                continue

                            yield event

            except asyncio.CancelledError:
                _log.info(f"SSE stream cancelled: {url}")
                return
            except httpx.ReadTimeout:
                _log.warning(
                    f"SSE stream no data for 90s — zombie connection detected, "
                    f"forcing reconnect: {url}"
                )
            except Exception as e:
                _log.error(f"SSE stream error ({url}): {e} — reconnecting in {delay:.0f}s")
                await asyncio.sleep(delay)
                delay = min(delay * 2, self._max_delay)

    async def stream_quotes(self, symbols: str) -> AsyncIterator[dict]:
        """
        Stream real-time quotes for one or more symbols.

        Parameters
        ----------
        symbols : str
            Symbol or comma-separated list (e.g. ``"GCJ26"`` or ``"GCJ26,ESM26"``).

        Yields
        ------
        dict
            Quote event with keys: Symbol, Bid, Ask, BidSize, AskSize,
            Last, LastSize, Volume, TradeTime, etc.

        """
        url = f"{self._base_url}/marketdata/stream/quotes/{symbols}"
        async for event in self._stream(url):
            yield event

    async def stream_bars(
        self,
        symbol: str,
        interval: str,
        unit: str,
        session_template: str | None = None,
    ) -> AsyncIterator[dict]:
        """
        Stream real-time bar updates for a symbol.

        Parameters
        ----------
        symbol : str
            The contract symbol (e.g. ``"GCJ26"``).
        interval : str
            Bar interval string (e.g. ``"1"``, ``"5"``, ``"15"``).
        unit : str
            Bar unit: ``"Minute"`` or ``"Daily"``.
        session_template : str, optional
            TS session template. Use ``"USEQPreAndPost"`` for extended hours
            equity bars. Default ``None`` uses TS default (RTH only for equities).

        Yields
        ------
        dict
            Bar event with keys: Open, High, Low, Close, TotalVolume,
            TimeStamp, Status (``"Historical"`` or ``"RealTime"``).

        """
        url = f"{self._base_url}/marketdata/stream/barcharts/{symbol}"
        params: dict[str, str] = {"interval": interval, "unit": unit, "barsback": "1"}
        if session_template:
            params["sessiontemplate"] = session_template
        async for event in self._stream(url, params=params):
            # Pass all events through — including Historical seed bars.
            # The caller (_stream_bars) uses them for reconnection gap recovery.
            yield event

    async def stream_orders(self, account_id: str) -> AsyncIterator[dict]:
        """
        Stream real-time order status changes for an account.

        Parameters
        ----------
        account_id : str
            The TradeStation account ID (e.g. ``"SIM0000001F"``).

        Yields
        ------
        dict
            Order event with keys: OrderID, Status, FilledQuantity,
            AveragePrice, TradeAction, etc.

        """
        url = f"{self._base_url}/brokerage/stream/accounts/{account_id}/orders"
        async for event in self._stream(url):
            yield event

    async def stream_market_depth(self, symbol: str) -> AsyncIterator[dict]:
        """
        Stream level-2 market depth (order book) for a symbol.

        Parameters
        ----------
        symbol : str
            The contract symbol (e.g. ``"GCJ26"``).

        Yields
        ------
        dict
            Market depth event with keys: Side (``"Bid"``/``"Ask"``),
            Price, Size, OrderCount, etc.

        """
        url = f"{self._base_url}/marketdata/stream/marketdepth/{symbol}"
        async for event in self._stream(url):
            yield event
