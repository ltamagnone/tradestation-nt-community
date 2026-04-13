"""
TradeStation data client implementation.
"""

import asyncio
from datetime import datetime

import pandas as pd

from tradestation_nt_community.common.enums import TradeStationBarUnit
from tradestation_nt_community.constants import TRADESTATION_CLIENT_ID
from tradestation_nt_community.constants import TRADESTATION_VENUE
from tradestation_nt_community.historical.client import TradeStationHistoricalClient
from tradestation_nt_community.http.client import TradeStationHttpClient
from tradestation_nt_community.parsing.data import bar_spec_to_ts_params  # noqa: F401
from tradestation_nt_community.parsing.data import parse_bars  # noqa: F401
from tradestation_nt_community.parsing.data import parse_quote_tick
from tradestation_nt_community.parsing.data import parse_trade_tick
from tradestation_nt_community.providers import TradeStationInstrumentProvider
from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import MessageBus
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.data.messages import RequestBars
from nautilus_trader.data.messages import SubscribeBars
from nautilus_trader.data.messages import SubscribeQuoteTicks
from nautilus_trader.data.messages import SubscribeTradeTicks
from nautilus_trader.data.messages import UnsubscribeBars
from nautilus_trader.data.messages import UnsubscribeQuoteTicks
from nautilus_trader.data.messages import UnsubscribeTradeTicks
from nautilus_trader.live.data_client import LiveMarketDataClient
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarSpecification
from nautilus_trader.model.data import BarType
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.enums import BarAggregation
from nautilus_trader.model.identifiers import ClientId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.instruments import Instrument


class TradeStationDataClient(LiveMarketDataClient):
    """
    Provide a data client for TradeStation.

    Parameters
    ----------
    loop : asyncio.AbstractEventLoop
        The event loop for the client.
    client : TradeStationHttpClient
        The TradeStation HTTP client.
    msgbus : MessageBus
        The message bus for the client.
    cache : Cache
        The cache for the client.
    clock : LiveClock
        The clock for the client.
    instrument_provider : TradeStationInstrumentProvider
        The instrument provider.

    """

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        client: TradeStationHttpClient,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
        instrument_provider: TradeStationInstrumentProvider,
        instrument_ids: tuple[str, ...] = (),
        use_streaming: bool = False,
        streaming_reconnect_delay_secs: float = 5.0,
    ) -> None:
        super().__init__(
            loop=loop,
            client_id=ClientId(TRADESTATION_CLIENT_ID.value),
            venue=TRADESTATION_VENUE,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            instrument_provider=instrument_provider,
        )

        self._client = client
        self._historical = TradeStationHistoricalClient(http_client=client)
        self._update_instrument_interval = 60.0 * 60.0  # 1 hour
        self._instrument_ids_to_load = instrument_ids

        # Streaming configuration
        self._use_streaming = use_streaming
        self._stream_client: "TradeStationStreamClient | None" = None
        if use_streaming:
            from tradestation_nt_community.streaming.client import (
                TradeStationStreamClient,
            )
            self._stream_client = TradeStationStreamClient(
                access_token_provider=lambda: self._client.access_token,
                base_url=self._client.base_url,
                reconnect_delay_secs=streaming_reconnect_delay_secs,
            )

        # Bar subscription state
        self._bar_subscriptions: dict[BarType, asyncio.Task] = {}
        self._last_bar_ts: dict[BarType, str] = {}

        # Quote/trade tick state (polling or streaming tasks)
        self._quote_subscriptions: dict[InstrumentId, asyncio.Task] = {}
        self._trade_subscriptions: dict[InstrumentId, asyncio.Task] = {}
        self._last_quote: dict[InstrumentId, tuple[float, float]] = {}  # (bid, ask)
        self._last_trade: dict[InstrumentId, float] = {}  # last price

        # Shared SSE multiplexer: one stream_quotes() connection per instrument,
        # fanning out to both quote and trade tick handlers.
        self._quote_stream_tasks: dict[InstrumentId, asyncio.Task] = {}
        self._quote_stream_wants_quotes: set[InstrumentId] = set()
        self._quote_stream_wants_trades: set[InstrumentId] = set()

    async def _connect(self) -> None:
        self._log.info("Connecting to TradeStation...")
        # HTTP client is already authenticated in constructor

        # Pre-load instruments so they are in cache before on_start() fires.
        # subscribe_bars() is async so it runs after on_start() — without pre-loading,
        # strategies that call cache.instrument() during on_start() get None.
        if self._instrument_ids_to_load:
            ids = [InstrumentId.from_str(s) for s in self._instrument_ids_to_load]
            self._log.info(f"Pre-loading {len(ids)} instruments...")
            await self._instrument_provider.load_ids_async(ids)
            loaded = 0
            for inst_id in ids:
                instrument = self._instrument_provider.find(inst_id)
                if instrument:
                    self._cache.add_instrument(instrument)
                    loaded += 1
                else:
                    self._log.warning(f"Could not pre-load instrument: {inst_id}")
            self._log.info(f"Pre-loaded {loaded}/{len(ids)} instruments into cache")

        self._log.info("Connected to TradeStation")

    async def _disconnect(self) -> None:
        self._log.info("Disconnecting from TradeStation...")

        async def _cancel(tasks: dict) -> None:
            for task in list(tasks.values()):
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            tasks.clear()

        await _cancel(self._bar_subscriptions)
        await _cancel(self._quote_subscriptions)
        await _cancel(self._trade_subscriptions)
        await _cancel(self._quote_stream_tasks)
        self._quote_stream_wants_quotes.clear()
        self._quote_stream_wants_trades.clear()
        self._last_bar_ts.clear()
        self._last_quote.clear()
        self._last_trade.clear()
        await self._client.close()
        self._log.info("Disconnected from TradeStation")

    # -- SUBSCRIPTIONS --------------------------------------------------------

    async def _subscribe_instrument(self, command) -> None:
        """Subscribe to instrument updates (loads instrument if not in cache)."""
        instrument_id = command.instrument_id
        try:
            # Load instrument if not in cache
            if instrument_id not in self._cache.instrument_ids():
                await self._instrument_provider.load_async(instrument_id)

                # Get instrument from provider and add to cache
                instrument = self._instrument_provider.find(instrument_id)
                if instrument:
                    self._cache.add_instrument(instrument)
                    self._handle_data(instrument)
                    self._log.info(f"Loaded and subscribed to instrument: {instrument_id}")
                else:
                    self._log.error(f"Failed to load instrument: {instrument_id}")
            else:
                self._log.info(f"Instrument already in cache: {instrument_id}")
        except Exception as e:
            self._log.error(f"Error subscribing to instrument {instrument_id}: {e}")

    async def _unsubscribe_instrument(self, command) -> None:
        """Unsubscribe from instrument updates."""
        pass  # No-op for HTTP-based client

    async def _subscribe_bars(self, command: SubscribeBars) -> None:
        """Subscribe to bar updates via SSE streaming (preferred) or polling fallback.

        When ``use_streaming=True``, opens an SSE connection to the barcharts
        endpoint. The stream sends in-progress bar updates; we buffer the
        current bar and only emit it when its timestamp changes (= bar closed).

        When streaming is not enabled, falls back to HTTP polling every 60s
        (MINUTE) or 300s (HOUR/DAY).
        """
        bar_type = command.bar_type
        if bar_type in self._bar_subscriptions:
            self._log.warning(f"Already subscribed to {bar_type}")
            return

        # Validate aggregation and get TradeStation parameters
        try:
            interval, unit = self._bar_spec_to_ts_params(bar_type.spec)
        except ValueError as e:
            self._log.error(str(e))
            return

        symbol = bar_type.instrument_id.symbol.value

        # Ensure instrument is in cache (needed for price precision in _parse_bars)
        if bar_type.instrument_id not in self._cache.instrument_ids():
            await self._instrument_provider.load_async(bar_type.instrument_id)
            instrument = self._instrument_provider.find(bar_type.instrument_id)
            if instrument:
                self._cache.add_instrument(instrument)

        instrument = self._cache.instrument(bar_type.instrument_id)
        if not instrument:
            self._log.error(
                f"Cannot subscribe to {bar_type}: instrument not found for "
                f"{bar_type.instrument_id}. Ensure the instrument is loaded first."
            )
            return

        if self._use_streaming and self._stream_client:
            task = self._loop.create_task(
                self._stream_bars(bar_type, symbol, interval, unit, instrument)
            )
            self._bar_subscriptions[bar_type] = task
            self._log.info(
                f"Subscribed to {bar_type} (SSE streaming, "
                f"TS params: interval={interval} unit={unit.value})"
            )
        else:
            # Polling fallback
            if bar_type.spec.aggregation == BarAggregation.MINUTE:
                poll_secs = 60.0
            else:
                poll_secs = 300.0

            task = self._loop.create_task(
                self._poll_bars(bar_type, symbol, interval, unit, instrument, poll_secs)
            )
            self._bar_subscriptions[bar_type] = task
            self._log.info(
                f"Subscribed to {bar_type} (polling every {poll_secs:.0f}s, "
                f"TS params: interval={interval} unit={unit.value})"
            )

    async def _stream_bars(
        self,
        bar_type: BarType,
        symbol: str,
        interval: str,
        unit: TradeStationBarUnit,
        instrument: Instrument,
    ) -> None:
        """SSE streaming loop for bars.

        The TradeStation barcharts SSE endpoint sends partial (in-progress) bar
        updates during the bar period, then a final update when the bar closes
        and the next bar's first tick arrives (the timestamp changes).

        We buffer each event and only emit the bar when we see a *new* timestamp,
        meaning the previous bar has closed. This matches the polling behaviour
        where strategies receive completed bars.

        Reconnection recovery
        ---------------------
        On reconnect, TradeStation sends a ``Status=Historical`` seed bar
        containing the last *completed* bar with accurate OHLCV.  We use it to:

        * **Correct the buffer** — if the seed timestamp matches the buffered
          bar, it replaces the (possibly stale) OHLCV values.
        * **Ignore stale seed** — if the seed timestamp is *older* than the
          buffer, it means we reconnected within the same bar period.  The
          seed is a bar already emitted; the buffer is still in-progress.
        * **Fill a gap** — if the seed timestamp is *newer* than the buffer,
          the buffered bar is emitted first, then the seed bar is emitted as
          a completed bar that was missed during the outage.
        """
        self._log.info(f"Bar SSE stream started for {bar_type}")
        buffered_event: dict | None = None
        buffered_ts: str = ""
        initialized = False  # True after first RealTime event processed

        try:
            async for event in self._stream_client.stream_bars(
                symbol=symbol,
                interval=interval,
                unit=unit.value,
            ):
                event_ts = event.get("TimeStamp", "")
                if not event_ts:
                    continue

                is_historical = event.get("Status") == "Historical"

                if is_historical:
                    if not initialized:
                        # Initial connection seed bar — skip it; the buffer
                        # is empty and we don't want to emit a potentially
                        # stale bar before live data starts flowing.
                        self._log.debug(
                            f"Bar stream skipping initial seed for {bar_type}: "
                            f"ts={event_ts}"
                        )
                        continue

                    # Reconnection seed bar — use it for gap recovery.
                    if event_ts == buffered_ts:
                        # Same bar period as buffer → replace with accurate
                        # close values from the completed bar.
                        buffered_event = event
                        self._log.debug(
                            f"Bar stream corrected buffer for {bar_type}: "
                            f"ts={event_ts}"
                        )
                    elif event_ts < buffered_ts:
                        # Seed is OLDER than the buffered bar — we reconnected
                        # within the same bar period.  The seed is a bar we
                        # already emitted before.  Ignore it and keep buffering.
                        self._log.debug(
                            f"Bar stream ignoring stale seed for {bar_type}: "
                            f"seed_ts={event_ts} < buffered_ts={buffered_ts}"
                        )
                    else:
                        # Seed is NEWER → the buffered bar closed during the
                        # outage and this seed bar is the completed version
                        # of a bar we missed.  Emit both in chronological order.
                        if buffered_event:
                            bars = self._parse_bars(
                                [buffered_event], bar_type, instrument,
                            )
                            for bar in bars:
                                self._handle_data(bar)
                            self._log.debug(
                                f"Bar emitted (pre-gap) for {bar_type}: "
                                f"ts={buffered_ts}"
                            )
                        # Emit the seed bar (accurately closed during gap)
                        bars = self._parse_bars(
                            [event], bar_type, instrument,
                        )
                        for bar in bars:
                            self._handle_data(bar)
                        self._log.info(
                            f"Bar gap recovery for {bar_type}: emitted seed "
                            f"ts={event_ts}"
                        )
                        # Reset buffer — the next RealTime event will start
                        # a fresh buffer for the currently-forming bar.
                        buffered_ts = ""
                        buffered_event = None
                    continue

                # --- RealTime event ---

                if not buffered_ts:
                    # First RealTime event (or first after gap recovery reset).
                    buffered_ts = event_ts
                    buffered_event = event
                    initialized = True
                    self._log.debug(
                        f"Bar stream initialised for {bar_type}: ts={event_ts}"
                    )
                    continue

                if event_ts != buffered_ts:
                    # Timestamp changed → the buffered bar is now closed.
                    if buffered_event:
                        bars = self._parse_bars(
                            [buffered_event], bar_type, instrument,
                        )
                        for bar in bars:
                            self._handle_data(bar)
                        self._log.debug(
                            f"Bar emitted for {bar_type}: ts={buffered_ts}"
                        )
                    # Start buffering the new bar
                    buffered_ts = event_ts
                    buffered_event = event
                else:
                    # Same timestamp — update the buffer with latest OHLCV
                    buffered_event = event

        except asyncio.CancelledError:
            self._log.info(f"Bar SSE stream stopped for {bar_type}")

    async def _poll_bars(
        self,
        bar_type: BarType,
        symbol: str,
        interval: str,
        unit: TradeStationBarUnit,
        instrument: Instrument,
        poll_secs: float,
    ) -> None:
        """Background polling loop — fetches last completed bar every poll_secs seconds.

        On first poll, initialises _last_bar_ts so subsequent polls only push new bars.
        Recovers from transient API errors without stopping.
        """
        self._log.info(f"Bar polling loop started for {bar_type}")
        first_poll = True

        while True:
            try:
                await asyncio.sleep(poll_secs)

                raw_bars = await self._client.get_bars(
                    symbol=symbol,
                    interval=interval,
                    unit=unit,
                    barsback=1,
                )

                if not raw_bars:
                    continue

                latest_ts = raw_bars[-1].get("TimeStamp", "")

                if first_poll:
                    # Record the current bar timestamp so we only push bars that arrive *after*
                    # subscription. This avoids replaying a stale bar immediately on startup.
                    self._last_bar_ts[bar_type] = latest_ts
                    first_poll = False
                    self._log.debug(
                        f"Bar polling initialised for {bar_type}: last_ts={latest_ts}"
                    )
                    continue

                last_seen = self._last_bar_ts.get(bar_type, "")

                if latest_ts != last_seen:
                    self._last_bar_ts[bar_type] = latest_ts
                    bars = self._parse_bars(raw_bars, bar_type, instrument)
                    for bar in bars:
                        self._handle_data(bar)
                    self._log.debug(f"New bar pushed for {bar_type}: ts={latest_ts}")

            except asyncio.CancelledError:
                self._log.info(f"Bar polling loop stopped for {bar_type}")
                break
            except Exception as e:
                self._log.error(f"Error in bar polling loop for {bar_type}: {e}")
                await asyncio.sleep(5.0)  # Brief back-off before retrying

    async def _unsubscribe_bars(self, command: UnsubscribeBars) -> None:
        """Cancel the bar polling/streaming task for this bar type."""
        bar_type = command.bar_type
        task = self._bar_subscriptions.pop(bar_type, None)
        if task:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._last_bar_ts.pop(bar_type, None)
        self._log.info(f"Unsubscribed from {bar_type}")

    # -- QUOTE / TRADE TICK SUBSCRIPTIONS (shared multiplexer) -----------------

    async def _subscribe_quote_ticks(self, command: SubscribeQuoteTicks) -> None:
        instrument_id = command.instrument_id
        if instrument_id in self._quote_subscriptions:
            return
        instrument = self._cache.instrument(instrument_id)
        if not instrument:
            self._log.error(
                f"Cannot subscribe to quote ticks for {instrument_id}: "
                "instrument not in cache. Ensure bars are subscribed first."
            )
            return
        if self._use_streaming and self._stream_client:
            self._quote_stream_wants_quotes.add(instrument_id)
            self._ensure_quote_stream(instrument_id, instrument)
            mode = "SSE streaming (shared)"
        else:
            task = self._loop.create_task(self._poll_quotes(instrument_id, instrument))
            self._quote_subscriptions[instrument_id] = task
            mode = "polling every 1s"
        self._log.info(f"Subscribed to quote ticks for {instrument_id} ({mode})")

    async def _unsubscribe_quote_ticks(self, command: UnsubscribeQuoteTicks) -> None:
        instrument_id = command.instrument_id
        # Polling mode: cancel the dedicated task
        task = self._quote_subscriptions.pop(instrument_id, None)
        if task:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        # Streaming mode: remove interest; stop stream if no one else needs it
        self._quote_stream_wants_quotes.discard(instrument_id)
        self._maybe_stop_quote_stream(instrument_id)
        self._last_quote.pop(instrument_id, None)
        self._log.info(f"Unsubscribed from quote ticks for {instrument_id}")

    async def _subscribe_trade_ticks(self, command: SubscribeTradeTicks) -> None:
        instrument_id = command.instrument_id
        if instrument_id in self._trade_subscriptions:
            return
        instrument = self._cache.instrument(instrument_id)
        if not instrument:
            self._log.error(
                f"Cannot subscribe to trade ticks for {instrument_id}: "
                "instrument not in cache. Ensure bars are subscribed first."
            )
            return
        if self._use_streaming and self._stream_client:
            self._quote_stream_wants_trades.add(instrument_id)
            self._ensure_quote_stream(instrument_id, instrument)
            mode = "SSE streaming (shared)"
        else:
            task = self._loop.create_task(self._poll_trades(instrument_id, instrument))
            self._trade_subscriptions[instrument_id] = task
            mode = "polling every 1s"
        self._log.info(f"Subscribed to trade ticks for {instrument_id} ({mode})")

    async def _unsubscribe_trade_ticks(self, command: UnsubscribeTradeTicks) -> None:
        instrument_id = command.instrument_id
        # Polling mode: cancel the dedicated task
        task = self._trade_subscriptions.pop(instrument_id, None)
        if task:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        # Streaming mode: remove interest; stop stream if no one else needs it
        self._quote_stream_wants_trades.discard(instrument_id)
        self._maybe_stop_quote_stream(instrument_id)
        self._last_trade.pop(instrument_id, None)
        self._log.info(f"Unsubscribed from trade ticks for {instrument_id}")

    def _ensure_quote_stream(
        self, instrument_id: InstrumentId, instrument: Instrument
    ) -> None:
        """Start the shared SSE quote stream for *instrument_id* if not already running."""
        if instrument_id in self._quote_stream_tasks:
            return  # already streaming
        task = self._loop.create_task(
            self._stream_quote_mux(instrument_id, instrument)
        )
        self._quote_stream_tasks[instrument_id] = task

    def _maybe_stop_quote_stream(self, instrument_id: InstrumentId) -> None:
        """Stop the shared SSE stream when no subscriber needs it any more."""
        if (
            instrument_id not in self._quote_stream_wants_quotes
            and instrument_id not in self._quote_stream_wants_trades
        ):
            task = self._quote_stream_tasks.pop(instrument_id, None)
            if task:
                task.cancel()

    async def _stream_quote_mux(
        self, instrument_id: InstrumentId, instrument: Instrument
    ) -> None:
        """Single SSE connection that fans out to both QuoteTick and TradeTick handlers.

        One ``stream_quotes()`` connection per instrument replaces the previous
        pattern of opening two separate connections (one for quotes, one for
        trades). Each incoming event is dispatched to the quote handler if
        ``instrument_id`` is in ``_quote_stream_wants_quotes``, and to the trade
        handler if it is in ``_quote_stream_wants_trades``.
        """
        symbol = instrument_id.symbol.value
        self._log.info(f"Shared quote/trade SSE stream started for {instrument_id}")
        try:
            async for event in self._stream_client.stream_quotes(symbol):
                # --- Quote tick fan-out ---
                if instrument_id in self._quote_stream_wants_quotes:
                    bid = float(event.get("Bid") or 0)
                    ask = float(event.get("Ask") or 0)
                    if bid > 0 and ask > 0:
                        last_bid, last_ask = self._last_quote.get(
                            instrument_id, (0.0, 0.0)
                        )
                        if bid != last_bid or ask != last_ask:
                            self._last_quote[instrument_id] = (bid, ask)
                            tick = parse_quote_tick(event, instrument_id, instrument)
                            if tick:
                                self._handle_data(tick)

                # --- Trade tick fan-out ---
                if instrument_id in self._quote_stream_wants_trades:
                    last_px = float(event.get("Last") or 0)
                    if last_px > 0 and last_px != self._last_trade.get(
                        instrument_id, 0.0
                    ):
                        self._last_trade[instrument_id] = last_px
                        tick = parse_trade_tick(event, instrument_id, instrument)
                        if tick:
                            self._handle_data(tick)
        except asyncio.CancelledError:
            self._log.info(
                f"Shared quote/trade SSE stream stopped for {instrument_id}"
            )

    # -- POLLING FALLBACKS (used when streaming is disabled) -------------------

    async def _poll_quotes(self, instrument_id: InstrumentId, instrument: Instrument) -> None:
        """Background loop: fetch bid/ask every second, emit QuoteTick when changed."""
        symbol = instrument_id.symbol.value
        self._log.info(f"Quote polling loop started for {instrument_id}")
        while True:
            try:
                await asyncio.sleep(1.0)
                raw_quotes = await self._client.get_quotes(symbol)
                if not raw_quotes:
                    continue
                raw = raw_quotes[0]
                bid = float(raw.get("Bid") or 0)
                ask = float(raw.get("Ask") or 0)
                if bid == 0 or ask == 0:
                    continue
                last_bid, last_ask = self._last_quote.get(instrument_id, (0.0, 0.0))
                if bid != last_bid or ask != last_ask:
                    self._last_quote[instrument_id] = (bid, ask)
                    tick = parse_quote_tick(raw, instrument_id, instrument)
                    if tick:
                        self._handle_data(tick)
            except asyncio.CancelledError:
                self._log.info(f"Quote polling loop stopped for {instrument_id}")
                break
            except Exception as e:
                self._log.error(f"Error in quote polling loop for {instrument_id}: {e}")
                await asyncio.sleep(5.0)

    async def _poll_trades(self, instrument_id: InstrumentId, instrument: Instrument) -> None:
        """Background loop: fetch last trade every second, emit TradeTick when changed."""
        symbol = instrument_id.symbol.value
        self._log.info(f"Trade polling loop started for {instrument_id}")
        while True:
            try:
                await asyncio.sleep(1.0)
                raw_quotes = await self._client.get_quotes(symbol)
                if not raw_quotes:
                    continue
                raw = raw_quotes[0]
                last_px = float(raw.get("Last") or 0)
                if last_px == 0:
                    continue
                if last_px != self._last_trade.get(instrument_id, 0.0):
                    self._last_trade[instrument_id] = last_px
                    tick = parse_trade_tick(raw, instrument_id, instrument)
                    if tick:
                        self._handle_data(tick)
            except asyncio.CancelledError:
                self._log.info(f"Trade polling loop stopped for {instrument_id}")
                break
            except Exception as e:
                self._log.error(f"Error in trade polling loop for {instrument_id}: {e}")
                await asyncio.sleep(5.0)

    # -- REQUESTS -------------------------------------------------------------

    async def _request_instrument(
        self,
        instrument_id: InstrumentId,
        correlation_id: UUID4,
        start: pd.Timestamp | None = None,
        end: pd.Timestamp | None = None,
    ) -> None:
        # Load instrument if not in cache
        if instrument_id not in self._cache.instrument_ids():
            await self._instrument_provider.load_async(instrument_id)

            # Get instrument from provider and add to cache
            instrument = self._instrument_provider.find(instrument_id)
            if instrument:
                self._cache.add_instrument(instrument)

        # Get from cache
        instrument = self._cache.instrument(instrument_id)

        if instrument:
            self._handle_data(instrument)
        else:
            self._log.error(f"Failed to load instrument: {instrument_id}")

    async def _request_bars(self, request: RequestBars) -> None:
        bar_type = request.bar_type

        # Ensure instrument is in cache so the historical client can parse prices
        instrument = self._cache.instrument(bar_type.instrument_id)
        if not instrument:
            await self._instrument_provider.load_async(bar_type.instrument_id)
            instrument = self._cache.instrument(bar_type.instrument_id)

        if not instrument:
            self._log.error(
                f"Cannot request bars: instrument not found for {bar_type.instrument_id}"
            )
            return

        start_ts = pd.Timestamp(request.start) if request.start is not None else None

        bars = await self._historical.get_bars(
            bar_type=bar_type,
            instrument=instrument,
            start=start_ts,
            limit=request.limit,
        )

        if not bars:
            return

        self._handle_bars(
            bar_type,
            bars,
            request.id,
            request.start,
            request.end,
            request.params,
        )

    # -- HELPERS --------------------------------------------------------------

    def _bar_spec_to_ts_params(
        self, spec: BarSpecification
    ) -> tuple[str, TradeStationBarUnit]:
        """Map a NautilusTrader BarSpecification to TradeStation interval and unit."""
        return bar_spec_to_ts_params(spec)

    def _parse_bars(
        self,
        raw_bars: list[dict],
        bar_type: BarType,
        instrument: Instrument,
    ) -> list[Bar]:
        return parse_bars(raw_bars, bar_type)
