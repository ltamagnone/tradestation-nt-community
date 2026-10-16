"""
TradeStation data client implementation.
"""

import asyncio
from datetime import datetime

import pandas as pd

from tradestation_nt_community.bar_gate import BarCloseGate
from tradestation_nt_community.bar_gate import bar_end_ns
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


_BAR_AGG_SECS = {
    BarAggregation.MINUTE: 60,
    BarAggregation.HOUR: 3600,
    BarAggregation.DAY: 86400,
}

# Bars requested on every bar-stream (re)connect. The seed's closed bars newer than the
# watermark are replayed, so a reconnect gap of up to 9 bars loses nothing (strategies
# LESSONS §207). Older seed bars are dropped by the gate, so the extra history is harmless.
_STREAM_BARSBACK = 10


def _bar_interval_secs(bar_type) -> int | None:
    """Bar interval in seconds (step x MINUTE/HOUR/DAY); None if unknown/unparseable."""
    try:
        spec = bar_type.spec
        unit = _BAR_AGG_SECS.get(spec.aggregation)
        if unit is None or spec.step <= 0:
            return None
        return int(spec.step) * unit
    except Exception:
        return None


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
        extended_hours: bool = False,
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
        self._extended_hours = extended_hours
        self._stream_client: "TradeStationStreamClient | None" = None
        if use_streaming:
            from tradestation_nt_community.streaming.client import (
                TradeStationStreamClient,
            )
            self._stream_client = TradeStationStreamClient(
                access_token_provider=lambda: self._client.access_token,
                base_url=self._client.base_url,
                reconnect_delay_secs=streaming_reconnect_delay_secs,
                on_auth_error=self._client._ensure_authenticated,
            )

        # Bar subscription state
        self._bar_subscriptions: dict[BarType, asyncio.Task] = {}
        self._last_bar_ts: dict[BarType, str] = {}
        # One BarCloseGate per bar type; never cleared, so its watermark survives reconnects
        # and watchdog task restarts (strategies LESSONS §207).
        self._bar_gates: dict[BarType, BarCloseGate] = {}

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
        endpoint. Bars are emitted when TradeStation marks them ``BarStatus`` Closed
        (see ``_stream_bars``).

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

        TradeStation v3 bar events carry ``BarStatus`` ("Open" while forming, "Closed" once
        final - sent ~0.1 s after the boundary, no trade needed), ``IsRealtime`` (False for
        the history seed sent on every connect) and ``TimeStamp`` = the bar's END. The bar
        type's ``BarCloseGate`` decides what is emitted: closed bars once, seeds and repeats
        never, bars that closed during a reconnect gap from the seed (strategies LESSONS §207).
        """
        self._log.info(f"Bar SSE stream started for {bar_type}")
        gate = self._bar_gates.setdefault(bar_type, BarCloseGate())
        gate.reset_connection()

        try:
            # When extended_hours is enabled and the instrument is an equity,
            # use USEQPreAndPost to receive pre-market and after-hours bars.
            from nautilus_trader.model.instruments import Equity
            session_tpl = (
                "USEQPreAndPost"
                if self._extended_hours and isinstance(instrument, Equity)
                else None
            )

            async for event in self._stream_client.stream_bars(
                symbol=symbol,
                interval=interval,
                unit=unit.value,
                session_template=session_tpl,
                barsback=_STREAM_BARSBACK,
            ):
                if event.get("_reconnected"):
                    gate.reset_connection()
                    continue
                self._emit_gated(gate, event, bar_type, instrument)

        except asyncio.CancelledError:
            self._log.info(f"Bar SSE stream stopped for {bar_type}")

    def _emit_gated(
        self,
        gate: BarCloseGate,
        event: dict,
        bar_type: BarType,
        instrument: Instrument,
    ) -> None:
        """Feed one TradeStation bar event through ``gate`` and emit what it releases."""
        ts_ns = bar_end_ns(event.get("TimeStamp", ""))
        if ts_ns is None:
            return
        for raw in gate.on_event(event, ts_ns):
            if raw.get("BarStatus") != "Closed":
                self._log.info(
                    f"[BAR-CLOSE-FALLBACK] {bar_type} ts={raw.get('TimeStamp')}: no Closed "
                    f"event - emitted on the next bar's first update"
                )
            for bar in self._parse_bars([raw], bar_type, instrument):
                self._handle_data(bar)
                self._last_bar_ts[bar_type] = datetime.utcnow().isoformat() + "Z"  # §70
            self._log.debug(f"Bar emitted for {bar_type}: ts={raw.get('TimeStamp')}")

    async def _poll_bars(
        self,
        bar_type: BarType,
        symbol: str,
        interval: str,
        unit: TradeStationBarUnit,
        instrument: Instrument,
        poll_secs: float,
    ) -> None:
        """Background polling loop: every ``poll_secs`` read the last two bars and emit each
        bar once it is closed, through the same ``BarCloseGate`` as the stream. The first
        poll only sets the watermark, so no stale bar is replayed on startup. Recovers from
        transient API errors without stopping.
        """
        self._log.info(f"Bar polling loop started for {bar_type}")
        gate = self._bar_gates.setdefault(bar_type, BarCloseGate())
        gate.reset_connection()
        first_poll = True

        while True:
            try:
                await asyncio.sleep(poll_secs)

                raw_bars = await self._client.get_bars(
                    symbol=symbol,
                    interval=interval,
                    unit=unit,
                    barsback=2,
                )
                for raw in raw_bars or []:
                    self._emit_gated(gate, raw, bar_type, instrument)
                if first_poll:
                    gate.reset_connection()  # watermark set: closed bars are live from now
                    first_poll = False

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

    async def reconnect_bar_streams(
        self,
        stale_only: bool = True,
        max_age_secs: float = 7200,
        bar_types=None,
    ) -> int:
        """Cancel and restart bar SSE streams.

        Parameters
        ----------
        stale_only : bool, default True
            If True, only reconnect streams that haven't received data within
            their threshold: ``max(max_age_secs, 2 x bar interval)`` per
            subscription, so an hourly stream 45 min after its last bar is not
            torn down by a 30-min pass (unknown interval → ``max_age_secs``).
            A stream with no recorded bar timestamp is always reconnected.
            If False, reconnect ALL bar streams.
        max_age_secs : float, default 7200
            Minimum age threshold (seconds) for considering a stream stale.
        bar_types : Iterable[BarType], optional
            If given, only these subscriptions are considered (still subject to
            ``stale_only`` and their threshold); every other stream is left
            untouched. An empty iterable reconnects nothing. ``None`` (default)
            considers all bar subscriptions.

        Returns
        -------
        int
            Number of streams reconnected.
        """
        if not self._use_streaming or not self._stream_client:
            return 0

        import time as _time
        now = _time.time()
        reconnected = 0

        # Refresh OAuth token once before the loop — maintenance window may have
        # let the token near expiry before streams are reconnected. (§70)
        try:
            await self._client._ensure_authenticated()
        except Exception as e:
            self._log.warning(f"Token refresh before stream reconnect failed: {e}")

        wanted = None if bar_types is None else set(bar_types)
        for bar_type, task in list(self._bar_subscriptions.items()):
            if wanted is not None and bar_type not in wanted:
                continue
            if stale_only:
                last_ts = self._last_bar_ts.get(bar_type)
                if last_ts:
                    try:
                        from datetime import datetime as _dt
                        last_dt = _dt.fromisoformat(last_ts.replace("Z", "+00:00"))
                        age = now - last_dt.timestamp()
                        interval = _bar_interval_secs(bar_type)
                        threshold = (
                            max(max_age_secs, 2 * interval) if interval else max_age_secs
                        )
                        if age < threshold:
                            continue  # genuinely fresh — skip
                    except Exception:
                        pass
                # No timestamp → can't confirm freshness → fall through to reconnect (§70)

            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass

            self._last_bar_ts.pop(bar_type, None)

            from nautilus_trader.model.data import BarSpecification
            from nautilus_trader.model.enums import BarAggregation
            spec = bar_type.spec
            symbol = bar_type.instrument_id.symbol.value
            if spec.aggregation == BarAggregation.HOUR:
                interval = str(spec.step * 60)
                unit = TradeStationBarUnit.MINUTE
            elif spec.aggregation == BarAggregation.DAY:
                interval = str(spec.step)
                unit = TradeStationBarUnit.DAILY
            else:
                interval = str(spec.step)
                unit = TradeStationBarUnit.MINUTE

            instrument = self._cache.instrument(bar_type.instrument_id)
            if not instrument:
                self._log.warning(f"Cannot reconnect {bar_type}: instrument not in cache")
                continue

            new_task = self._loop.create_task(
                self._stream_bars(bar_type, symbol, interval, unit, instrument)
            )
            self._bar_subscriptions[bar_type] = new_task
            reconnected += 1
            self._log.warning(f"Reconnected bar stream: {bar_type}")

        return reconnected

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

        # The history holds only closed bars (historical client): the stream must not
        # re-emit them (strategies LESSONS §207).
        self._bar_gates.setdefault(bar_type, BarCloseGate()).seed_from_history(
            bars[-1].ts_event
        )

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
