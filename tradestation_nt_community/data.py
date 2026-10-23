"""
TradeStation data client implementation.
"""

import asyncio
from datetime import datetime

import pandas as pd

from tradestation_nt_community.bar_gate import BarCloseGate
from tradestation_nt_community.bar_gate import bar_end_ns
from tradestation_nt_community.bar_gate import ohlcv
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

# Late thin-hour bars (plan 2026-10-23, ``bar_release_mode``). In thin hours TradeStation closes
# a stream bar only with the next trade, can merge the next period's trades into it, and can
# then never send the next bar. "shadow" measures what a release / backfill would do (log only,
# emissions unchanged), "on" does it, "off" is the old path. Never for DAY bars, never polling.
_CLOSE_GRACE_NS = 10_000_000_000  # no Closed by end + 10 s: release the bar as it stood
_SNAPSHOT_OFFSETS_NS = (0, 2_000_000_000, 10_000_000_000, 30_000_000_000)  # [BAR-SNAPSHOT]
_BACKFILL_DELAY_S = 120.0  # mode on: REST read at missing_end + this ...
_BACKFILL_RETRY_S = 60.0  # ... and once more after this if a missing bar is still Open
_BACKFILL_MAX_PERIODS = 3  # a longer gap is a session break or an outage, not a hole
_BACKFILL_SHADOW_DELAYS_S = (60.0, 120.0, 180.0, 300.0)  # shadow: measure when REST has it
_VERIFY_DELAY_S = 120.0  # [BAR-RELEASE-DIVERGED]: REST read at bar end + this
_BAR_RELEASE_MODES = ("off", "shadow", "on")
_TS_FMT = "%Y-%m-%dT%H:%M:%SZ"


def _intraday_step_ns(bar_type) -> int | None:
    """Bar step in ns for MINUTE/HOUR bars; None for DAY and anything unknown."""
    spec = getattr(bar_type, "spec", None)
    if spec is None or getattr(spec, "aggregation", None) == BarAggregation.DAY:
        return None
    secs = _bar_interval_secs(bar_type)
    return secs * 1_000_000_000 if secs else None


def _ts_str(ts_ns: int) -> str:
    return pd.Timestamp(ts_ns, unit="ns", tz="UTC").strftime(_TS_FMT)


def _ohlcv_str(event: dict) -> str:
    return "/".join(str(event.get(k)) for k in ("Open", "High", "Low", "Close", "TotalVolume"))


def _cancel_bar_jobs(client, bar_type) -> None:
    """Cancel a bar type's release timer and backfill (every reset / unsubscribe / reconnect /
    disconnect path). A cancelled backfill rolls its hold back: parked bars never reached a
    strategy, and the next connection's seed replays them in order."""
    timers = getattr(client, "_release_timers", None)
    if timers is None:  # a minimal fake self (older tests)
        return
    handle = timers.pop(bar_type, None)
    if handle is not None:
        handle.cancel()
    client._release_targets.pop(bar_type, None)
    task = client._backfill_tasks.pop(bar_type, None)
    gate = client._bar_gates.get(bar_type)
    if gate is not None:
        gate.abort_backfill()
    if task is not None:
        task.cancel()


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
        bar_release_mode: str = "off",
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
        # Late-bar release / backfill (plan 2026-10-23): asyncio timers only (no LiveClock
        # timers, their callbacks run on a tokio thread - strategies LESSONS §210).
        if bar_release_mode not in _BAR_RELEASE_MODES:
            self._log.error(
                f"bar_release_mode={bar_release_mode!r} is not one of {_BAR_RELEASE_MODES} - "
                f"using 'off'"
            )
            bar_release_mode = "off"
        self._bar_release_mode = bar_release_mode
        self._release_timers: dict[BarType, asyncio.TimerHandle] = {}
        self._release_targets: dict[BarType, int] = {}  # armed release time (ns) per bar type
        self._release_fired: dict[BarType, int] = {}  # newest release time already handled
        self._backfill_tasks: dict[BarType, asyncio.Task] = {}
        self._verify_tasks: dict[BarType, set] = {}  # [BAR-RELEASE-DIVERGED] REST reads

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

        for bar_type in set(self._release_timers) | set(self._backfill_tasks):
            _cancel_bar_jobs(self, bar_type)
        self._release_fired.clear()
        for tasks in list(self._verify_tasks.values()):
            for task in list(tasks):
                task.cancel()
        self._verify_tasks.clear()
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
        _cancel_bar_jobs(self, bar_type)
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
                    _cancel_bar_jobs(self, bar_type)
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
        live_stream: bool = True,
    ) -> None:
        """Feed one TradeStation bar event through ``gate`` and emit what it releases.

        ``live_stream`` is False for polling, where every bar is history: only a stream's
        reconnect seed can replay a bar that closed during a gap. Release / backfill
        (``bar_release_mode``) run for intraday stream bars only.
        """
        ts_ns = bar_end_ns(event.get("TimeStamp", ""))
        if ts_ns is None:
            return
        # A gate release advances the watermark; an event that cannot become a Bar must not
        # get that far, or the bar is lost for good (seeds never replay it) - §207 review M4.
        if not self._parse_bars([event], bar_type, instrument):
            self._log.warning(
                f"[BAR-PARSE-SKIP] {bar_type} ts={event.get('TimeStamp')}: event does not parse "
                f"- not fed to the gate"
            )
            return
        mode = self._bar_release_mode if live_stream else "off"
        step_ns = _intraday_step_ns(bar_type) if mode != "off" else None
        if step_ns is None:
            mode = "off"  # DAY bars and polling: the plain gate
        now_ns = self._clock.timestamp_ns() if mode != "off" else None
        if mode != "off":
            # Before on_event: in mode on the barrier must exist before the later bar is fed.
            self._maybe_start_backfill(gate, ts_ns, step_ns, bar_type, instrument, mode)
        for raw in gate.on_event(event, ts_ns, now_ns=now_ns):
            self._log_emit_kind(raw, bar_type, live_stream)
            if (
                mode != "off"
                and raw.get("BarStatus") == "Closed"
                and raw.get("IsRealtime") is not False
            ):
                self._log_snapshot(gate, raw, bar_type, now_ns)
            self._emit_released(raw, bar_type, instrument)
        if mode == "off":
            return
        self._drain_diverged(gate, bar_type)
        pending_ns = gate.pending_end_ns
        if pending_ns is not None:
            self._arm_release(bar_type, instrument, pending_ns + _CLOSE_GRACE_NS)

    def _log_emit_kind(self, raw: dict, bar_type: BarType, live_stream: bool) -> None:
        if raw.get("BarStatus") != "Closed":
            self._log.info(
                f"[BAR-CLOSE-FALLBACK] {bar_type} ts={raw.get('TimeStamp')}: no Closed "
                f"event - emitted on the next bar's first update"
            )
        elif live_stream and raw.get("IsRealtime") is False:
            self._log.info(
                f"[BAR-GAP-REPLAY] {bar_type} ts={raw.get('TimeStamp')}: closed while the "
                f"stream was down - replayed from the reconnect seed"
            )

    def _emit_released(
        self,
        raw: dict,
        bar_type: BarType,
        instrument: Instrument,
        tag: str | None = None,
    ) -> bool:
        """Hand one raw bar to NT. ``tag`` is "[BAR-RELEASE]" or "[BAR-BACKFILL]"."""
        if tag == "[BAR-RELEASE]":
            self._log.info(
                f"[BAR-RELEASE] {bar_type} ts={raw.get('TimeStamp')}: no Closed event by end + "
                f"{_CLOSE_GRACE_NS // 1_000_000_000} s - emitted as it stood "
                f"O/H/L/C/V={_ohlcv_str(raw)}"
            )
        elif tag == "[BAR-BACKFILL]":
            self._log.info(
                f"[BAR-BACKFILL] {bar_type} ts={raw.get('TimeStamp')}: never sent by the stream "
                f"- emitted from REST history O/H/L/C/V={_ohlcv_str(raw)}"
            )
        bars = self._parse_bars([raw], bar_type, instrument)
        for bar in bars:
            self._handle_data(bar)
            self._last_bar_ts[bar_type] = datetime.utcnow().isoformat() + "Z"  # §70
        self._log.debug(f"Bar emitted for {bar_type}: ts={raw.get('TimeStamp')}")
        return bool(bars)

    def _log_snapshot(self, gate: BarCloseGate, raw: dict, bar_type: BarType, now_ns) -> None:
        """DEBUG [BAR-SNAPSHOT]: did the bar as it stood at end + N s equal its Closed?"""
        ts_ns = bar_end_ns(raw.get("TimeStamp", ""))
        snaps = gate.snapshots(ts_ns)
        closed = ohlcv(raw)
        parts = []
        for off in _SNAPSHOT_OFFSETS_NS:
            snap = snaps.get(off)
            parts.append(
                f"eq{off // 1_000_000_000}={None if snap is None else ohlcv(snap[1]) == closed}"
            )
        snap10 = snaps.get(_CLOSE_GRACE_NS)
        self._log.debug(
            f"[BAR-SNAPSHOT] {bar_type} ts={raw.get('TimeStamp')} "
            f"closed_lag_s={(now_ns - ts_ns) / 1e9:.3f} {' '.join(parts)} "
            f"vol_closed={raw.get('TotalVolume')} "
            f"vol10={None if snap10 is None else snap10[1].get('TotalVolume')}"
        )

    def _arm_release(self, bar_type: BarType, instrument: Instrument, target_ns: int) -> None:
        """Wake at ``target_ns`` (pending end + grace) for the forming bar; one timer per type."""
        if self._release_targets.get(bar_type) == target_ns:
            return
        if target_ns <= self._release_fired.get(bar_type, -1):
            return  # already handled (shadow keeps the pending bar for its real Closed)
        old = self._release_timers.pop(bar_type, None)
        if old is not None:
            old.cancel()
        delay = max(0.0, (target_ns - self._clock.timestamp_ns()) / 1e9)
        self._release_timers[bar_type] = self._loop.call_later(
            delay, self._on_release_due, bar_type, instrument, target_ns
        )
        self._release_targets[bar_type] = target_ns

    def _on_release_due(self, bar_type: BarType, instrument: Instrument, target_ns: int) -> None:
        """Release timer callback (event loop). A stale callback finds nothing to do."""
        try:
            if self._release_targets.get(bar_type) != target_ns:
                return  # re-armed or cancelled since
            self._release_timers.pop(bar_type, None)
            self._release_targets.pop(bar_type, None)
            gate = self._bar_gates.get(bar_type)
            if gate is None or gate.pending_end_ns is None:
                return  # its Closed came
            if gate.pending_end_ns + _CLOSE_GRACE_NS != target_ns:
                return  # another bar is forming now
            now_ns = self._clock.timestamp_ns()
            if now_ns < target_ns:  # call_later runs on monotonic time: fired marginally early
                self._arm_release(bar_type, instrument, target_ns)
                return
            self._release_fired[bar_type] = target_ns
            if self._bar_release_mode == "shadow":
                for raw in gate.release_due(now_ns, _CLOSE_GRACE_NS, commit=False):
                    self._log.info(
                        f"[BAR-RELEASE-SHADOW] {bar_type} would emit ts={raw.get('TimeStamp')} "
                        f"O/H/L/C/V={_ohlcv_str(raw)}"
                    )
            elif self._bar_release_mode == "on":
                for raw in gate.release_due(now_ns, _CLOSE_GRACE_NS):
                    self._emit_released(raw, bar_type, instrument, tag="[BAR-RELEASE]")
        except Exception as e:  # noqa: BLE001 - asyncio would swallow it into its own logger
            self._log.error(f"Bar release timer failed for {bar_type}: {e!r}")

    def _drain_diverged(self, gate: BarCloseGate, bar_type: BarType) -> None:
        """One REST read per released bar whose Closed differed (shadow and on)."""
        while gate.diverged:
            ts_ns, snap, event = gate.diverged.pop(0)
            task = self._loop.create_task(self._verify_divergence(bar_type, ts_ns, snap, event))
            tasks = self._verify_tasks.setdefault(bar_type, set())
            tasks.add(task)
            task.add_done_callback(tasks.discard)

    async def _verify_divergence(
        self, bar_type: BarType, ts_ns: int, snap: tuple, event: dict,
    ) -> None:
        """INFO [BAR-RELEASE-DIVERGED], exactly once: the snapshot vs the Closed vs REST.

        ``rest_eq_snapshot``: True (TradeStation merged the next bar into the stream's Closed),
        False, ``open`` (REST still forming), ``missing``, ``error`` or ``cancelled``.
        """
        result = "error"
        try:
            delay = ts_ns / 1e9 + _VERIFY_DELAY_S - self._clock.timestamp_ns() / 1e9
            if delay > 0:
                await asyncio.sleep(delay)
            interval, unit = bar_spec_to_ts_params(bar_type.spec)
            raws = await self._client.get_bars(
                symbol=bar_type.instrument_id.symbol.value,
                interval=interval,
                unit=unit,
                barsback=_STREAM_BARSBACK,
            )
            rest = next(
                (r for r in raws or [] if bar_end_ns(r.get("TimeStamp", "")) == ts_ns), None
            )
            if rest is None:
                result = "missing"
            elif rest.get("BarStatus") != "Closed":
                result = "open"
            else:
                result = str(ohlcv(rest) == snap)
        except asyncio.CancelledError:
            result = "cancelled"
            raise
        except Exception:  # noqa: BLE001 - any failure is reported as rest_eq_snapshot=error
            result = "error"
        finally:
            self._log.info(
                f"[BAR-RELEASE-DIVERGED] {bar_type} ts={_ts_str(ts_ns)} "
                f"snapshot={'/'.join(map(str, snap))} closed={_ohlcv_str(event)} "
                f"rest_eq_snapshot={result}"
            )

    def _maybe_start_backfill(
        self,
        gate: BarCloseGate,
        ts_ns: int,
        step_ns: int,
        bar_type: BarType,
        instrument: Instrument,
        mode: str,
    ) -> None:
        """A later bar arrived over a hole: fetch the missing period(s) from REST history.

        Triggered by the later bar, never by the wall clock (a no-trade period, a session
        close or a dead stream sends no later bar). Mode on holds every emission above the
        hole until the backfill is done, so strategies see the bars in order.
        """
        task = self._backfill_tasks.get(bar_type)
        if task is not None and not task.done():
            return
        periods = gate.missing_periods(ts_ns, step_ns, _BACKFILL_MAX_PERIODS)
        if not periods:
            return
        barrier_ns = gate.effective_watermark(ts_ns)
        gate.begin_backfill(periods)
        if mode == "on":
            gate.hold(barrier_ns)
        self._backfill_tasks[bar_type] = self._loop.create_task(
            self._backfill(gate, bar_type, instrument, periods, mode)
        )

    async def _backfill(
        self,
        gate: BarCloseGate,
        bar_type: BarType,
        instrument: Instrument,
        periods: list[int],
        mode: str,
    ) -> None:
        """Read the missing period(s) from REST; log them (shadow) or emit them (on)."""
        missing = ",".join(_ts_str(p) for p in periods)
        wanted = set(periods)
        last_end_s = periods[-1] / 1e9
        targets = (
            _BACKFILL_SHADOW_DELAYS_S
            if mode == "shadow"
            else (_BACKFILL_DELAY_S, _BACKFILL_DELAY_S + _BACKFILL_RETRY_S)
        )
        completed = False
        try:
            interval, unit = bar_spec_to_ts_params(bar_type.spec)
            found: dict[int, dict] = {}
            i = 0
            while i < len(targets):
                wait = last_end_s + targets[i] - self._clock.timestamp_ns() / 1e9
                if wait > 0:
                    await asyncio.sleep(wait)
                now_s = self._clock.timestamp_ns() / 1e9
                while i + 1 < len(targets) and last_end_s + targets[i + 1] <= now_s:
                    i += 1  # triggered late: the targets already passed are one poll
                raws = await self._client.get_bars(
                    symbol=bar_type.instrument_id.symbol.value,
                    interval=interval,
                    unit=unit,
                    barsback=len(periods) + _STREAM_BARSBACK,
                )
                found = {}
                for r in raws or []:
                    t = bar_end_ns(r.get("TimeStamp", ""))
                    if t in wanted:
                        found[t] = r
                done = all(p not in found or found[p].get("BarStatus") == "Closed" for p in periods)
                if mode == "shadow":
                    status = ",".join(
                        "absent" if p not in found else str(found[p].get("BarStatus"))
                        for p in periods
                    )
                    closed = [p for p in periods if found.get(p, {}).get("BarStatus") == "Closed"]
                    self._log.info(
                        f"[BAR-BACKFILL-SHADOW] {bar_type} delay_s={now_s - last_end_s:.1f} "
                        f"missing=[{missing}] status=[{status}] "
                        f"history_has=[{','.join(_ts_str(p) for p in closed)}] "
                        f"vol=[{','.join(str(found[p].get('TotalVolume')) for p in closed)}]"
                    )
                i += 1
                if done:
                    break
            for p in periods:
                raw = found.get(p)
                if raw is None:
                    self._log.info(
                        f"[BAR-BACKFILL-SKIP] {bar_type} ts={_ts_str(p)}: no history bar "
                        f"(no trades)"
                    )
                elif raw.get("BarStatus") != "Closed":
                    self._log.info(
                        f"[BAR-BACKFILL-SKIP] {bar_type} ts={_ts_str(p)}: history bar still "
                        f"Open after the last read"
                    )
                elif (
                    mode == "on"
                    and gate.backfill_ok(p)
                    and self._emit_released(raw, bar_type, instrument, tag="[BAR-BACKFILL]")
                ):
                    gate.mark_emitted(p)
            completed = True
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001 - logged as [BAR-BACKFILL-FAILED], hold released
            completed = True  # a failed episode is not retried by every later update
            self._log.warning(f"[BAR-BACKFILL-FAILED] {bar_type} missing=[{missing}]: {e!r}")
        finally:
            if self._backfill_tasks.get(bar_type) is asyncio.current_task():
                self._backfill_tasks.pop(bar_type, None)
                if completed:
                    gate.end_backfill()
                else:
                    gate.abort_backfill()
                for raw, released in gate.release_hold():
                    if released:
                        self._emit_released(raw, bar_type, instrument, tag="[BAR-RELEASE]")
                    else:
                        self._log_emit_kind(raw, bar_type, True)
                        self._emit_released(raw, bar_type, instrument)

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
                    self._emit_gated(gate, raw, bar_type, instrument, live_stream=False)
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
        _cancel_bar_jobs(self, bar_type)
        getattr(self, "_release_fired", {}).pop(bar_type, None)
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
            _cancel_bar_jobs(self, bar_type)
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
            # Answer anyway: without a response NT never calls the requester's callback
            # (the runner's warm-up would wait for a live bar) - LESSONS §207 follow-ups.
            self._handle_bars(
                bar_type, [], request.id, request.start, request.end, request.params,
            )
            return

        start_ts = pd.Timestamp(request.start) if request.start is not None else None

        bars = await self._historical.get_bars(
            bar_type=bar_type,
            instrument=instrument,
            start=start_ts,
            limit=request.limit,
        )

        if bars:
            # The history holds only closed bars (historical client): the stream must not
            # re-emit them (strategies LESSONS §207).
            self._bar_gates.setdefault(bar_type, BarCloseGate()).seed_from_history(
                bars[-1].ts_event
            )
        else:
            self._log.warning(
                f"Historical: no bars for {bar_type} - answering empty so the requester is "
                f"not left waiting"
            )
            bars = []

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
