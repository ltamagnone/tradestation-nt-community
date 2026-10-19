"""
TradeStation historical bar data client.

Encapsulates all historical bar request logic — bar-count estimation from a
date range and the actual ``get_bars`` API call — so that ``data.py`` can
delegate cleanly without mixing live-subscription and historical-request code.
"""
import asyncio
import logging
from collections.abc import Awaitable, Callable

import httpx
import pandas as pd

from tradestation_nt_community.http.client import GetBarsFailedException, TradeStationHttpClient
from tradestation_nt_community.parsing.data import (
    bar_spec_to_ts_params,
    parse_bars,
)
from nautilus_trader.model.data import Bar, BarSpecification, BarType
from nautilus_trader.model.enums import BarAggregation
from nautilus_trader.model.instruments import Instrument


_log = logging.getLogger(__name__)

# Maximum bars per request — TradeStation enforces this limit.
_MAX_BARS_PER_REQUEST: int = 3_000

# Minimum bars to request even when the calculated range is tiny.
_MIN_BARS: int = 10

# Warm-up history retry (strategies LESSONS §210 addendum, 2026-10-19): a failed request is
# tried at most _HIST_ATTEMPTS times, waiting _HIST_BACKOFF_S[i] before attempt i + 2 (the
# last entry is reused if _HIST_ATTEMPTS outgrows the table).
_HIST_ATTEMPTS: int = 3
_HIST_BACKOFF_S: tuple[float, ...] = (1.0, 3.0)


def _is_transient(error: Exception) -> bool:
    """True for a failure worth retrying: a timeout or transport error, or HTTP 5xx.

    ``httpx.TransportError`` covers ``TimeoutException`` (connect/read/write/pool),
    ``ConnectError``, ``ReadError`` and ``RemoteProtocolError``. Everything else is
    permanent: HTTP 4xx (wrong symbol, bad request, a 429 that ``_request`` already gave up
    on), a body that does not parse (``ValueError`` / ``httpx.DecodingError``), and any
    untyped error.
    """
    if isinstance(error, (httpx.TimeoutException, httpx.TransportError)):
        return True
    return isinstance(error, GetBarsFailedException) and 500 <= error.status_code < 600


def _describe(error: Exception) -> str:
    """The error for a log line: a timeout's message is empty, so name the class too."""
    if isinstance(error, GetBarsFailedException):
        return str(error)
    text = str(error)
    return f"{type(error).__name__}: {text}" if text else type(error).__name__


def estimate_barsback(
    spec: BarSpecification,
    start: pd.Timestamp | None,
    limit: int,
) -> int:
    """Estimate the ``barsback`` parameter for a historical bar request.

    TradeStation only supports ``barsback`` (not firstdate/lastdate) to avoid
    date-format ambiguity.  This function derives a bar count from either a
    start timestamp (warm-up path) or an explicit limit (on-demand path).

    Parameters
    ----------
    spec : BarSpecification
        The bar specification (step + aggregation) for the request.
    start : pd.Timestamp or None
        The requested start time.  When provided, the bar count is estimated
        from ``now - start`` using a ~23 h/day trading assumption.  When
        ``None``, ``limit`` is used directly.
    limit : int
        Explicit bar count requested.  Used when ``start`` is ``None``.

    Returns
    -------
    int
        Bar count to request, clamped to [``_MIN_BARS``, ``_MAX_BARS_PER_REQUEST``].
    """
    if start is not None:
        ts = start
        if ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        delta_days = (pd.Timestamp.utcnow() - ts).total_seconds() / 86400

        if spec.aggregation == BarAggregation.MINUTE:
            barsback = int(delta_days * 23 * 60 / spec.step) + 20
        elif spec.aggregation == BarAggregation.HOUR:
            barsback = int(delta_days * 23 / spec.step) + 10
        else:  # DAY or other
            barsback = int(delta_days) + 5
    else:
        barsback = limit if limit > 0 else 100

    return min(max(barsback, _MIN_BARS), _MAX_BARS_PER_REQUEST)


class TradeStationHistoricalClient:
    """
    Fetch and parse historical bar data from TradeStation.

    This client is used internally by ``TradeStationDataClient`` to handle
    ``RequestBars`` commands.  It delegates HTTP calls to the shared
    ``TradeStationHttpClient`` so authentication and connection reuse are
    transparent.

    Parameters
    ----------
    http_client : TradeStationHttpClient
        The shared async HTTP client.
    sleep : callable, optional
        ``async (seconds) -> None`` used for the retry backoff; ``asyncio.sleep`` when
        ``None`` (tests inject a recorder).

    """

    def __init__(
        self,
        http_client: TradeStationHttpClient,
        sleep: Callable[[float], Awaitable[None]] | None = None,
    ) -> None:
        self._http = http_client
        self._sleep = sleep

    def _http_closed(self) -> bool:
        """True once the shared HTTP client was closed (node shutdown).

        ``getattr`` with a default and ``is True``: a mock or other http object without a
        real ``is_closed`` counts as open.
        """
        return getattr(self._http, "is_closed", False) is True

    async def get_bars(
        self,
        bar_type: BarType,
        instrument: Instrument,
        start: pd.Timestamp | None = None,
        limit: int = 0,
    ) -> list[Bar]:
        """
        Fetch historical bars and return them as NautilusTrader Bar objects.

        Parameters
        ----------
        bar_type : BarType
            The bar type to request (instrument + spec).
        instrument : Instrument
            The instrument for price precision (used by the parser).
        start : pd.Timestamp or None
            Requested history start (warm-up path).  When ``None``, ``limit``
            is used.
        limit : int, default 0
            Explicit bar count (on-demand path).  Ignored when ``start`` is set.

        Returns
        -------
        list[Bar]
            Parsed bars in chronological order.  Empty list if the API returns
            no data or an error occurs.

        Notes
        -----
        A failed request is retried only on a transient failure (an httpx timeout or
        transport error, or HTTP 5xx): at most 3 attempts, waiting 1 s then 3 s. HTTP 4xx
        and parse errors fail at once, and so does any failure once the shared HTTP client is
        closed (``is_closed``: a node shutdown closes it under an in-flight GET, which then
        raises ``ReadError('')``; checked again after the backoff). Each retry logs WARNING
        ``[HIST-RETRY] {bar_type} attempt {n}/3 after {error}``, ``n`` being the attempt
        about to run (2, then 3). The final failure logs the ERROR
        ``Historical bar request failed for {bar_type}: {e}`` and returns ``[]``, as before.

        Worst-case delay: three 30 s timeouts + 4 s of backoff ~= 94 s (64 s more than the
        single attempt before the retry). That is not a hard ceiling: the httpx timeout
        (30 s) applies per phase (connect, read, write, pool), and each attempt still goes
        through ``_request``, whose HTTP 429 waits (each capped by ``retry_delay_max_ms``)
        can happen inside an attempt when a 429 is followed by a 5xx; those waits are not
        counted in the 94 s. A 429 that ``_request`` gives up on is a 4xx here: not retried.
        The strategies runner's warm-up hold cap is 60 s, so a live bar may complete the
        warm-up before a late retry answers; that is today's outcome on a failure, not worse.
        Order calls never come through here.

        """
        try:
            interval, unit = bar_spec_to_ts_params(bar_type.spec)
        except ValueError as e:
            _log.error(f"Unsupported bar spec for historical request: {e}")
            return []

        symbol = bar_type.instrument_id.symbol.value
        barsback = estimate_barsback(bar_type.spec, start, limit)

        _log.info(
            f"Historical request: {symbol} {barsback} bars "
            f"(interval={interval} unit={unit.value})"
        )

        attempt = 1
        while True:
            try:
                raw_bars = await self._http.get_bars(
                    symbol=symbol,
                    interval=interval,
                    unit=unit,
                    barsback=barsback,
                )
                break
            except Exception as e:
                # A closed client is a node shutdown (the ReadError('') of 2026-06-22 and
                # 2026-09-06), not an API failure: today's ERROR, no retry.
                if attempt >= _HIST_ATTEMPTS or not _is_transient(e) or self._http_closed():
                    _log.error(f"Historical bar request failed for {bar_type}: {e}")
                    return []
                delay = _HIST_BACKOFF_S[min(attempt, len(_HIST_BACKOFF_S)) - 1]
                attempt += 1
                _log.warning(
                    f"[HIST-RETRY] {bar_type} attempt {attempt}/{_HIST_ATTEMPTS} after "
                    f"{_describe(e)}"
                )
                await (self._sleep or asyncio.sleep)(delay)
                if self._http_closed():  # shut down during the backoff: no further GET
                    _log.error(f"Historical bar request failed for {bar_type}: {e}")
                    return []

        if not raw_bars:
            _log.warning(f"No historical bars received for {bar_type}")
            return []

        # TS returns the still-forming bar last (BarStatus "Open", TimeStamp = its future END):
        # warm-up must not replay a partial bar - the stream emits it complete when it closes
        # (strategies LESSONS §207).
        closed = [b for b in raw_bars if b.get("BarStatus") != "Open"]
        if len(closed) != len(raw_bars):
            _log.debug(
                f"Historical: dropped {len(raw_bars) - len(closed)} forming bar(s) for {bar_type}"
            )
        bars = parse_bars(closed, bar_type)
        _log.info(f"Historical: received {len(bars)} bars for {bar_type}")
        return bars
