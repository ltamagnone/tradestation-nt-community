"""
TradeStation historical bar data client.

Encapsulates all historical bar request logic — bar-count estimation from a
date range and the actual ``get_bars`` API call — so that ``data.py`` can
delegate cleanly without mixing live-subscription and historical-request code.
"""
import logging

import pandas as pd

from nautilus_tradestation.http.client import TradeStationHttpClient
from nautilus_tradestation.parsing.data import (
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

    """

    def __init__(self, http_client: TradeStationHttpClient) -> None:
        self._http = http_client

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

        try:
            raw_bars = await self._http.get_bars(
                symbol=symbol,
                interval=interval,
                unit=unit,
                barsback=barsback,
            )
        except Exception as e:
            _log.error(f"Historical bar request failed for {bar_type}: {e}")
            return []

        if not raw_bars:
            _log.warning(f"No historical bars received for {bar_type}")
            return []

        bars = parse_bars(raw_bars, bar_type)
        _log.info(f"Historical: received {len(bars)} bars for {bar_type}")
        return bars
