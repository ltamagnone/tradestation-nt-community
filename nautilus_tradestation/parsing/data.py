"""
Parsing functions for TradeStation market data (bars, quotes).
"""
import logging

import pandas as pd

from nautilus_tradestation.common.enums import TradeStationBarUnit
from nautilus_trader.core.datetime import dt_to_unix_nanos
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.model.data import Bar, BarSpecification, BarType, QuoteTick, TradeTick
from nautilus_trader.model.enums import AggressorSide, BarAggregation
from nautilus_trader.model.identifiers import InstrumentId, TradeId
from nautilus_trader.model.instruments import Instrument
from nautilus_trader.model.objects import Price, Quantity


_log = logging.getLogger(__name__)


def bar_spec_to_ts_params(spec: BarSpecification) -> tuple[str, TradeStationBarUnit]:
    """Map a NautilusTrader BarSpecification to TradeStation interval and unit.

    Mapping
    -------
    MINUTE  → Minute unit, interval = step
    HOUR    → Minute unit, interval = step * 60  (e.g. 1-HOUR → "60" minutes)
    DAY     → Daily unit, interval = "1"

    Raises
    ------
    ValueError
        If the aggregation type is not supported.
    """
    if spec.aggregation == BarAggregation.MINUTE:
        return str(spec.step), TradeStationBarUnit.MINUTE
    elif spec.aggregation == BarAggregation.HOUR:
        return str(spec.step * 60), TradeStationBarUnit.MINUTE
    elif spec.aggregation == BarAggregation.DAY:
        return "1", TradeStationBarUnit.DAILY
    else:
        raise ValueError(
            f"Unsupported bar aggregation: {spec.aggregation}. "
            f"Supported aggregations: MINUTE, HOUR, DAY."
        )


def parse_bars(raw_bars: list[dict], bar_type: BarType) -> list[Bar]:
    """Parse raw TradeStation bar dicts into NautilusTrader Bar objects.

    Parameters
    ----------
    raw_bars : list[dict]
        Raw bar data from the TradeStation API (each dict has TimeStamp,
        Open, High, Low, Close, TotalVolume).
    bar_type : BarType
        The bar type to assign to each parsed bar.

    Returns
    -------
    list[Bar]
        Parsed bars in chronological order (skipping any malformed entries).
    """
    bars = []
    for raw_bar in raw_bars:
        try:
            ts_str = raw_bar["TimeStamp"]
            ts = pd.Timestamp(ts_str, tz="UTC")
            ts_event = dt_to_unix_nanos(ts)
            bar = Bar(
                bar_type=bar_type,
                open=Price.from_str(str(raw_bar["Open"])),
                high=Price.from_str(str(raw_bar["High"])),
                low=Price.from_str(str(raw_bar["Low"])),
                close=Price.from_str(str(raw_bar["Close"])),
                volume=Quantity.from_str(str(raw_bar.get("TotalVolume", 0))),
                ts_event=ts_event,
                ts_init=ts_event,
            )
            bars.append(bar)
        except Exception as e:
            _log.error(f"Error parsing bar: {e}, raw_bar={raw_bar}")
            continue
    return bars


def parse_quote_tick(
    raw_quote: dict,
    instrument_id: InstrumentId,
    instrument: Instrument,
) -> QuoteTick | None:
    """Parse a raw TradeStation quote dict into a NautilusTrader QuoteTick.

    Parameters
    ----------
    raw_quote : dict
        Raw quote dict from ``get_quotes()`` containing Bid, Ask, BidSize, AskSize,
        and optionally TimeStamp.
    instrument_id : InstrumentId
        The instrument this quote belongs to.
    instrument : Instrument
        Used for price/size precision.

    Returns
    -------
    QuoteTick or None
        Parsed tick, or None if bid or ask is missing / zero.
    """
    try:
        bid = float(raw_quote.get("Bid") or 0)
        ask = float(raw_quote.get("Ask") or 0)
        if bid == 0 or ask == 0:
            return None

        prec = instrument.price_precision
        size_prec = instrument.size_precision

        ts_str = raw_quote.get("TimeStamp", "")
        if ts_str:
            ts_event = dt_to_unix_nanos(pd.Timestamp(ts_str, tz="UTC"))
        else:
            ts_event = pd.Timestamp.utcnow().value

        return QuoteTick(
            instrument_id=instrument_id,
            bid_price=Price(bid, prec),
            ask_price=Price(ask, prec),
            bid_size=Quantity(float(raw_quote.get("BidSize") or 1), size_prec),
            ask_size=Quantity(float(raw_quote.get("AskSize") or 1), size_prec),
            ts_event=ts_event,
            ts_init=ts_event,
        )
    except Exception as e:
        _log.error(f"Error parsing quote tick: {e}, raw_quote={raw_quote}")
        return None


def parse_trade_tick(
    raw_quote: dict,
    instrument_id: InstrumentId,
    instrument: Instrument,
) -> TradeTick | None:
    """Parse the Last/LastSize fields of a TradeStation quote into a TradeTick.

    Parameters
    ----------
    raw_quote : dict
        Raw quote dict from ``get_quotes()`` containing Last, LastSize, and TimeStamp.
    instrument_id : InstrumentId
        The instrument this trade belongs to.
    instrument : Instrument
        Used for price/size precision.

    Returns
    -------
    TradeTick or None
        Parsed tick, or None if Last price is zero or missing.
    """
    try:
        last = float(raw_quote.get("Last") or 0)
        if last == 0:
            return None

        prec = instrument.price_precision
        size_prec = instrument.size_precision

        ts_str = raw_quote.get("TimeStamp", "")
        if ts_str:
            ts_event = dt_to_unix_nanos(pd.Timestamp(ts_str, tz="UTC"))
        else:
            ts_event = pd.Timestamp.utcnow().value

        return TradeTick(
            instrument_id=instrument_id,
            price=Price(last, prec),
            size=Quantity(float(raw_quote.get("LastSize") or 1), size_prec),
            aggressor_side=AggressorSide.NO_AGGRESSOR,
            trade_id=TradeId(str(UUID4())),
            ts_event=ts_event,
            ts_init=ts_event,
        )
    except Exception as e:
        _log.error(f"Error parsing trade tick: {e}, raw_quote={raw_quote}")
        return None
