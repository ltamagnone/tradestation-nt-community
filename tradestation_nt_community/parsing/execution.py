"""
Parsing functions for TradeStation execution reports and order conversion.
"""
import logging
from decimal import Decimal
from typing import Any

import pandas as pd

from nautilus_trader.core.datetime import dt_to_unix_nanos
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.execution.reports import FillReport, OrderStatusReport
from nautilus_trader.model.currencies import USD
from nautilus_trader.model.enums import ContingencyType, LiquiditySide, OrderSide, OrderStatus, OrderType, TimeInForce, TriggerType
from nautilus_trader.model.identifiers import AccountId, ClientOrderId, InstrumentId, TradeId, VenueOrderId
from nautilus_trader.model.objects import Currency, Money, Price, Quantity
from nautilus_trader.model.orders import LimitOrder, MarketOrder, Order, StopLimitOrder, StopMarketOrder

from tradestation_nt_community.common import order_status as _st


_log = logging.getLogger(__name__)

# Module-level constants — built once at import time, not on every call.
_TIF_TO_TS: dict[TimeInForce, str] = {
    TimeInForce.DAY: "DAY",
    TimeInForce.GTC: "GTC",
    TimeInForce.IOC: "IOC",
}

_TS_STATUS_TO_NT: dict[str, OrderStatus] = {
    **{c: OrderStatus.ACCEPTED for c in _st.OPEN},
    **{c: OrderStatus.CANCELED for c in _st.CANCELED},
    # R24: Canceled-category codes the adapter holds (assume live) so NT
    # reconciliation agrees with the live dispatch paths.
    **{c: OrderStatus.ACCEPTED for c in _st.CANCELED & _st.WATCH},
    "OPN": OrderStatus.SUBMITTED,
    "PLA": OrderStatus.SUBMITTED,
    "FPR": OrderStatus.PARTIALLY_FILLED,
    "EXP": OrderStatus.EXPIRED,
    "REJ": OrderStatus.REJECTED,
    "FLL": OrderStatus.FILLED,
    "FLP": OrderStatus.PARTIALLY_FILLED,  # unchanged: never observed (0 in logs 2026-10-08)
}

_TS_ORDER_TYPE_TO_NT: dict[str, OrderType] = {
    "Market": OrderType.MARKET,
    "Limit": OrderType.LIMIT,
    "StopMarket": OrderType.STOP_MARKET,
    "StopLimit": OrderType.STOP_LIMIT,
}


def convert_order_type(order: Order) -> str:
    """Convert a NautilusTrader order to a TradeStation order type string.

    Raises
    ------
    ValueError
        If the order type is not supported by TradeStation.
    """
    if isinstance(order, MarketOrder):
        return "Market"
    if isinstance(order, LimitOrder):
        return "Limit"
    if isinstance(order, StopMarketOrder):
        return "StopMarket"
    if isinstance(order, StopLimitOrder):
        return "StopLimit"
    raise ValueError(f"Unsupported order type: {type(order)}")


def convert_time_in_force(tif: TimeInForce) -> str:
    """Convert a NautilusTrader TimeInForce to a TradeStation duration string.

    Raises
    ------
    ValueError
        If ``tif`` is ``TimeInForce.FOK``. TradeStation always rejects FOK
        orders; callers must use ``TimeInForce.DAY`` instead.

    Notes
    -----
    Unknown values other than FOK fall back to ``"DAY"`` (safe default).
    """
    if tif == TimeInForce.FOK:
        raise ValueError(
            "TradeStation rejects FOK orders — use TimeInForce.DAY instead."
        )
    return _TIF_TO_TS.get(tif, "DAY")


def convert_order_to_ts_format(order: Order, account_id: str) -> dict[str, Any]:
    """Convert a NautilusTrader order to the kwargs dict for TradeStationHttpClient.place_order.

    Parameters
    ----------
    order : Order
        The order to convert.
    account_id : str
        The TradeStation account ID.

    Returns
    -------
    dict[str, Any]
        Keyword arguments ready to pass to ``client.place_order(**result)``.
    """
    symbol = str(order.instrument_id.symbol)
    ts_order_type = convert_order_type(order)
    ts_trade_action = "Buy" if order.side == OrderSide.BUY else "Sell"
    ts_tif = convert_time_in_force(order.time_in_force)

    params: dict[str, Any] = {
        "account_id": account_id,
        "symbol": symbol,
        "quantity": str(order.quantity),
        "order_type": ts_order_type,
        "trade_action": ts_trade_action,
        "time_in_force": ts_tif,
    }

    if isinstance(order, LimitOrder):
        params["limit_price"] = str(order.price)
    elif isinstance(order, StopMarketOrder):
        params["stop_price"] = str(order.trigger_price)
    elif isinstance(order, StopLimitOrder):
        params["limit_price"] = str(order.price)
        params["stop_price"] = str(order.trigger_price)

    return params


def parse_order_status(ts_status: str) -> OrderStatus:
    """TS status -> NT OrderStatus by spec category (common/order_status.py).
    Unknown codes are assumed live (ACCEPTED) — never invent a terminal state."""
    return _TS_STATUS_TO_NT.get(ts_status, OrderStatus.ACCEPTED)


def parse_ts_order_type(ts_order_type: str) -> OrderType:
    """Parse a TradeStation order type string to NautilusTrader OrderType."""
    return _TS_ORDER_TYPE_TO_NT.get(ts_order_type, OrderType.MARKET)


# TradeStation v3 order shape (OpenAPI Order/OrderBase/OrderLeg): quantity, side and
# fill quantity live on ``Legs[0]`` (QuantityOrdered / BuyOrSell / ExecQuantity);
# there is NO top-level Quantity/TradeAction/FilledQuantity/AveragePrice. The
# average fill price is the top-level ``FilledPrice`` and the TIF is the
# top-level ``Duration`` string. The old top-level names are read only as a
# fallback when ``Legs`` is absent (legacy shape / synthetic callers).
_BUY_ACTIONS = frozenset({"buy", "buytocover", "buytoopen", "buytoclose"})
_SELL_ACTIONS = frozenset({"sell", "sellshort", "selltoopen", "selltoclose"})

_TS_DURATION_TO_NT: dict[str, TimeInForce] = {
    "DAY": TimeInForce.DAY,
    "DYP": TimeInForce.DAY,
    "DAY+": TimeInForce.DAY,
    "GTC": TimeInForce.GTC,
    "GTC+": TimeInForce.GTC,
    "GCP": TimeInForce.GTC,
    "IOC": TimeInForce.IOC,
}


def _first_leg(ts_order: dict) -> dict | None:
    legs = ts_order.get("Legs")
    if isinstance(legs, list) and legs and isinstance(legs[0], dict):
        return legs[0]
    return None


def parse_order_side(ts_order: dict) -> OrderSide | None:
    """Order side from ``Legs[0].BuyOrSell`` (fallback: top-level ``TradeAction``).

    Returns ``None`` when the side is absent or unrecognised. Callers must skip
    the report: the side is never defaulted (a defaulted Buy turns every SELL
    into a BUY and NT would net it into the wrong position).
    """
    leg = _first_leg(ts_order)
    raw = leg.get("BuyOrSell") if leg is not None else None
    if not raw:
        raw = ts_order.get("TradeAction")
    key = str(raw or "").strip().lower()
    if key in _BUY_ACTIONS:
        return OrderSide.BUY
    if key in _SELL_ACTIONS:
        return OrderSide.SELL
    return None


def _order_quantities(ts_order: dict) -> tuple[Decimal, Decimal, bool]:
    """(ordered, filled, has_exec_qty) from Legs[0], else legacy top-level fields."""
    leg = _first_leg(ts_order)
    if leg is not None:
        ordered = Decimal(leg.get("QuantityOrdered") or "0")
        raw_filled = leg.get("ExecQuantity")
        return ordered, Decimal(raw_filled or "0"), raw_filled not in (None, "")
    ordered = Decimal(ts_order.get("Quantity") or "0")
    raw_filled = ts_order.get("FilledQuantity")
    return ordered, Decimal(raw_filled or "0"), raw_filled not in (None, "")


def _avg_fill_price(ts_order: dict) -> str:
    """Average fill price: ``FilledPrice`` (spec) -> ``AveragePrice`` (legacy)."""
    return ts_order.get("FilledPrice") or ts_order.get("AveragePrice") or "0"


def parse_time_in_force(ts_order: dict) -> TimeInForce:
    """TIF from top-level ``Duration`` (spec) or ``TimeInForce.Duration`` (request shape).

    Unknown durations map to DAY (never GTC: do not invent a longer life)."""
    dur = ts_order.get("Duration")
    if not dur:
        tif = ts_order.get("TimeInForce")
        dur = tif.get("Duration") if isinstance(tif, dict) else None
    return _TS_DURATION_TO_NT.get(str(dur or "").strip().upper(), TimeInForce.DAY)


def parse_order_status_report(
    ts_order: dict,
    instrument_id: InstrumentId,
    client_order_id: ClientOrderId,
    account_id: AccountId,
    ts_now: int,
) -> OrderStatusReport | None:
    """Parse a raw TradeStation v3 order dict into an OrderStatusReport.

    Reads quantity / side / fill quantity from ``Legs[0]``, the limit and stop
    prices from ``LimitPrice`` / ``StopPrice`` and the TIF from ``Duration``.

    Returns
    -------
    OrderStatusReport or None
        ``None`` (with a WARN where the order is real) when the quantity is
        zero, the side is unknown, a stop order has no ``StopPrice``, or the
        dict cannot be parsed.
    """
    try:
        ts_order_id = ts_order.get("OrderID")
        status = parse_order_status(ts_order.get("Status", ""))

        qty_ordered, qty_filled, _ = _order_quantities(ts_order)

        # TS returns zero quantity for rejected/cancelled-before-placed orders.
        # NT's Quantity requires a positive value — skip rather than error.
        if qty_ordered == 0:
            return None

        side = parse_order_side(ts_order)
        if side is None:
            _log.warning(
                f"Skipping order {ts_order_id}: unknown side "
                f"(BuyOrSell/TradeAction not recognised) - not defaulting to Buy"
            )
            return None

        order_type = parse_ts_order_type(ts_order.get("OrderType", "Market"))

        price_str = ts_order.get("LimitPrice") or ts_order.get("Price") or "0"
        price = Price.from_str(price_str) if price_str != "0" else None

        trigger_price = None
        trigger_type = None
        if order_type in (OrderType.STOP_MARKET, OrderType.STOP_LIMIT):
            stop_str = ts_order.get("StopPrice") or "0"
            if stop_str == "0":
                _log.warning(f"Skipping stop order {ts_order_id}: no StopPrice")
                return None
            trigger_price = Price.from_str(stop_str)
            trigger_type = TriggerType.DEFAULT
        if order_type == OrderType.STOP_MARKET:
            price = None  # a stop-market carries no limit price

        avg_str = _avg_fill_price(ts_order)
        avg_px = Decimal(avg_str) if avg_str != "0" else None

        kwargs: dict[str, Any] = {}
        if trigger_price is not None:
            kwargs["trigger_price"] = trigger_price
            kwargs["trigger_type"] = trigger_type

        return OrderStatusReport(
            account_id=account_id,
            instrument_id=instrument_id,
            client_order_id=client_order_id,
            venue_order_id=VenueOrderId(ts_order_id) if ts_order_id else None,
            order_side=side,
            order_type=order_type,
            time_in_force=parse_time_in_force(ts_order),
            order_status=status,
            price=price,
            quantity=Quantity.from_str(str(qty_ordered)),
            filled_qty=Quantity.from_str(str(qty_filled)),
            avg_px=avg_px,
            report_id=UUID4(),
            ts_accepted=ts_now,
            ts_last=ts_now,
            ts_init=ts_now,
            **kwargs,
        )

    except Exception as e:
        _log.error(f"Failed to parse order status report: {e}")
        return None


def parse_fill_report(
    ts_order: dict,
    instrument_id: InstrumentId,
    account_id: AccountId,
    ts_now: int,
    client_order_id: ClientOrderId | None = None,
) -> FillReport | None:
    """Parse a filled TradeStation v3 order dict into a NautilusTrader FillReport.

    Quantity is ``Legs[0].ExecQuantity`` (never the ordered quantity: an FLP
    without an ExecQuantity yields ``None``), price is ``FilledPrice`` (fallback
    ``Legs[0].ExecutionPrice``), side is ``Legs[0].BuyOrSell``.

    Returns
    -------
    FillReport or None
        ``None`` if the fill price, executed quantity or side cannot be read.
    """
    try:
        ts_order_id = ts_order.get("OrderID", "")

        # Fill price: FilledPrice (spec) / AveragePrice (legacy), then Legs ExecutionPrice
        avg_px_str = _avg_fill_price(ts_order)
        if avg_px_str == "0":
            leg = _first_leg(ts_order)
            avg_px_str = (leg.get("ExecutionPrice") if leg else None) or "0"
        if avg_px_str == "0":
            return None

        # Fill quantity: executed only. Legacy shape keeps its old Quantity fallback.
        leg = _first_leg(ts_order)
        if leg is not None:
            qty = Decimal(leg.get("ExecQuantity") or "0")
        else:
            qty = Decimal(ts_order.get("FilledQuantity") or ts_order.get("Quantity") or "0")
        if qty == 0:
            return None

        order_side = parse_order_side(ts_order)
        if order_side is None:
            _log.warning(
                f"Skipping fill for order {ts_order_id}: unknown side - not defaulting to Buy"
            )
            return None

        # Timestamp
        closed_str = ts_order.get("ClosedDateTime", "") or ts_order.get("OpenedDateTime", "")
        if closed_str:
            ts_event = dt_to_unix_nanos(pd.Timestamp(closed_str, tz="UTC"))
        else:
            ts_event = ts_now

        coid = client_order_id or ClientOrderId(f"TS-{ts_order_id}")

        return FillReport(
            account_id=account_id,
            instrument_id=instrument_id,
            venue_order_id=VenueOrderId(ts_order_id) if ts_order_id else None,
            trade_id=TradeId(f"FILL-{ts_order_id}"),
            order_side=order_side,
            last_qty=Quantity.from_str(str(qty)),
            last_px=Price.from_str(avg_px_str),
            commission=Money(0.0, USD),
            liquidity_side=LiquiditySide.NO_LIQUIDITY_SIDE,
            report_id=UUID4(),
            ts_event=ts_event,
            ts_init=ts_now,
            client_order_id=coid,
        )

    except Exception as e:
        _log.error(f"Failed to parse fill report: {e}")
        return None


def _group_type_for_order_list(orders: list[Order]) -> str | None:
    """Determine the TradeStation group type for an OrderList.

    Returns
    -------
    str | None
        ``"OCO"`` when all orders have OCO contingency,
        ``"BRK"`` when the list follows an OTO/bracket pattern (one OTO entry
        + two or more OCO exit legs), or ``None`` when the list doesn't match
        a recognised group pattern and should be submitted individually.
    """
    if not orders:
        return None

    contingencies = {o.contingency_type for o in orders}

    # Pure OCO: all orders cancel each other (e.g. two exit orders)
    if contingencies == {ContingencyType.OCO}:
        return "OCO"

    # Bracket: one OTO entry + OCO exits
    oto_orders = [o for o in orders if o.contingency_type == ContingencyType.OTO]
    oco_orders = [o for o in orders if o.contingency_type == ContingencyType.OCO]
    if len(oto_orders) == 1 and len(oco_orders) >= 2:
        return "BRK"

    return None


def convert_order_list_to_ts_group(
    orders: list[Order],
    account_id: str,
) -> tuple[str, list[dict]] | None:
    """Convert an NT OrderList into a TradeStation group order payload.

    Parameters
    ----------
    orders : list[Order]
        The orders from the OrderList (must be 2+ orders with contingencies).
    account_id : str
        The TradeStation account ID.

    Returns
    -------
    tuple[str, list[dict]] | None
        ``(group_type, orders_payload)`` if the list is a supported group
        pattern, or ``None`` if it should be submitted individually.
    """
    group_type = _group_type_for_order_list(orders)
    if group_type is None:
        return None

    order_payloads = []
    for order in orders:
        params = convert_order_to_ts_format(order, account_id)
        # convert_order_to_ts_format returns kwargs for place_order;
        # the group API uses the same fields but in a dict with TS-style keys.
        payload: dict = {
            "AccountID": params["account_id"],
            "Symbol": params["symbol"],
            "Quantity": params["quantity"],
            "OrderType": params["order_type"],
            "TradeAction": params["trade_action"],
            "TimeInForce": {"Duration": params["time_in_force"]},
        }
        if "limit_price" in params:
            payload["LimitPrice"] = params["limit_price"]
        if "stop_price" in params:
            payload["StopPrice"] = params["stop_price"]
        order_payloads.append(payload)

    return group_type, order_payloads
