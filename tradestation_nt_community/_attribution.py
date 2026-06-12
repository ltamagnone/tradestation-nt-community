"""Phase 0A: attribution dict for an NT order, for structured submit logging.

TradeStation has no round-tripping tag field, so we keep the 'who/why' on our
side.  Pure — no I/O, trivially testable.
"""
from __future__ import annotations

from typing import Any

from nautilus_trader.model.orders import Order


def order_attribution(order: Order) -> dict[str, Any]:
    """Return a plain dict capturing the attribution (who/why) of an NT order.

    Parameters
    ----------
    order : Order
        Any NautilusTrader order object.

    Returns
    -------
    dict[str, Any]
        Attribution dict with keys: client_order_id, strategy_id,
        instrument_id, side, type, tags.
    """
    return {
        "client_order_id": str(order.client_order_id),
        "strategy_id": str(order.strategy_id),
        "instrument_id": str(order.instrument_id),
        "side": order.side.name,
        "type": order.order_type.name,
        "tags": list(order.tags) if order.tags else [],
    }
