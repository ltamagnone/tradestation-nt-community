"""Task 14 (§153): the parsers read the real TradeStation v3 order shape.

Fixtures are copied from the OpenAPI spec examples
(docs/api_reference/specificationOpenAPI2026.json, GET /brokerage/accounts/{a}/orders
200 example): quantity / side / executed quantity are on ``Legs[0]``
(QuantityOrdered / BuyOrSell / ExecQuantity), the average fill price is the
top-level ``FilledPrice``, the TIF is the top-level ``Duration``, and there is
NO top-level Quantity / TradeAction / FilledQuantity / AveragePrice.
The symbols are swapped from MSFT to a future so the instrument matches.
"""
from __future__ import annotations

import copy
import logging

import pytest

from nautilus_trader.model.enums import OrderSide, OrderStatus, OrderType, TimeInForce, TriggerType
from nautilus_trader.model.identifiers import AccountId, ClientOrderId, InstrumentId

from tradestation_nt_community.parsing.execution import (
    parse_fill_report,
    parse_order_status_report,
)
from tests.test_kit import TSTestOrderStubs

ACCT = AccountId("TRADESTATION-SIM0000001F")
IID = InstrumentId.from_str("GCJ26.TRADESTATION")
NOW = 1_700_000_000_000_000_000


def _leg(side, qty, exec_qty, remaining, openclose="Open", **extra):
    leg = {
        "OpenOrClose": openclose,
        "QuantityOrdered": qty,
        "ExecQuantity": exec_qty,
        "QuantityRemaining": remaining,
        "BuyOrSell": side,
        "Symbol": "GCJ26",
        "AssetType": "FUTURE",
    }
    leg.update(extra)
    return leg


# spec example #1 (OPN, Market, Buy 10) shape
SPEC_OPEN_MARKET_BUY = {
    "AccountID": "123456782", "CommissionFee": "0", "Currency": "USD", "Duration": "GTC",
    "GoodTillDate": "2021-05-25T00:00:00Z",
    "Legs": [_leg("Buy", "10", "0", "10", ExecutionPrice="112.28")],
    "OrderID": "286234131", "OpenedDateTime": "2021-02-24T15:47:45Z", "OrderType": "Market",
    "PriceUsedForBuyingPower": "230.46", "Routing": "Intelligent", "Status": "OPN",
    "StatusDescription": "Sent", "UnbundledRouteFee": "0",
}
# spec example #2 (FLL, Market, Buy 10, FilledPrice 216.68)
SPEC_FILLED_MARKET_BUY = {
    "AccountID": "123456782", "CommissionFee": "0", "ClosedDateTime": "2020-11-16T16:53:37Z",
    "Currency": "USD", "Duration": "GTC", "FilledPrice": "216.68",
    "Legs": [_leg("Buy", "10", "10", "0")],
    "OrderID": "123456789", "OpenedDateTime": "2020-11-16T16:53:37Z", "OrderType": "Market",
    "Routing": "Intelligent", "Status": "FLL", "StatusDescription": "Filled",
}
# spec example #3 (OCO Limit, Sell 10, LimitPrice 230, Close)
SPEC_OCO_LIMIT_SELL = {
    "AccountID": "123456782", "CommissionFee": "0",
    "ConditionalOrders": [{"Relationship": "OCO", "OrderID": "286179864"}],
    "Currency": "USD", "Duration": "GTC", "GoodTillDate": "2021-02-15T00:00:00Z",
    "GroupName": "OCO 2706452145",
    "Legs": [_leg("Sell", "10", "0", "10", openclose="Close")],
    "LimitPrice": "230", "OrderID": "286179863", "OpenedDateTime": "2020-11-17T16:34:37Z",
    "OrderType": "Limit", "Routing": "Intelligent", "Status": "ACK",
    "StatusDescription": "Received",
}
# spec example #4 (OCO StopMarket Sell) plus StopPrice (OrderBase.StopPrice, spec'd on Order)
SPEC_OCO_STOP_SELL = {
    "AccountID": "123456782", "CommissionFee": "0",
    "ConditionalOrders": [{"Relationship": "OCO", "OrderID": "286179863"}],
    "Currency": "USD", "Duration": "GTC", "GroupName": "OCO 2706452145",
    "Legs": [_leg("Sell", "10", "0", "10", openclose="Close")],
    "OrderID": "286179864", "OpenedDateTime": "2020-11-17T16:34:37Z",
    "OrderType": "StopMarket", "StopPrice": "215.0", "Routing": "Intelligent",
    "Status": "ACK", "StatusDescription": "Received",
}


def _report(order):
    return parse_order_status_report(order, IID, ClientOrderId("TS-X"), ACCT, NOW)


class TestSpecShapeOrderStatusReport:
    def test_open_market_buy(self):
        r = _report(SPEC_OPEN_MARKET_BUY)
        assert r is not None
        assert r.order_side == OrderSide.BUY
        assert r.quantity == 10
        assert r.filled_qty == 0
        assert r.order_type == OrderType.MARKET
        assert r.order_status == OrderStatus.SUBMITTED  # OPN
        assert r.time_in_force == TimeInForce.GTC
        assert str(r.venue_order_id) == "286234131"

    def test_filled_market_buy_uses_exec_quantity_and_filled_price(self):
        r = _report(SPEC_FILLED_MARKET_BUY)
        assert r.order_status == OrderStatus.FILLED
        assert r.quantity == 10 and r.filled_qty == 10
        assert float(r.avg_px) == pytest.approx(216.68)

    def test_sell_is_sell_not_defaulted_to_buy(self):
        r = _report(SPEC_OCO_LIMIT_SELL)
        assert r.order_side == OrderSide.SELL
        assert r.order_type == OrderType.LIMIT
        assert float(r.price) == 230.0
        assert r.order_status == OrderStatus.ACCEPTED  # ACK
        assert r.time_in_force == TimeInForce.GTC

    def test_stop_market_sell_carries_trigger_price(self):
        r = _report(SPEC_OCO_STOP_SELL)
        assert r.order_side == OrderSide.SELL
        assert r.order_type == OrderType.STOP_MARKET
        assert float(r.trigger_price) == 215.0
        assert r.trigger_type == TriggerType.DEFAULT
        assert r.price is None

    def test_stop_without_stop_price_is_skipped(self):
        o = copy.deepcopy(SPEC_OCO_STOP_SELL)
        del o["StopPrice"]
        assert _report(o) is None

    def test_duration_day_and_missing(self):
        o = copy.deepcopy(SPEC_OPEN_MARKET_BUY)
        o["Duration"] = "DAY"
        assert _report(o).time_in_force == TimeInForce.DAY
        del o["Duration"]
        assert _report(o).time_in_force == TimeInForce.DAY

    def test_time_in_force_request_shape_fallback(self):
        o = copy.deepcopy(SPEC_OPEN_MARKET_BUY)
        del o["Duration"]
        o["TimeInForce"] = {"Duration": "GTC"}
        assert _report(o).time_in_force == TimeInForce.GTC

    def test_sell_short_and_buy_to_cover_actions(self):
        o = copy.deepcopy(SPEC_OPEN_MARKET_BUY)
        o["Legs"][0]["BuyOrSell"] = "SellShort"
        assert _report(o).order_side == OrderSide.SELL
        o["Legs"][0]["BuyOrSell"] = "BuyToCover"
        assert _report(o).order_side == OrderSide.BUY

    def test_unknown_side_is_skipped_with_warning(self, caplog):
        o = copy.deepcopy(SPEC_OPEN_MARKET_BUY)
        o["Legs"][0]["BuyOrSell"] = "Hold"
        with caplog.at_level(logging.WARNING):
            assert _report(o) is None
        assert any("unknown side" in m for m in caplog.messages)

    def test_missing_side_is_never_defaulted_to_buy(self, caplog):
        o = copy.deepcopy(SPEC_OPEN_MARKET_BUY)
        del o["Legs"][0]["BuyOrSell"]
        with caplog.at_level(logging.WARNING):
            assert _report(o) is None
        assert any("unknown side" in m for m in caplog.messages)

    def test_zero_quantity_ordered_is_skipped(self):
        o = copy.deepcopy(SPEC_OPEN_MARKET_BUY)
        o["Legs"][0]["QuantityOrdered"] = "0"
        assert _report(o) is None

    def test_fixture_resources_are_spec_shaped(self):
        for o in (TSTestOrderStubs.market_order_filled(), TSTestOrderStubs.limit_order_open(),
                  TSTestOrderStubs.stop_order_filled(), TSTestOrderStubs.order_canceled()):
            assert "Quantity" not in o and "TradeAction" not in o and "FilledQuantity" not in o
            assert _report(o) is not None or o["Status"] == "CAN"


class TestSpecShapeFillReport:
    def _fill(self, order):
        return parse_fill_report(order, IID, ACCT, NOW)

    def test_fill_from_exec_quantity_and_filled_price(self):
        f = self._fill(SPEC_FILLED_MARKET_BUY)
        assert f is not None
        assert f.order_side == OrderSide.BUY
        assert f.last_qty == 10
        assert float(f.last_px) == pytest.approx(216.68)

    def test_sell_fill_is_sell(self):
        o = copy.deepcopy(SPEC_FILLED_MARKET_BUY)
        o["Legs"][0]["BuyOrSell"] = "Sell"
        assert self._fill(o).order_side == OrderSide.SELL

    def test_flp_without_exec_quantity_is_not_the_full_ordered_quantity(self):
        o = copy.deepcopy(SPEC_FILLED_MARKET_BUY)
        o["Status"] = "FLP"
        del o["Legs"][0]["ExecQuantity"]
        assert self._fill(o) is None

    def test_flp_partial_uses_exec_quantity(self):
        o = copy.deepcopy(SPEC_FILLED_MARKET_BUY)
        o["Status"] = "FLP"
        o["Legs"][0]["ExecQuantity"] = "4"
        o["Legs"][0]["QuantityRemaining"] = "6"
        assert self._fill(o).last_qty == 4

    def test_unknown_side_fill_is_skipped_with_warning(self, caplog):
        o = copy.deepcopy(SPEC_FILLED_MARKET_BUY)
        o["Legs"][0]["BuyOrSell"] = "???"
        with caplog.at_level(logging.WARNING):
            assert self._fill(o) is None
        assert any("unknown side" in m for m in caplog.messages)

    def test_price_falls_back_to_leg_execution_price(self):
        o = copy.deepcopy(SPEC_FILLED_MARKET_BUY)
        del o["FilledPrice"]
        o["Legs"][0]["ExecutionPrice"] = "216.5"
        assert float(self._fill(o).last_px) == pytest.approx(216.5)


class TestLegacyShapeStillReads:
    """The old synthetic top-level shape is read only when Legs is absent."""

    LEGACY = {
        "OrderID": "L-1", "Status": "FLL", "OrderType": "Limit", "LimitPrice": "3400.0",
        "TradeAction": "Sell", "Quantity": "2", "FilledQuantity": "2", "AveragePrice": "3400.0",
        "ClosedDateTime": "2026-04-08T14:30:01Z",
    }

    def test_legacy_report_and_fill(self):
        r = _report(self.LEGACY)
        assert r.order_side == OrderSide.SELL and r.quantity == 2 and r.filled_qty == 2
        f = parse_fill_report(self.LEGACY, IID, ACCT, NOW)
        assert f.order_side == OrderSide.SELL and f.last_qty == 2

    def test_legacy_without_trade_action_is_skipped(self):
        o = dict(self.LEGACY)
        del o["TradeAction"]
        assert _report(o) is None
