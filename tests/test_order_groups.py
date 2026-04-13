"""
Tests for Phase 8 — OCO and bracket order group support.

Tests the parsing helpers and HTTP client fixture.  The full _submit_order_list
integration path requires a live NT node and is covered by the fallback path test.
"""
import json
from pathlib import Path

import pytest

from tradestation_nt_community.parsing.execution import (
    _group_type_for_order_list,
    convert_order_list_to_ts_group,
)
from nautilus_trader.model.enums import (
    ContingencyType,
    OrderSide,
    TimeInForce,
    TriggerType,
)
from nautilus_trader.model.identifiers import (
    ClientOrderId,
    InstrumentId,
    OrderListId,
    StrategyId,
    TraderId,
)
from nautilus_trader.model.objects import Price, Quantity
from nautilus_trader.core.uuid import UUID4


_RESOURCES = Path(__file__).parent / "resources"
_INSTRUMENT_ID = InstrumentId.from_str("GCJ26.TRADESTATION")
_ACCOUNT_ID = "SIM0000001F"


def _uid():
    return UUID4()


def _limit_order(coid: str, side: OrderSide, price: float,
                 contingency: ContingencyType = ContingencyType.OCO,
                 order_list_id: str = "OL-001",
                 linked: list[str] | None = None):
    from nautilus_trader.model.orders import LimitOrder
    return LimitOrder(
        trader_id=TraderId("T-001"), strategy_id=StrategyId("S-001"),
        instrument_id=_INSTRUMENT_ID,
        client_order_id=ClientOrderId(coid),
        order_side=side,
        quantity=Quantity.from_int(1),
        price=Price(price, 1),
        time_in_force=TimeInForce.GTC,
        init_id=_uid(), ts_init=0,
        contingency_type=contingency,
        order_list_id=OrderListId(order_list_id),
        linked_order_ids=[ClientOrderId(c) for c in (linked or [])],
    )


def _market_order(coid: str, side: OrderSide,
                  contingency: ContingencyType = ContingencyType.OTO,
                  order_list_id: str = "OL-001",
                  linked: list[str] | None = None):
    from nautilus_trader.model.orders import MarketOrder
    return MarketOrder(
        trader_id=TraderId("T-001"), strategy_id=StrategyId("S-001"),
        instrument_id=_INSTRUMENT_ID,
        client_order_id=ClientOrderId(coid),
        order_side=OrderSide.BUY,
        quantity=Quantity.from_int(1),
        time_in_force=TimeInForce.DAY,
        init_id=_uid(), ts_init=0,
        contingency_type=contingency,
        order_list_id=OrderListId(order_list_id),
        linked_order_ids=[ClientOrderId(c) for c in (linked or [])],
    )


def _stop_order(coid: str, side: OrderSide, stop_price: float,
                contingency: ContingencyType = ContingencyType.OCO,
                order_list_id: str = "OL-001",
                linked: list[str] | None = None):
    from nautilus_trader.model.orders import StopMarketOrder
    return StopMarketOrder(
        trader_id=TraderId("T-001"), strategy_id=StrategyId("S-001"),
        instrument_id=_INSTRUMENT_ID,
        client_order_id=ClientOrderId(coid),
        order_side=side,
        quantity=Quantity.from_int(1),
        trigger_price=Price(stop_price, 1),
        trigger_type=TriggerType.DEFAULT,
        time_in_force=TimeInForce.GTC,
        init_id=_uid(), ts_init=0,
        contingency_type=contingency,
        order_list_id=OrderListId(order_list_id),
        linked_order_ids=[ClientOrderId(c) for c in (linked or [])],
    )



class TestGroupTypeDetection:
    """Tests for _group_type_for_order_list — pattern detection."""

    def test_pure_oco_two_limits(self):
        """Two OCO limit orders → 'OCO'."""
        sl = _limit_order("O-SL", OrderSide.SELL, 3300.0, ContingencyType.OCO, linked=["O-TP"])
        tp = _limit_order("O-TP", OrderSide.SELL, 3500.0, ContingencyType.OCO, linked=["O-SL"])
        assert _group_type_for_order_list([sl, tp]) == "OCO"

    def test_pure_oco_stop_and_limit(self):
        """OCO with stop + limit exits → 'OCO'."""
        sl = _stop_order("O-SL", OrderSide.SELL, 3300.0, ContingencyType.OCO, linked=["O-TP"])
        tp = _limit_order("O-TP", OrderSide.SELL, 3500.0, ContingencyType.OCO, linked=["O-SL"])
        assert _group_type_for_order_list([sl, tp]) == "OCO"

    def test_bracket_market_entry_plus_two_oco_exits(self):
        """OTO entry + 2 OCO exits → 'BRK'."""
        entry = _market_order("O-E", OrderSide.BUY, ContingencyType.OTO, linked=["O-SL", "O-TP"])
        sl = _stop_order("O-SL", OrderSide.SELL, 3300.0, ContingencyType.OCO, linked=["O-TP"])
        tp = _limit_order("O-TP", OrderSide.SELL, 3500.0, ContingencyType.OCO, linked=["O-SL"])
        assert _group_type_for_order_list([entry, sl, tp]) == "BRK"

    def test_no_contingency_returns_none(self):
        """Orders with NO_CONTINGENCY → None (submit individually)."""
        o1 = _limit_order("O-1", OrderSide.BUY, 3400.0, ContingencyType.NO_CONTINGENCY)
        o2 = _limit_order("O-2", OrderSide.SELL, 3450.0, ContingencyType.NO_CONTINGENCY)
        assert _group_type_for_order_list([o1, o2]) is None

    def test_mixed_unrecognised_returns_none(self):
        """OTO entry + only one OCO exit → None (bracket needs ≥ 2 OCO exits)."""
        entry = _market_order("O-E", OrderSide.BUY, ContingencyType.OTO, linked=["O-SL"])
        sl = _stop_order("O-SL", OrderSide.SELL, 3300.0, ContingencyType.OCO, linked=["O-E"])
        assert _group_type_for_order_list([entry, sl]) is None

    def test_empty_list_returns_none(self):
        assert _group_type_for_order_list([]) is None



class TestConvertOrderListToTsGroup:
    """Tests for convert_order_list_to_ts_group — payload generation."""

    def test_oco_returns_oco_type(self):
        sl = _stop_order("O-SL", OrderSide.SELL, 3300.0, ContingencyType.OCO, linked=["O-TP"])
        tp = _limit_order("O-TP", OrderSide.SELL, 3500.0, ContingencyType.OCO, linked=["O-SL"])
        result = convert_order_list_to_ts_group([sl, tp], _ACCOUNT_ID)
        assert result is not None
        group_type, payloads = result
        assert group_type == "OCO"

    def test_oco_payload_count(self):
        sl = _stop_order("O-SL", OrderSide.SELL, 3300.0, ContingencyType.OCO, linked=["O-TP"])
        tp = _limit_order("O-TP", OrderSide.SELL, 3500.0, ContingencyType.OCO, linked=["O-SL"])
        _, payloads = convert_order_list_to_ts_group([sl, tp], _ACCOUNT_ID)
        assert len(payloads) == 2

    def test_oco_payload_has_required_fields(self):
        sl = _stop_order("O-SL", OrderSide.SELL, 3300.0, ContingencyType.OCO, linked=["O-TP"])
        tp = _limit_order("O-TP", OrderSide.SELL, 3500.0, ContingencyType.OCO, linked=["O-SL"])
        _, payloads = convert_order_list_to_ts_group([sl, tp], _ACCOUNT_ID)
        for p in payloads:
            assert "AccountID" in p
            assert "Symbol" in p
            assert "Quantity" in p
            assert "OrderType" in p
            assert "TradeAction" in p
            assert "TimeInForce" in p

    def test_stop_order_payload_has_stop_price(self):
        sl = _stop_order("O-SL", OrderSide.SELL, 3300.0, ContingencyType.OCO, linked=["O-TP"])
        tp = _limit_order("O-TP", OrderSide.SELL, 3500.0, ContingencyType.OCO, linked=["O-SL"])
        _, payloads = convert_order_list_to_ts_group([sl, tp], _ACCOUNT_ID)
        stop_payload = next(p for p in payloads if p["OrderType"] == "StopMarket")
        assert "StopPrice" in stop_payload
        assert float(stop_payload["StopPrice"]) == pytest.approx(3300.0, rel=1e-4)

    def test_limit_order_payload_has_limit_price(self):
        sl = _stop_order("O-SL", OrderSide.SELL, 3300.0, ContingencyType.OCO, linked=["O-TP"])
        tp = _limit_order("O-TP", OrderSide.SELL, 3500.0, ContingencyType.OCO, linked=["O-SL"])
        _, payloads = convert_order_list_to_ts_group([sl, tp], _ACCOUNT_ID)
        limit_payload = next(p for p in payloads if p["OrderType"] == "Limit")
        assert "LimitPrice" in limit_payload
        assert float(limit_payload["LimitPrice"]) == pytest.approx(3500.0, rel=1e-4)

    def test_bracket_returns_brk_type(self):
        entry = _market_order("O-E", OrderSide.BUY, ContingencyType.OTO, linked=["O-SL", "O-TP"])
        sl = _stop_order("O-SL", OrderSide.SELL, 3300.0, ContingencyType.OCO, linked=["O-TP"])
        tp = _limit_order("O-TP", OrderSide.SELL, 3500.0, ContingencyType.OCO, linked=["O-SL"])
        result = convert_order_list_to_ts_group([entry, sl, tp], _ACCOUNT_ID)
        assert result is not None
        group_type, payloads = result
        assert group_type == "BRK"
        assert len(payloads) == 3

    def test_no_contingency_returns_none(self):
        o1 = _limit_order("O-1", OrderSide.BUY, 3400.0, ContingencyType.NO_CONTINGENCY)
        o2 = _limit_order("O-2", OrderSide.SELL, 3450.0, ContingencyType.NO_CONTINGENCY)
        assert convert_order_list_to_ts_group([o1, o2], _ACCOUNT_ID) is None

    def test_account_id_in_all_payloads(self):
        sl = _stop_order("O-SL", OrderSide.SELL, 3300.0, ContingencyType.OCO, linked=["O-TP"])
        tp = _limit_order("O-TP", OrderSide.SELL, 3500.0, ContingencyType.OCO, linked=["O-SL"])
        _, payloads = convert_order_list_to_ts_group([sl, tp], _ACCOUNT_ID)
        assert all(p["AccountID"] == _ACCOUNT_ID for p in payloads)



class TestPlaceOrderGroupFixture:
    """Tests for the place_order_group_response.json fixture and mock routing."""

    def test_fixture_has_order_group_id(self):
        data = json.loads((_RESOURCES / "place_order_group_response.json").read_text())
        assert "OrderGroupId" in data
        assert data["OrderGroupId"] == "GRP-001"

    def test_fixture_has_two_orders(self):
        data = json.loads((_RESOURCES / "place_order_group_response.json").read_text())
        assert len(data["Orders"]) == 2

    def test_fixture_each_order_has_id(self):
        data = json.loads((_RESOURCES / "place_order_group_response.json").read_text())
        for order in data["Orders"]:
            assert "OrderID" in order

    @pytest.mark.asyncio
    async def test_mock_client_returns_group_response(self, stub_http_client):
        result = await stub_http_client.place_order_group(
            group_type="OCO",
            orders=[{"AccountID": "SIM001", "Symbol": "GCJ26"}],
        )
        assert result["OrderGroupId"] == "GRP-001"
        assert len(result["Orders"]) == 2
