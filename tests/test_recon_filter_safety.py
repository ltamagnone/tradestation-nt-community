"""Task 14 (§153): restart behaviour must not change once the parser yields reports.

With ``filter_unclaimed_external_orders=True`` (the runner's config, see
run_paper_trading._build_exec_engine_config), NT's mass-status reconciliation
must create NO order, fill or position from parsed TS v3 order/fill reports.
NT source: ``LiveExecutionEngine._generate_order`` returns None for an
unclaimed VENUE order (execution_engine.py:3438) and ``_reconcile_order_report``
returns before ``cache.add_order`` / the trades loop (:2938-2946).

Differential test: engine A reconciles a mass status built from parsed reports,
engine B the same mass status with NO order/fill reports (== what the broken
parser produced). The caches must be identical. A control engine WITHOUT the
filter proves the test would catch the regression (it creates EXTERNAL orders).
"""
from __future__ import annotations

import asyncio

import pytest

from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock, MessageBus
from nautilus_trader.config import LiveExecEngineConfig
from nautilus_trader.execution.reports import ExecutionMassStatus as _EMS
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.live.execution_engine import LiveExecutionEngine
from nautilus_trader.execution.reports import PositionStatusReport
from nautilus_trader.model.enums import PositionSide
from nautilus_trader.model.identifiers import AccountId, ClientId, ClientOrderId, InstrumentId, TraderId, Venue
from nautilus_trader.model.objects import Quantity

from tradestation_nt_community.parsing.execution import parse_fill_report, parse_order_status_report
from tests.test_kit import TSTestInstrumentStubs
from tests.test_order_v3_shape import (
    SPEC_FILLED_MARKET_BUY,
    SPEC_OCO_LIMIT_SELL,
    SPEC_OCO_STOP_SELL,
    SPEC_OPEN_MARKET_BUY,
)

ACCT = AccountId("TRADESTATION-SIM0000001F")
IID = InstrumentId.from_str("GCJ26.TRADESTATION")


def _engine(loop, **cfg):
    clock = LiveClock()
    trader = TraderId("TESTER-001")
    msgbus = MessageBus(trader_id=trader, clock=clock)
    cache = Cache()
    cache.add_instrument(TSTestInstrumentStubs.gc_futures_contract())
    eng = LiveExecutionEngine(loop=loop, msgbus=msgbus, cache=cache, clock=clock,
                              config=LiveExecEngineConfig(reconciliation=True, **cfg))
    return eng, cache


def _mass_status(with_orders: bool, position_qty: int | None):
    ms = _EMS(client_id=ClientId("TRADESTATION"), account_id=ACCT, venue=Venue("TRADESTATION"),
              report_id=UUID4(), ts_init=0)
    if with_orders:
        orders = [SPEC_OPEN_MARKET_BUY, SPEC_OCO_LIMIT_SELL, SPEC_OCO_STOP_SELL, SPEC_FILLED_MARKET_BUY]
        reports = [parse_order_status_report(o, IID, ClientOrderId(f"TS-{o['OrderID']}"), ACCT, 1) for o in orders]
        assert all(r is not None for r in reports)  # the parser now yields real reports
        ms.add_order_reports(reports=reports)
        fills = [parse_fill_report(o, IID, ACCT, 1, ClientOrderId(f"TS-{o['OrderID']}"))
                 for o in orders if o["Status"] == "FLL"]
        assert fills and all(f is not None for f in fills)
        ms.add_fill_reports(reports=fills)
    if position_qty:
        ms.add_position_reports(reports=[PositionStatusReport(
            account_id=ACCT, instrument_id=IID, position_side=PositionSide.LONG,
            quantity=Quantity.from_int(position_qty), report_id=UUID4(), ts_last=1, ts_init=1)])
    return ms


def _snapshot(cache):
    return (
        sorted((str(o.strategy_id), tuple(o.tags or ()), str(o.status), str(o.side), str(o.quantity))
               for o in cache.orders()),
        sorted((str(p.instrument_id), str(p.side), str(p.quantity)) for p in cache.positions()),
    )


def test_filter_makes_parsed_reports_inert_without_positions():
    loop = asyncio.new_event_loop()
    eng, cache = _engine(loop, filter_unclaimed_external_orders=True)
    assert eng._reconcile_execution_mass_status(_mass_status(True, None)) is True
    assert cache.orders() == [] and cache.positions() == []


def test_filter_is_differential_noop_with_a_broker_position():
    """With a broker position report (existing behaviour: NT aligns via a RECONCILIATION
    fill) the parsed order/fill reports add nothing on top of it."""
    loop = asyncio.new_event_loop()
    a, cache_a = _engine(loop, filter_unclaimed_external_orders=True)
    b, cache_b = _engine(loop, filter_unclaimed_external_orders=True)
    a._reconcile_execution_mass_status(_mass_status(True, 3))
    b._reconcile_execution_mass_status(_mass_status(False, 3))
    assert _snapshot(cache_a) == _snapshot(cache_b)
    # no TS-<id> external order and nothing tagged VENUE
    orders, positions = _snapshot(cache_a)
    assert not any(o.client_order_id.value.startswith("TS-") for o in cache_a.orders())
    assert not any("VENUE" in o[1] for o in orders)
    assert [o[1] for o in orders] == [("RECONCILIATION",)]  # the pre-existing position alignment only


def test_control_without_filter_would_create_external_orders():
    """Proves the differential test has teeth: the default config DOES adopt them."""
    loop = asyncio.new_event_loop()
    eng, cache = _engine(loop)  # filter_unclaimed_external_orders defaults False
    eng._reconcile_execution_mass_status(_mass_status(True, None))
    assert len(cache.orders()) > 0
    assert all(str(o.strategy_id) == "EXTERNAL" for o in cache.orders())


# ---------------------------------------------------------------------------
# Own cached orders: the adapter must withhold reports (Task 14 fix round 1)
# ---------------------------------------------------------------------------
import copy
from unittest.mock import AsyncMock, MagicMock

from nautilus_trader.common.factories import OrderFactory
from nautilus_trader.model.enums import OrderSide as _OS
from nautilus_trader.model.identifiers import StrategyId, VenueOrderId
from nautilus_trader.model.objects import Price
from nautilus_trader.test_kit.stubs.events import TestEventStubs

from tradestation_nt_community.execution import TradeStationExecutionClient

OWN_TS_ID = "286179863"  # SPEC_OCO_LIMIT_SELL OrderID


def _own_order_engine(loop):
    clock = LiveClock()
    trader = TraderId("TESTER-001")
    cache = Cache()
    cache.add_instrument(TSTestInstrumentStubs.gc_futures_contract())
    eng = LiveExecutionEngine(loop=loop, msgbus=MessageBus(trader_id=trader, clock=clock), cache=cache,
                              clock=clock, config=LiveExecEngineConfig(
                                  reconciliation=True, filter_unclaimed_external_orders=True))
    order = OrderFactory(trader_id=trader, strategy_id=StrategyId("S-001"), clock=clock).limit(
        IID, _OS.SELL, Quantity.from_int(10), Price.from_str("230.0"))
    cache.add_order(order)
    order.apply(TestEventStubs.order_submitted(order))
    order.apply(TestEventStubs.order_accepted(order, venue_order_id=VenueOrderId(OWN_TS_ID)))
    cache.update_order(order)
    return eng, cache, order


def _adapter_stub(cache, ts_orders):
    stub = MagicMock()
    stub._client.get_orders = AsyncMock(return_value=ts_orders)
    stub._account_id = "SIM0000001F"
    stub._account_id_nautilus = ACCT
    stub._clock.timestamp_ns = lambda: 1
    stub._cache = cache
    stub._log = MagicMock()
    stub._ts_order_id_to_client_order_id = {}
    stub._client_order_id_to_ts_order_id = {}
    stub._parse_order_status = lambda s: __import__(
        "tradestation_nt_community.parsing.execution", fromlist=["x"]).parse_order_status(s)
    stub._parse_order_status_report = lambda o, i, c: parse_order_status_report(o, i, c, ACCT, 1)
    stub._is_own_cached_order = lambda c, t=None: TradeStationExecutionClient._is_own_cached_order(stub, c, t)
    stub._cancel_was_requested = lambda c: False
    stub._inflight_status_report = lambda i, c, t: TradeStationExecutionClient._inflight_status_report(stub, i, c, t)
    return stub


def _cmds(coid):
    from types import SimpleNamespace as NS
    return (NS(instrument_id=None, open_only=False, start=None, end=None),
            NS(instrument_id=None, venue_order_id=None, start=None, end=None),
            NS(instrument_id=IID, client_order_id=coid, venue_order_id=VenueOrderId(OWN_TS_ID)))


@pytest.mark.parametrize("status", ["FLL", "CAN", "OUT", "DON", "UCN"])
def test_own_cached_order_gets_no_reports_and_no_events(status):
    loop = asyncio.new_event_loop()
    eng, cache, order = _own_order_engine(loop)
    ts = copy.deepcopy(SPEC_OCO_LIMIT_SELL)
    ts["Status"] = status
    if status == "FLL":
        ts["Legs"][0]["ExecQuantity"] = "10"
        ts["FilledPrice"] = "230"
        ts["ClosedDateTime"] = "2020-11-17T17:00:00Z"
    stub = _adapter_stub(cache, [ts])
    stub._ts_order_id_to_client_order_id = {OWN_TS_ID: order.client_order_id}
    stub._client_order_id_to_ts_order_id = {order.client_order_id: OWN_TS_ID}
    c_orders, c_fills, c_single = _cmds(order.client_order_id)
    run = loop.run_until_complete
    reports = run(TradeStationExecutionClient.generate_order_status_reports(stub, c_orders))
    fills = run(TradeStationExecutionClient.generate_fill_reports(stub, c_fills))
    single = run(TradeStationExecutionClient.generate_order_status_report(stub, c_single))
    assert reports == [] and fills == []
    # F4: the single-order (in-flight) query now reads the broker; the mass-status paths stay empty.
    assert single is not None and single.client_order_id == order.client_order_id

    n_events, st = order.event_count, order.status
    ms = _EMS(client_id=ClientId("TRADESTATION"), account_id=ACCT, venue=Venue("TRADESTATION"),
              report_id=UUID4(), ts_init=0)
    ms.add_order_reports(reports=reports)
    ms.add_fill_reports(reports=fills)
    eng._reconcile_execution_mass_status(ms)
    assert order.event_count == n_events and order.status == st == cache.order(order.client_order_id).status
    assert len(cache.orders()) == 1


def test_control_unguarded_reports_would_move_the_own_order():
    """Teeth: feeding the parsed FLL report for the own order DOES change NT state."""
    loop = asyncio.new_event_loop()
    eng, cache, order = _own_order_engine(loop)
    ts = copy.deepcopy(SPEC_OCO_LIMIT_SELL)
    ts["Status"] = "FLL"
    ts["Legs"][0]["ExecQuantity"] = "10"
    ts["FilledPrice"] = "230"
    rep = parse_order_status_report(ts, IID, order.client_order_id, ACCT, 1)
    n = order.event_count
    eng._reconcile_order_report(rep, [])
    assert order.event_count > n
