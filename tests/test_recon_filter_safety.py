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
