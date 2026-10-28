"""
TradeStation execution client implementation.
"""

import asyncio
import json
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import httpx
import pandas as pd

from tradestation_nt_community._attribution import order_attribution
from tradestation_nt_community.common import order_status as _st
from tradestation_nt_community.http.client import BrokerReadError
from tradestation_nt_community.http.client import DuplicateOrderConfirmIdException
from tradestation_nt_community.http.client import OrderGroupPartialException
from tradestation_nt_community.http.client import TradeStationHttpClient
from tradestation_nt_community.parsing.execution import convert_order_to_ts_format
from tradestation_nt_community.parsing.execution import convert_order_type
from tradestation_nt_community.parsing.execution import convert_time_in_force
from tradestation_nt_community.parsing.execution import convert_order_list_to_ts_group
from tradestation_nt_community.parsing.execution import event_ts
from tradestation_nt_community.parsing.execution import parse_fill_report
from tradestation_nt_community.parsing.execution import parse_order_status
from tradestation_nt_community.parsing.execution import parse_order_status_report
from tradestation_nt_community.parsing.execution import parse_ts_order_type
from tradestation_nt_community.parsing.execution import ts_fill_price
from tradestation_nt_community.providers import TradeStationInstrumentProvider
from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import MessageBus
from nautilus_trader.common.enums import LogColor
from nautilus_trader.core.datetime import dt_to_unix_nanos
from nautilus_trader.core.datetime import millis_to_nanos
from nautilus_trader.core.datetime import secs_to_nanos
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.execution.messages import BatchCancelOrders
from nautilus_trader.execution.messages import CancelAllOrders
from nautilus_trader.execution.messages import CancelOrder
from nautilus_trader.execution.messages import ModifyOrder
from nautilus_trader.execution.messages import QueryOrder
from nautilus_trader.execution.messages import SubmitOrder
from nautilus_trader.execution.messages import GenerateFillReports
from nautilus_trader.execution.messages import GenerateOrderStatusReport
from nautilus_trader.execution.messages import GenerateOrderStatusReports
from nautilus_trader.execution.messages import GeneratePositionStatusReports
from nautilus_trader.execution.messages import SubmitOrderList
from nautilus_trader.execution.reports import ExecutionMassStatus
from nautilus_trader.execution.reports import FillReport
from nautilus_trader.execution.reports import OrderStatusReport
from nautilus_trader.execution.reports import PositionStatusReport
from nautilus_trader.live.execution_client import LiveExecutionClient
from nautilus_trader.model.enums import AccountType
from nautilus_trader.model.enums import LiquiditySide
from nautilus_trader.model.enums import OmsType
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import PositionSide
from nautilus_trader.model.enums import OrderStatus
from nautilus_trader.model.enums import OrderType
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.identifiers import AccountId
from nautilus_trader.model.identifiers import ClientId
from nautilus_trader.model.identifiers import ClientOrderId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import PositionId
from nautilus_trader.model.identifiers import TradeId
from nautilus_trader.model.identifiers import VenueOrderId
from nautilus_trader.model.objects import AccountBalance
from nautilus_trader.model.objects import Currency
from nautilus_trader.model.objects import MarginBalance
from nautilus_trader.model.objects import Money
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity
from nautilus_trader.model.orders import LimitOrder
from nautilus_trader.model.orders import MarketOrder
from nautilus_trader.model.orders import Order
from nautilus_trader.model.orders import StopLimitOrder
from nautilus_trader.model.orders import StopMarketOrder


# §151: tolerance band around the previous process's last-alive stamp (see
# classify_uncached_fill). Absorbs the 60 s stamp resolution and clock skew.
_ALIVE_TOLERANCE_S = 60.0
_ALIVE_STAMP_EVERY_S = 60.0
_ORDER_MAP_META = "_meta"

# Failures after which the POST may have reached TradeStation: the request was written and
# the answer lost. Connect/pool failures mean it was never sent (strategies LESSONS §207).
_AMBIGUOUS_SUBMIT_ERRORS = (
    httpx.ReadTimeout, httpx.WriteTimeout, httpx.RemoteProtocolError, httpx.ReadError,
    DuplicateOrderConfirmIdException,
)


def _is_ambiguous_submit(e: BaseException) -> bool:
    """True when a failed submit may still have placed the order at TradeStation."""
    return isinstance(e, _AMBIGUOUS_SUBMIT_ERRORS)


# The reject reason of an ambiguous submit starts with this marker, so a consumer can tell
# "may be live at TradeStation" from a real reject and never resend it (strategies
# OPEN_POINTS 1.7). Byte-stable: the strategies runner matches on it.
SUBMIT_AMBIGUOUS_REASON_PREFIX = "SUBMIT-AMBIGUOUS: "


def _reject_reason(e: BaseException) -> str:
    """The NT reject reason for a failed submit: ``str(e)``, marked when ambiguous (an
    httpx timeout's ``str`` can be empty, so the type always names it then)."""
    if _is_ambiguous_submit(e):
        return f"{SUBMIT_AMBIGUOUS_REASON_PREFIX}{type(e).__name__}: {e}"[:500]
    return str(e)


def classify_uncached_fill(closed_s, instance_started_s, entry_group, node_group,
                           alive_prev_s) -> str:
    """§151 classification of an FLL/FLP on a mapped TS order absent from the NT cache.

    Returns one of:
      ``live``            fill at/after this instance started, order owned by this group
      ``downtime``        fill between the group's last-alive stamp (-60 s) and this
                          instance's start: nobody was alive to deliver it
      ``delivered``       fill well before the group's last-alive stamp: the previous
                          process (alive then) delivered it
      ``delivered_other`` the order belongs to the other node's group; its own adapter
                          classifies it
      ``unknown``         anything we cannot place (no/unparseable ClosedDateTime, no
                          group info on the order or this node, no alive stamp): fail loud

    A fill was delivered only if the process that OWNED the order was alive when it
    filled — the old "predates this instance" test missed the case where the owner was
    down, and the case where the "previous process" was not the owner at all.
    """
    if closed_s is None or instance_started_s is None:
        return "unknown"
    if not entry_group or not node_group:
        return "unknown"
    if entry_group != node_group:
        return "delivered_other"
    if closed_s >= instance_started_s:
        return "live"
    if alive_prev_s is None:
        return "unknown"
    if closed_s < alive_prev_s - _ALIVE_TOLERANCE_S:
        return "delivered"
    return "downtime"


def _uncached_fill_message(ts_order_id: str, client_order_id, tag: str = "[UNCACHED-FILL-LIVE]",
                           note: str = "") -> str:
    """Greppable warning for a fill on a mapped order that NT's cache does not hold.

    Happens for orders placed before a node restart: the persisted order map knows
    them, but startup reconciliation does not rebuild them into the NT cache
    (2026-09-28 NQZ26 #972721176), so the fill cannot be delivered to the strategy.
    """
    return (
        f"{tag} TS order {ts_order_id} ({client_order_id}) filled but is not "
        f"in the NT cache (placed before a restart?){note} — the strategy will not see this "
        f"fill; HEAL CASE-A/PERIODIC-RECON must reconcile"
    )


def _log_fill_after_cancel(log, status, ts_order_id, client_order_id, cached_order) -> None:
    """§149 observe-only: the broker filled an order NT already closed as
    CANCELED/EXPIRED/REJECTED (REJECTED: a late-accepted submit, [LATE-ACCEPT-CANCELLED])
    — the position may be untracked.  No fill is emitted."""
    if status in _st.FILLED and cached_order.status in (
            OrderStatus.CANCELED, OrderStatus.EXPIRED, OrderStatus.REJECTED):
        log.error(
            f"[FILL-AFTER-CANCEL] TS order {ts_order_id} ({client_order_id}, "
            f"{cached_order.instrument_id.symbol.value}) filled at the broker but "
            f"NT closed it as {cached_order.status.name} — position may be "
            f"untracked (§149)"
        )


def _log_uncached_fill(log, ts_order: dict, ts_order_id: str, client_order_id,
                       instance_started_s, entry_group=None, node_group=None,
                       alive_prev_s=None, handed_to_runner=False, tag_group=None) -> str:
    """Log (and classify) an FLL/FLP fill for a mapped TS order absent from the NT cache.

    2026-09-29 13:23Z/13:28Z: after a restart the persisted order map is loaded but
    `_order_last_status` starts empty, so the first status read sees orders that
    filled HOURS EARLIER in the previous process — whose fills were already
    delivered at the time. §151 classifies via classify_uncached_fill and returns
    the kind. WARNING tags: `[UNCACHED-FILL-LIVE]` (live/unknown) and
    `[DOWNTIME-FILL]`; the INFO lines (delivered / other node) carry the bare
    `[UNCACHED-FILL]` text, which the agent forwarder must NOT match.
    """
    closed_s = None
    closed_str = ts_order.get("ClosedDateTime", "")
    if closed_str:
        try:
            closed_s = pd.Timestamp(closed_str, tz="UTC").timestamp()
        except Exception:
            closed_s = None
    kind = classify_uncached_fill(closed_s, instance_started_s, entry_group, node_group,
                                  alive_prev_s)
    _tag_note = (f", tagged group {tag_group} (this node {node_group}): hosting "
                 f"decides" if tag_group and node_group and tag_group != node_group
                 else "")
    if kind == "delivered":
        log.info(
            f"[UNCACHED-FILL] TS order {ts_order_id} ({client_order_id}) fill predates "
            f"this session{_tag_note} — already delivered by the previous process"
        )
    elif kind == "delivered_other":
        log.info(
            f"[UNCACHED-FILL] TS order {ts_order_id} ({client_order_id}) owned by the "
            f"other node (group {entry_group}) — its adapter classifies this fill"
        )
    elif handed_to_runner:
        # A runner callback is registered: it resolves the owner (§233: by hosting, the
        # tag only breaks ties) and is the sole alerter. INFO + bare tag so the log
        # forwarder (which matches only the WARN tags) stays silent.
        log.info(
            f"[UNCACHED-FILL] TS order {ts_order_id} ({client_order_id}) {kind} fill, "
            f"not in the NT cache{_tag_note} — handed to the runner"
        )
    elif kind == "downtime":
        log.warning(_uncached_fill_message(
            ts_order_id, client_order_id, "[DOWNTIME-FILL]",
            f" (filled while group {node_group} was not running)"))
    else:
        log.warning(_uncached_fill_message(
            ts_order_id, client_order_id, "[UNCACHED-FILL-LIVE]",
            " (attribution unknown)" if kind == "unknown" else ""))
    return kind


def _str_or_none(v):
    return v if isinstance(v, str) and v else None


def _float_or_none(v):
    return float(v) if isinstance(v, (int, float)) and not isinstance(v, bool) else None


def _dec_or_none(v) -> "Decimal | None":
    """Decimal of a TS numeric string, or None when blank/unparseable."""
    if v is None or isinstance(v, bool) or v == "":
        return None
    try:
        d = Decimal(str(v))
    except Exception:
        return None
    return d if d.is_finite() else None


def _executed_total(ts_order: dict) -> "Decimal | None":
    """Total executed qty of a TS order: ``Legs[*].ExecQuantity`` summed over legs
    (v3 shape), else the legacy top-level ``FilledQuantity``. ``None`` when the order
    carries no parseable executed quantity (never falls back to the ordered qty)."""
    total = None
    legs = ts_order.get("Legs")
    if isinstance(legs, list):
        for leg in legs:
            if isinstance(leg, dict):
                q = _dec_or_none(leg.get("ExecQuantity"))
                if q is not None:
                    total = (total or Decimal(0)) + q
    if total is None:
        total = _dec_or_none(ts_order.get("FilledQuantity"))
    return total


def _event_ts(ts_order: dict, ts_now: int) -> int:
    return event_ts(ts_order, ts_now)


def _resolve_fill_price(client, ts_order: dict, order, client_order_id) -> "float | None":
    """THE price resolver (SSE, poll, FLP and the in-flight report). A lost fill is worse
    than an approximate price: TS fields first (AveragePrice/FilledPrice, then
    ``Legs[0].ExecutionPrice``), then the order's own stop trigger / limit price with a
    ``[FILL-PRICE-FALLBACK]`` WARNING. ``None`` only if all of them are missing."""
    px = ts_fill_price(ts_order)
    if px is not None:
        return px
    own = None
    if order is not None:
        own = getattr(order, "trigger_price", None) or getattr(order, "price", None)
    try:
        own_px = float(str(own)) if own is not None else 0.0
    except (TypeError, ValueError):
        own_px = 0.0
    if own_px > 0:
        client._log.warning(
            f"[FILL-PRICE-FALLBACK] {client_order_id}: no usable fill price in the TS order; "
            f"using the order's own price {own_px}"
        )
        return own_px
    return None


# §241: monotonic clock alias — tests patch ``execution._mono``, never ``time.monotonic``
# (asyncio's loop clock uses it).
_mono = time.monotonic


@dataclass
class _AmendMeta:
    """§241: bookkeeping for an amend whose PUT failed ambiguously (5xx / exception).

    ``sent``/``stamp``: ``_mono()`` when the PUT left / when it failed. Recovery reads
    must START after ``stamp``. ``prev``: the (trigger_price, price) the order had before
    the modify (None for a field not being modified). ``old_reads``: consecutive
    non-overlapping reads that still showed ``prev``; ``last_read_done``: when the last
    counted read was processed. ``warned``: the one [MODIFY-UNKNOWN] WARNING was logged."""
    sent: float
    stamp: float
    prev: tuple
    old_reads: int = 0
    last_read_done: float = float("-inf")
    warned: bool = False


def _modify_inflight_begin(client, client_order_id) -> None:
    """§241: one more PUT of this order is in flight (per-coid counter: overlapping
    modifies of one order must not clear each other's flag)."""
    client._modify_inflight[client_order_id] = (
        client._modify_inflight.get(client_order_id, 0) + 1)


def _modify_inflight_end(client, client_order_id) -> None:
    n = client._modify_inflight.get(client_order_id, 0) - 1
    if n > 0:
        client._modify_inflight[client_order_id] = n
    else:
        client._modify_inflight.pop(client_order_id, None)


def _drop_pending_amend(client, client_order_id) -> None:
    """§241: forget a pending amend and its meta (every former bare pop goes through here)."""
    client._pending_modify_trigger_price.pop(client_order_id, None)
    client._pending_amend_meta.pop(client_order_id, None)


def _pending_modify_prices(entry) -> tuple:
    """The (trigger_price, price) an amend pending after an ambiguous PUT failure (§241)
    requested. ``entry`` is the stored
    tuple, or a bare value (a trigger price only, the pre-§212 shape), or None."""
    if isinstance(entry, tuple):
        return (entry + (None, None))[:2]
    return entry, None


def _modify_prices_match(ts_order: dict, trigger_price, price) -> bool:
    """§212: True when the broker order carries EVERY price the modify requested —
    StopPrice == trigger_price (when requested) and LimitPrice == price (when requested).
    Nothing requested, or a requested field missing/unparseable at the broker -> False
    (the recovery classifies the read as 'old' or 'neither', §241)."""
    wanted = [(v, f) for v, f in ((trigger_price, "StopPrice"), (price, "LimitPrice"))
              if v is not None]
    if not wanted:
        return False
    for v, f in wanted:
        raw = ts_order.get(f)
        try:
            if raw in (None, "") or abs(float(v) - float(raw)) >= 1e-6:
                return False
        except (TypeError, ValueError):
            return False
    return True


# §241 amend recovery after an ambiguous PUT failure. NT's own in-flight check queries a
# PENDING_UPDATE order every ~5-6 s and, after inflight_check_retries (runner: 20), fabricates
# OrderCanceled (~100-125 s) — so every pending amend is decided by the deadline below.
_MODIFY_OLD_READS_TO_REJECT = 2      # consecutive non-overlapping reads showing the OLD price
_MODIFY_REJECT_MIN_AGE_S = 20.0      # ...and at least this long after the failure
_MODIFY_RECOVERY_DEADLINE_S = 60.0   # from PUT send: old -> reject, neither -> broker prices
_MODIFY_HARD_DEADLINE_S = 90.0       # from PUT send: decide even a very late failure (NT: ~126 s)


def _classify_modify_read(ts_order: dict, requested: tuple, prev: tuple) -> str:
    """§241: 'new' when the broker shows every requested price, 'old' when it shows the
    previous value of every requested field (previous known), else 'neither' (incl. a
    STOP_LIMIT with one field moved, a missing/unparseable field, an unknown previous)."""
    tp, px = requested
    if _modify_prices_match(ts_order, tp, px):
        return "new"
    ptp = prev[0] if tp is not None else None
    ppx = prev[1] if px is not None else None
    if ((tp is None or ptp is not None) and (px is None or ppx is not None)
            and _modify_prices_match(ts_order, ptp, ppx)):
        return "old"
    return "neither"


def _decide_amend(meta: "_AmendMeta", kind: str, read_started: float, now: float) -> str:
    """§241: what one broker read means for a pending amend. Pure apart from meta counters.

    'skip'          the read started before the PUT failed: its prices predate the outcome
    'updated'       broker has the requested prices
    'wait_old'      broker still has the previous prices; not yet N reads / min age
    'reject'        old after N non-overlapping reads and min age, or old at the deadline
                    (deadline = 60 s from send, but not before stamp + min age,
                    unless 90 s from send)
    'wait_unknown'  broker shows neither; keep pending (one WARNING, logged by the caller)
    'adopt_broker'  neither at the deadline: make NT match the broker's actual prices
    """
    if read_started <= meta.stamp:
        return "skip"
    if kind == "new":
        return "updated"
    # The deadline runs from the send but not before the failure had its settling time (a
    # PUT that fails late still waits stamp + min age), capped by a hard ceiling so even a
    # very late failure is decided before NT's own resolver fabricates a cancel.
    since_send = now - meta.sent
    past_deadline = (since_send >= _MODIFY_RECOVERY_DEADLINE_S
                     and (now - meta.stamp >= _MODIFY_REJECT_MIN_AGE_S
                          or since_send >= _MODIFY_HARD_DEADLINE_S))
    if kind == "old":
        if read_started > meta.last_read_done:          # overlapping reads count once
            meta.old_reads += 1
            meta.last_read_done = now
        if past_deadline or (meta.old_reads >= _MODIFY_OLD_READS_TO_REJECT
                             and now - meta.stamp >= _MODIFY_REJECT_MIN_AGE_S):
            return "reject"
        return "wait_old"
    meta.old_reads = 0                                   # "consecutive" old reads
    meta.last_read_done = now
    return "adopt_broker" if past_deadline else "wait_unknown"


def _broker_prices(client, cached_order, ts_order: dict, requested: tuple):
    """§241: the broker's actual (trigger, price) for the requested fields as NT Prices
    at the instrument's precision; None when the instrument or a value is unusable."""
    inst = client._cache.instrument(cached_order.instrument_id)
    if inst is None:
        return None
    out = []
    for want, field in zip(requested, ("StopPrice", "LimitPrice")):
        if want is None:
            out.append(None)
            continue
        raw = ts_order.get(field)
        try:
            out.append(inst.make_price(float(raw)))
        except (TypeError, ValueError):
            return None
    return tuple(out)


def _recover_amend(client, cached_order, client_order_id, ts_order_id: str,
                   ts_order: dict, read_started: float) -> None:
    """§241: resolve (or keep) one pending amend from one broker read."""
    requested = _pending_modify_prices(
        client._pending_modify_trigger_price.get(client_order_id))
    meta = client._pending_amend_meta.get(client_order_id)
    if meta is None:  # defensive: an amend recorded without meta decides from now on
        meta = _AmendMeta(sent=_mono(), stamp=float("-inf"), prev=(None, None))
        client._pending_amend_meta[client_order_id] = meta
    kind = _classify_modify_read(ts_order, requested, meta.prev)
    now = _mono()
    decision = _decide_amend(meta, kind, read_started, now)
    broker = (f"broker StopPrice={ts_order.get('StopPrice')} "
              f"LimitPrice={ts_order.get('LimitPrice')}")
    asked = (f"requested trigger={requested[0]} price={requested[1]}, "
             f"previous trigger={meta.prev[0]} price={meta.prev[1]}")
    age = f"{now - meta.stamp:.0f}s after the failed PUT"
    common = dict(strategy_id=cached_order.strategy_id,
                  instrument_id=cached_order.instrument_id,
                  client_order_id=client_order_id,
                  venue_order_id=VenueOrderId(ts_order_id),
                  ts_event=client._clock.timestamp_ns())
    if decision == "skip":
        return
    if decision == "updated":
        client.generate_order_updated(quantity=cached_order.quantity, price=requested[1],
                                      trigger_price=requested[0], **common)
        _drop_pending_amend(client, client_order_id)
        client._log.info(f"[MODIFY-RECOVERY] {client_order_id} amend landed: {broker}; "
                         f"{asked} ({age}) -> OrderUpdated")
    elif decision == "wait_old":
        client._log.info(f"[MODIFY-RECOVERY] {client_order_id} broker still shows the "
                         f"previous price (read {meta.old_reads}/"
                         f"{_MODIFY_OLD_READS_TO_REJECT}, {age}): {broker}; {asked} - waiting")
    elif decision == "wait_unknown":
        if not meta.warned:
            meta.warned = True
            client._log.warning(f"[MODIFY-UNKNOWN] {client_order_id} broker shows neither "
                                f"the requested nor the previous price: {broker}; {asked} "
                                f"({age}) - kept pending, decided by "
                                f"{_MODIFY_RECOVERY_DEADLINE_S:.0f}s after send")
    elif decision == "adopt_broker":
        adopted = _broker_prices(client, cached_order, ts_order, requested)
        if adopted is not None:
            client.generate_order_updated(quantity=cached_order.quantity, price=adopted[1],
                                          trigger_price=adopted[0], **common)
            _drop_pending_amend(client, client_order_id)
            client._log.error(f"[MODIFY-UNKNOWN] {client_order_id} deadline: NT now "
                              f"carries the broker's prices ({broker}); {asked} "
                              f"-> OrderUpdated")
            return
        # Unusable broker price at the deadline: the outcome is UNKNOWN. Reject (the safe
        # direction), but say so and never claim the broker shows the previous price.
        client.generate_order_modify_rejected(
            reason=(f"modify recovery: outcome unknown at deadline, broker price unusable "
                    f"({broker}; requested trigger={requested[0]} price={requested[1]}, "
                    f"previous trigger={meta.prev[0]} price={meta.prev[1]})")[:200],
            **common)
        _drop_pending_amend(client, client_order_id)
        client._log.error(f"[MODIFY-UNKNOWN] {client_order_id} deadline: broker price "
                          f"unusable ({broker}); {asked} ({age}) -> OrderModifyRejected")
        return
    if decision == "reject":
        client.generate_order_modify_rejected(
            reason=(f"modify recovery: broker still shows the previous price after an "
                    f"ambiguous PUT ({broker}; requested trigger={requested[0]} "
                    f"price={requested[1]})")[:200],
            **common)
        _drop_pending_amend(client, client_order_id)
        client._log.warning(f"[MODIFY-RECOVERY] {client_order_id} amend did not land: "
                            f"{broker}; {asked} ({age}) -> OrderModifyRejected")


def _track_partial(client, ts_order: dict, client_order_id, ts_now: int) -> None:
    """FPR (partial, still working): remember it per own order, WARN once, emit no NT
    event. The runner reads ``partial_orders()`` to decide when to stop waiting."""
    partial = getattr(client, "_partial_orders", None)
    if not isinstance(partial, dict):
        return
    exec_int = int(_executed_total(ts_order) or 0)
    prev = partial.get(client_order_id)
    if prev is None:
        partial[client_order_id] = (exec_int, ts_now)
        legs = ts_order.get("Legs")
        ordered = 0
        for leg in (legs if isinstance(legs, list) else []):
            try:
                ordered += int(float((leg or {}).get("QuantityOrdered") or 0))
            except (TypeError, ValueError, AttributeError):
                pass
        client._log.warning(
            f"[PARTIAL-FILL] {client_order_id} {exec_int}/{ordered if ordered else '?'}"
        )
    elif prev[0] != exec_int:
        partial[client_order_id] = (exec_int, prev[1])  # keep since_ns, update qty


def _clear_partial(client, client_order_id) -> None:
    partial = getattr(client, "_partial_orders", None)
    if isinstance(partial, dict):
        partial.pop(client_order_id, None)


def _clear_emitted_hwm(client, client_order_id) -> None:
    """Drop the emitted-exec high-water mark (M-7). Called only once NT's cache shows the
    order CLOSED (the events have been applied): clearing at emit time would reopen the
    same-drain window the mark exists to close."""
    hwm = getattr(client, "_emitted_exec_hwm", None)
    if isinstance(hwm, dict):
        hwm.pop(client_order_id, None)
    canceled = getattr(client, "_emitted_cancel_coids", None)
    if isinstance(canceled, dict):
        canceled.pop(client_order_id, None)


def _claim_cancel_emit(client, client_order_id) -> bool:
    """True if no OrderCanceled was emitted for ``client_order_id`` yet, and records it.

    §225: the REST cancel response and the stream/poll OUT for the same own cancel race;
    the first to arrive emits, the other is skipped. NT applies events later (queued), so
    ``cached_order.is_closed`` cannot be the guard. Check-and-add with no ``await`` in
    between: atomic on the event loop. Insertion-ordered dict so the cap drops the oldest.
    """
    sent = getattr(client, "_emitted_cancel_coids", None)
    if not isinstance(sent, dict):
        return True
    if client_order_id in sent:
        return False
    sent[client_order_id] = None
    while len(sent) > 2000:   # bounded backstop; normally cleared once NT shows it closed
        sent.pop(next(iter(sent)))
    return True


def _release_if_terminal_closed(client, status: str, client_order_id) -> None:
    """M-5: the unchanged-status early exit precedes the is_closed branch that clears the
    emitted-exec bookkeeping, so after FLL/CAN it stayed until the FIFO backstop. Clear it
    as soon as NT shows the order closed."""
    if status not in _st.FILLED and status not in _st.CANCELED and status not in _st.REJECTED:
        return
    try:
        cached = client._cache.order(client_order_id)
    except Exception:
        return
    if cached is not None and cached.is_closed:
        _clear_emitted_hwm(client, client_order_id)


def _emit_fill_delta(client, ts_order: dict, ts_order_id: str, client_order_id, cached_order,
                     venue_order_id, ts_now: int, source: str, *, partial: bool):
    """F1 + price fallback + F3: emit ONE OrderFilled for the executed quantity not yet
    applied to the NT order (delta vs ``cached_order.filled_qty``). Shared by the SSE
    and poll paths, FLL and FLP.

    Returns ``(emitted, exec_total, reason)``. ``exec_total`` is ``None`` when an FLP
    carries no executed qty (caller keeps the ``[FLP-NO-QTY]`` pending path). ``reason`` is
    ``"no_price"`` when no price could be resolved at all (ERROR ``[FILL-NO-PRICE]``): the
    callers then un-advance ``_order_last_status`` so the next poll retries, and an FLP
    does NOT cancel its remainder. Never emits qty <= 0 or px 0."""
    log = client._log
    exec_total = _executed_total(ts_order)
    if partial and (exec_total is None or exec_total <= 0):
        return False, None, ""
    if exec_total is None or exec_total <= 0:
        # FLL without a parseable executed qty: the order is fully filled.
        exec_total = cached_order.quantity.as_decimal()
    try:
        already = cached_order.filled_qty.as_decimal()
    except Exception:
        already = Decimal(0)
    # M-7: NT's live exec engine enqueues events, so ``cached_order.filled_qty`` lags a fill
    # we just emitted. A second path (SSE FLP then a poll CAN-with-exec in the same drain)
    # would re-emit the same delta: remember the highest executed qty WE emitted per order.
    hwm_map = getattr(client, "_emitted_exec_hwm", None)
    hwm_map = hwm_map if isinstance(hwm_map, dict) else None
    hwm = hwm_map.get(client_order_id, Decimal(0)) if hwm_map is not None else Decimal(0)
    delta = exec_total - max(already, hwm)
    if delta <= 0:
        log.info(
            f"[FILL-DELTA] {client_order_id}: executed {exec_total} already applied "
            f"({already}) - no fill emitted"
        )
        return False, exec_total, "already_applied"
    px = _resolve_fill_price(client, ts_order, cached_order, client_order_id)
    if px is None:
        log.error(
            f"[FILL-NO-PRICE] {client_order_id} (TS {ts_order_id}): executed {delta} but no "
            f"price anywhere (TS fields or the order's own) - fill NOT emitted, the next "
            f"poll retries (raw={ts_order})"
        )
        return False, exec_total, "no_price"
    instrument = client._cache.instrument(cached_order.instrument_id)
    prec = getattr(instrument, "price_precision", None)
    if isinstance(prec, int) and not isinstance(prec, bool):
        fill_px = Price(round(px, prec), prec)
        quote = instrument.quote_currency
    else:
        fill_px = Price.from_str(str(px))
        quote = Currency.from_str("USD")
    qty = Quantity.from_str(format(delta, "f"))
    sent = getattr(client, "_cancel_after_ack_sent", None)
    if isinstance(sent, dict) and client_order_id in sent:
        # §166 residual A, observe-only: the strategy cancelled this order before the ack
        # and has untracked it, but it filled in the ack -> cancel window. The fill is
        # still emitted; the position may now differ from what the strategy believes.
        log.error(
            f"[FILL-AFTER-ACK-CANCEL] {client_order_id} (TS {ts_order_id}) filled {delta} @ "
            f"{px} after its pre-ack cancel was sent - the strategy had dropped it; "
            f"check the position (LESSONS §166 residual A)"
        )
    client.generate_order_filled(
        strategy_id=cached_order.strategy_id,
        instrument_id=cached_order.instrument_id,
        client_order_id=client_order_id,
        venue_order_id=venue_order_id,
        venue_position_id=None,
        trade_id=TradeId(f"{ts_order_id}-{ts_now}"),
        order_side=cached_order.side,
        order_type=cached_order.order_type,
        last_qty=qty,
        last_px=fill_px,
        quote_currency=quote,
        commission=Money(Decimal("0"), Currency.from_str("USD")),
        liquidity_side=LiquiditySide.NO_LIQUIDITY_SIDE,
        ts_event=_event_ts(ts_order, ts_now),
    )
    if hwm_map is not None:
        hwm_map[client_order_id] = exec_total
        while len(hwm_map) > 2000:  # bounded backstop; normally cleared on terminal
            hwm_map.pop(next(iter(hwm_map)))
    log.info(
        f"Order filled: {client_order_id} @ {fill_px} qty={qty} [fill-source={source}]",
        LogColor.GREEN,
    )
    return True, exec_total, ""


def _apply_status_tail(client, status: str, ts_order: dict, ts_order_id: str,
                       client_order_id, cached_order, venue_order_id, ts_now: int,
                       *, stream: bool) -> None:
    """Non-FLL status dispatch shared by the SSE path (_process_order_event) and the
    poll path (_check_order_statuses).  Module-level (like _handle_uncached_fill) so
    both paths call one body.  Terminal REJ/CAN-class codes emit NT events; DON and
    held (WATCH/open/unknown/FLP) codes emit nothing and go to ``_watch_status``."""
    lead = "Stream: order" if stream else "Order"
    log = client._log
    if status in _st.REJECTED:
        reason = ts_order.get("RejectReason", f"TradeStation status: {status}")
        client.generate_order_rejected(
            strategy_id=cached_order.strategy_id,
            instrument_id=cached_order.instrument_id,
            client_order_id=client_order_id,
            reason=reason,
            ts_event=_event_ts(ts_order, ts_now),
        )
        _clear_partial(client, client_order_id)
        log.error(f"{lead} rejected: {client_order_id} ({reason})")

    elif status in _st.CANCELED and status not in _st.WATCH:
        # EXP (Expired) is reported as canceled, not OrderExpired: this adapter has
        # no generate_order_expired path anywhere, and the §111 cancel verifier plus
        # strategies only know the canceled event.  Reports for EXP map to
        # OrderStatus.EXPIRED (parsing/execution.py) on reconciliation.
        # A cancel with executed qty not yet applied (partial-then-cancelled reported as
        # CAN/OUT/EXP instead of FLP): the fill delta goes FIRST, or NT closes the order
        # with filled_qty 0 and the contracts exist only at the broker.
        exec_c = _executed_total(ts_order)
        try:
            filled_c = cached_order.filled_qty.as_decimal()
        except Exception:
            filled_c = Decimal(0)
        if exec_c is not None and exec_c > filled_c:
            try:
                _em, _tot, _why = _emit_fill_delta(
                    client, ts_order, ts_order_id, client_order_id, cached_order,
                    venue_order_id, ts_now, "cancel-with-exec", partial=True)
            except Exception as e:
                log.error(f"[CANCEL-FILL-ERROR] {client_order_id}: fill emit failed: {e} - "
                          f"order left open, the next poll retries")
                client._order_last_status.pop(ts_order_id, None)
                return
            if _why == "no_price":
                client._order_last_status.pop(ts_order_id, None)  # retry; cancel not sent
                return
            log.warning(f"[CANCEL-WITH-EXEC] {client_order_id}: {status} with {exec_c} "
                        f"executed - fill delivered before the cancel")
        if not _claim_cancel_emit(client, client_order_id):
            # Already queued, NT has not applied it yet: FLP-then-CAN in one drain (the FLP
            # path emitted the remainder cancel) or the REST cancel response (§225). A
            # second would be an InvalidStateTrigger warning and a second strategy event.
            log.info(f"[CANCEL-DUP] {client_order_id}: OrderCanceled already emitted - "
                     f"not repeated")
        else:
            client.generate_order_canceled(
                strategy_id=cached_order.strategy_id,
                instrument_id=cached_order.instrument_id,
                client_order_id=client_order_id,
                venue_order_id=venue_order_id,
                ts_event=_event_ts(ts_order, ts_now),
            )
        _clear_partial(client, client_order_id)
        client._register_cancel_verify(ts_order_id, requested=False)  # §111
        _drop_pending_amend(client, client_order_id)  # no stale entry
        log.info(f"{lead} canceled: {client_order_id} (status={status})")

    elif status == "DON":
        log.info(f"{lead} {client_order_id} status=DON (Queued) — session pause, kept open")

    elif status == "FPR":
        # Partial fill, still working: monitored, no NT event (the runner decides).
        _track_partial(client, ts_order, client_order_id, ts_now)

    elif status in _st.OPEN or status in _st.CANCELED:
        # R24: CANCELED∩WATCH codes land here too (held, no NT event).
        # R25: our own in-flight cancel's echo is expected, not watched.
        if status in _st.WATCH and cached_order.status != OrderStatus.PENDING_CANCEL:
            client._watch_status(ts_order_id, client_order_id, status)

    elif status in _st.FILLED:
        # Reached only for non-FLL filled codes (FLP = partial fill, UROut: terminal).
        # Fill the executed delta, then cancel the remainder. Never a new order.
        try:
            emitted, exec_total, reason = _emit_fill_delta(
                client, ts_order, ts_order_id, client_order_id, cached_order,
                venue_order_id, ts_now, "flp", partial=True)
        except Exception as e:  # never break the poll loop; HEAL/RECON recover the fill
            log.error(f"[FLP-FILL-ERROR] {client_order_id}: fill emit failed: {e} - "
                      f"order left open, the next poll retries")
            client._order_last_status.pop(ts_order_id, None)  # un-advance: retry
            return
        if exec_total is None:
            client._watch_status(
                ts_order_id, client_order_id, status,
                note="[FLP-NO-QTY] (Partial Fill UROut) without an executed qty - "
                     "left to the runner pending path")
            return
        if reason == "no_price":
            # Keep the order open and retry on the next status read: cancelling now would
            # close it with the executed qty never applied.
            client._order_last_status.pop(ts_order_id, None)
            return
        _clear_partial(client, client_order_id)
        remainder = cached_order.quantity.as_decimal() - exec_total
        if remainder > 0 and _claim_cancel_emit(client, client_order_id):
            client.generate_order_canceled(
                strategy_id=cached_order.strategy_id,
                instrument_id=cached_order.instrument_id,
                client_order_id=client_order_id,
                venue_order_id=venue_order_id,
                ts_event=_event_ts(ts_order, ts_now),
            )
            _drop_pending_amend(client, client_order_id)
            log.warning(
                f"[FLP-TERMINAL] {client_order_id}: {exec_total} filled, remainder "
                f"{remainder} canceled (fill {'emitted' if emitted else 'NOT emitted'})"
            )

    else:
        client._watch_status(
            ts_order_id, client_order_id, status or "UNKNOWN",
            note=f"unknown TS status {status or 'UNKNOWN'!r}")


def _stamp_group_if_missing(client, ts_order_id) -> None:
    """Final-review fix: a legacy order-map entry (no ``group``) whose order is in THIS
    process's NT cache was submitted by this process, so stamp the current node group
    (in memory; _persist_order_map writes it). Shrinks the ``unknown`` fill population."""
    try:
        node_group = _str_or_none(getattr(client, "_node_group", None))
        groups = getattr(client, "_ts_id_to_group", None)
        if node_group and isinstance(groups, dict) and not groups.get(ts_order_id):
            groups[ts_order_id] = node_group
    except Exception:
        pass


def _handle_uncached_fill(client, ts_order: dict, ts_order_id: str, client_order_id) -> str:
    """Classify + log an uncached fill, then notify the registered callback (§151).

    Both fill paths (poll + SSE) call this. The callback fires for every kind except
    ``delivered``; ``delivered_other`` is only produced when no callback is registered
    (§233). It is scheduled with ``call_soon`` and wrapped so it can never break the
    status poll. The callback runs ON the loop, so it must
    not block: the runner offloads HTTP/Telegram to executors. Returns the kind.
    """
    node_group = _str_or_none(getattr(client, "_node_group", None))
    groups = getattr(client, "_ts_id_to_group", None)
    entry_group = _str_or_none(groups.get(ts_order_id)) if isinstance(groups, dict) else None
    alive_map = getattr(client, "_previous_alive", None)
    alive_prev = (_float_or_none(alive_map.get(node_group))
                  if isinstance(alive_map, dict) and node_group else None)
    cb = getattr(client, "_uncached_fill_callback", None)
    if not callable(cb):
        cb = None
    # §233 one ownership rule: with a runner callback, the runner's hosting test decides
    # who owns an uncached fill; the order-map tag (the node that SUBMITTED the order,
    # write-once) only goes along as a tiebreak. A kept exit keeps its submit-time tag
    # after its strategy moves node (2026-10-25 btc #976210241: tag stable, hosted on
    # rolling), so deciding by the tag left the fill with no handler. Time the fill
    # against THIS node's own alive stamp, exactly like an own-tagged order (R3).
    timing_group = entry_group
    if cb is not None and entry_group and node_group and entry_group != node_group:
        timing_group = node_group
    kind = _log_uncached_fill(
        client._log, ts_order, ts_order_id, client_order_id,
        getattr(client, "_instance_started_s", None),
        entry_group=timing_group, node_group=node_group, alive_prev_s=alive_prev,
        handed_to_runner=cb is not None, tag_group=entry_group,
    )
    if kind in ("delivered", "delivered_other"):
        return kind
    if cb is None:
        return kind
    leg = (ts_order.get("Legs") or [{}])[0] if isinstance(ts_order, dict) else {}
    payload = {
        "ts_order_id": str(ts_order_id),
        "client_order_id": str(client_order_id),
        "status": ts_order.get("Status"),
        "closed": ts_order.get("ClosedDateTime"),
        "kind": kind,
        "exec_qty": leg.get("ExecQuantity") if isinstance(leg, dict) else None,
        "group": entry_group,
        "tag_mismatch": bool(timing_group != entry_group),
    }

    tag = "[DOWNTIME-FILL]" if kind == "downtime" else "[UNCACHED-FILL-LIVE]"

    def _fail(why: str, exc) -> None:
        # The INFO downgrade made the runner the sole alerter, so a failure of that path
        # must surface with a FORWARDED tag (WARN), not an ERROR-only line.
        try:
            client._log.warning(
                f"{tag} TS order {ts_order_id} ({client_order_id}) {why}: "
                f"{type(exc).__name__}: {exc} — the fill was NOT processed by the runner; "
                f"check /positions and /orders"
            )
        except Exception:
            pass

    def _run():
        try:
            cb(payload)
        except Exception as exc:  # never raise into the loop
            _fail("callback failed", exc)

    try:
        client._loop.call_soon(_run)
    except Exception as exc:
        _fail("could not schedule callback", exc)
    return kind


class TradeStationExecutionClient(LiveExecutionClient):
    """
    Provide an execution client for TradeStation.

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
        The instrument provider for the client.
    account_id : str
        The TradeStation account ID to use for trading.
    base_url_ws : str | None, optional
        The WebSocket base URL (for future streaming implementation).

    """

    # §111: post-cancel verification (QMU26/RTYU26/CLQ26 — TS sim confirmed the
    # cancel but kept the order live on its book).
    _CANCEL_VERIFY_MIN_AGE_S = 120.0
    _CANCEL_VERIFY_MAX_ATTEMPTS = 3
    _CANCEL_STUCK_ALERT_S = 600.0  # §150: requested cancel still UCN/LAT this long -> alert once

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        client: TradeStationHttpClient,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
        instrument_provider: TradeStationInstrumentProvider,
        account_id: str,
        base_url_ws: str | None = None,
        use_streaming: bool = False,
        streaming_reconnect_delay_secs: float = 5.0,
        streaming_status_poll_secs: float = 60.0,
        extended_hours: bool = False,
        order_map_path: str | None = None,
        node_group: str | None = None,
        report_own_orders: bool = False,
    ) -> None:
        super().__init__(
            loop=loop,
            client_id=ClientId("TRADESTATION"),
            venue=None,  # Multi-venue support
            oms_type=OmsType.NETTING,  # Futures: one net position per instrument
            instrument_provider=instrument_provider,
            account_type=AccountType.MARGIN,
            base_currency=Currency.from_str("USD"),
            msgbus=msgbus,
            cache=cache,
            clock=clock,
        )

        self._client = client
        self._account_id = account_id
        self._base_url_ws = base_url_ws

        # Wall-clock start of this adapter instance (UTC epoch seconds). Used to tell
        # a fill that happened after this process started from one that predates it
        # (already delivered by the previous process, before a restart) — see
        # _log_uncached_fill.
        self._instance_started_s = time.time()

        # Order tracking
        self._ts_order_id_to_client_order_id: dict[str, ClientOrderId] = {}
        self._client_order_id_to_ts_order_id: dict[ClientOrderId, str] = {}

        # Fill detection — polling or streaming
        self._order_last_status: dict[str, str] = {}   # ts_order_id → last seen status

        # Amend recovery (§212/§241). An entry exists ONLY after a modify PUT failed
        # ambiguously (5xx / exception): the broker may or may not have applied it and NT
        # sits in PENDING_UPDATE. _check_order_statuses resolves it from broker reads that
        # started after the failure. (trigger_price, price) requested; meta in
        # _pending_amend_meta. PUTs in flight are counted in _modify_inflight and are never
        # "recovered" — their own answer decides.
        self._pending_modify_trigger_price: dict[ClientOrderId, tuple] = {}
        self._pending_amend_meta: dict[ClientOrderId, _AmendMeta] = {}
        self._modify_inflight: dict[ClientOrderId, int] = {}
        self._fill_poll_task: asyncio.Task | None = None
        self._fill_poll_interval: float = 5.0  # seconds between order status polls
        self._token_keepalive_task: asyncio.Task | None = None
        self._TOKEN_KEEPALIVE_SECS: float = 600.0  # 10 min; TS tokens expire in 20 min
        # §95: streaming-mode safety net — the SSE stream can go one-way-dead
        # (heartbeats keep the connection alive while fill events are dropped),
        # so a low-frequency status poll runs alongside it.  0 disables.
        self._status_safety_poll_secs: float = streaming_status_poll_secs
        self._status_safety_poll_task: asyncio.Task | None = None

        # §111: post-cancel zombie verification — orders this session already
        # reported cancelled, checked against the safety poll's own order fetch.
        # In-memory only: cross-restart zombies remain the agent orphan scan's job.
        self._cancel_verify_pending: dict[str, tuple[float, int, bool, bool]] = {}
        # coid -> None, insertion-ordered, bounded: cancels this process SENT (attempted),
        # kept past the §111 registry's lifetime so report paths can tell "we asked" from
        # "the broker cancelled on its own" (F4).
        self._cancel_requested_coids: dict = {}
        # coid -> None, insertion-ordered, bounded: a cancel arrived BEFORE TradeStation
        # acknowledged the order (no TS id yet). The cancel is rejected to NT as before, and
        # sent for real as soon as the submit maps the TS id (_cancel_after_ack). Without
        # this the order lived on at the broker (fc_5_699 #974504866, 2026-10-12: 8 min).
        self._cancel_after_ack_coids: dict = {}
        # coid -> None, bounded: orders _cancel_after_ack actually cancelled (measurement).
        self._cancel_after_ack_sent: dict = {}
        # coid -> (executed qty, since_ns): own orders sitting in FPR (partial, working).
        self._partial_orders: dict = {}
        # coid -> highest executed qty an NT fill was emitted for (M-7 same-drain dedup).
        self._emitted_exec_hwm: dict = {}
        # coids whose OrderCanceled was already emitted: FLP remainder (M-3), REST cancel
        # response vs stream OUT (§225). Insertion-ordered dict, see _claim_cancel_emit.
        self._emitted_cancel_coids: dict = {}
        # ts_order_id -> (monotonic first-seen, status, client_order_id) — §149 recheck
        self._status_watch: dict[str, tuple[float, str, str]] = {}

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

        # Order-map persistence (T-2)
        self._order_map_path: str | None = order_map_path

        # §151: which node group (stable/rolling) this adapter serves. Stamped on every
        # order-map entry it submits and used to key the last-alive stamp.
        self._node_group: str | None = node_group or None
        # Act-mode flag (later plan): defined, unused. Shadow mode logs only.
        self._report_own_orders: bool = bool(report_own_orders)
        self._shadow_own_sig: dict = {}   # coid -> last logged shadow signature (dedup)
        self._ts_id_to_group: dict[str, str] = {}      # ts_order_id -> submitting group
        # group -> last-alive epoch of the PREVIOUS process
        self._previous_alive: dict[str, float] = {}
        # stamp only after the previous value is read
        self._alive_loaded = False
        self._last_alive_stamp_s = 0.0
        self._uncached_fill_callback = None

        # Account setup
        self._account_id_nautilus = AccountId(f"{self.id}-{account_id}")


    def set_uncached_fill_callback(self, callback) -> None:
        """§151: register ``callback(payload: dict)`` for a genuine uncached fill.

        Called (via ``loop.call_soon``, exceptions swallowed) for kinds live /
        downtime / unknown — not for fills the previous process delivered. With a
        callback registered an order-map tag of the other node no longer short-circuits
        (§233): the fill is timed against this node's own alive stamp and handed over.
        Payload: ts_order_id, client_order_id, status, closed, kind, exec_qty, group
        (the order-map tag or None), tag_mismatch (tag present and != this node's group).
        Mirrors ``TradeStationHttpClient.set_token_rotation_callback``.
        """
        self._uncached_fill_callback = callback

    def _read_order_map_file(self) -> dict:
        import json as _json
        try:
            with open(self._order_map_path, "r") as _f:
                raw = _json.load(_f)
            return raw if isinstance(raw, dict) else {}
        except Exception:
            return {}

    def _write_order_map_file(self, data: dict) -> None:
        import json as _json, os as _os
        tmp = self._order_map_path + f".{_os.getpid()}.tmp"
        with open(tmp, "w") as _f:
            _json.dump(data, _f)
        _os.replace(tmp, self._order_map_path)

    def _stamp_alive(self, force: bool = False) -> None:
        """§151: record "this group was alive at T" in the order map (``_meta.alive``).

        Read-merge-replace (same race profile as _persist_order_map: worst case one
        stale stamp, which classifies as downtime/unknown — the loud side). Throttled to
        once per _ALIVE_STAMP_EVERY_S; ``force`` for _disconnect. Never stamps before the
        previous process's value has been read by _load_order_map.
        """
        if not self._order_map_path or not self._node_group or not self._alive_loaded:
            return
        import time as _time
        now_s = _time.time()
        if not force and now_s - self._last_alive_stamp_s < _ALIVE_STAMP_EVERY_S:
            return
        try:
            data = self._read_order_map_file()
            meta = data.get(_ORDER_MAP_META)
            if not isinstance(meta, dict):
                meta = {}
            alive = meta.get("alive")
            if not isinstance(alive, dict):
                alive = {}
            alive[self._node_group] = now_s
            meta["alive"] = alive
            data[_ORDER_MAP_META] = meta
            self._write_order_map_file(data)
            self._last_alive_stamp_s = now_s
        except Exception as exc:
            self._log.warning(f"[ORDER-MAP] Failed to stamp alive: {exc}")

    def _persist_order_map(self) -> None:
        """Persist _ts_order_id_to_client_order_id to disk for cross-restart recovery."""
        if not self._order_map_path:
            return
        import json as _json, time as _time, os as _os
        try:
            now_s = _time.time()
            # Merge with existing file to preserve entries from both directions
            try:
                with open(self._order_map_path, "r") as _f:
                    existing = _json.load(_f)
            except Exception:
                existing = {}
            # Update with current in-memory entries (add submitted_ts if missing)
            for ts_id, coid in self._ts_order_id_to_client_order_id.items():
                # §151: merge into the existing entry so per-entry fields written by
                # either node (``group``) survive; a brand-new entry was submitted by
                # THIS node, so it gets this node's group.
                prev = existing.get(ts_id)
                entry = dict(prev) if isinstance(prev, dict) else {}
                entry["client_order_id"] = str(coid)
                entry["submitted_ts"] = entry.get("submitted_ts", now_s)
                grp = self._ts_id_to_group.get(ts_id)
                if grp is None and not entry.get("group") and not isinstance(prev, dict):
                    grp = self._node_group
                    if grp:
                        self._ts_id_to_group[ts_id] = grp
                if grp and not entry.get("group"):
                    entry["group"] = grp
                existing[ts_id] = entry
            tmp = self._order_map_path + f".{_os.getpid()}.tmp"
            with open(tmp, "w") as _f:
                _json.dump(existing, _f)
            _os.replace(tmp, self._order_map_path)
        except Exception as exc:
            self._log.warning(f"[ORDER-MAP] Failed to persist order map: {exc}")

    def _load_order_map(self) -> None:
        """Load persisted order map at startup; prune entries older than 7 days."""
        if not self._order_map_path:
            return
        import json as _json, time as _time
        try:
            with open(self._order_map_path, "r") as _f:
                raw = _json.load(_f)
        except FileNotFoundError:
            self._alive_loaded = True   # no previous process -> no alive stamp; ours may start
            return
        except Exception as exc:
            self._log.warning(f"[ORDER-MAP] Failed to load order map: {exc}")
            self._alive_loaded = True
            return
        cutoff = _time.time() - 7 * 86400
        loaded = pruned = 0
        # §151: previous process's last-alive stamps (before this instance overwrites them)
        meta = raw.get(_ORDER_MAP_META)
        if isinstance(meta, dict) and isinstance(meta.get("alive"), dict):
            for g, t in meta["alive"].items():
                try:
                    self._previous_alive[str(g)] = float(t)
                except (TypeError, ValueError):
                    pass
        self._alive_loaded = True
        for ts_id, entry in raw.items():
            if ts_id == _ORDER_MAP_META or not isinstance(entry, dict):
                continue
            if entry.get("submitted_ts", 0) < cutoff:
                pruned += 1
                continue
            try:
                coid = ClientOrderId(entry["client_order_id"])
                self._ts_order_id_to_client_order_id[ts_id] = coid
                self._client_order_id_to_ts_order_id[coid] = ts_id
                if entry.get("group"):
                    self._ts_id_to_group[ts_id] = str(entry["group"])
                loaded += 1
            except Exception:
                pass
        self._log.info(f"[ORDER-MAP] Loaded {loaded} order ID mappings ({pruned} pruned >7d)")

    async def _connect(self) -> None:
        """Connect to TradeStation."""
        self._log.info("Connecting to TradeStation...")

        # Test authentication by fetching account info
        try:
            accounts = await self._client.get_accounts()
            self._log.info(f"Successfully authenticated. Found {len(accounts)} account(s)")

            # Fetch initial balances and positions
            await self._update_account_state()

        except Exception as e:
            self._log.error(f"Failed to connect to TradeStation: {e}")
            raise

        # Start background fill detection (streaming or polling)
        if self._use_streaming and self._stream_client:
            self._fill_poll_task = self._loop.create_task(
                self._stream_order_fills()
            )
            self._token_keepalive_task = self._loop.create_task(
                self._token_keepalive_loop()
            )
            self._log.info("Started order fill detection via SSE streaming", LogColor.GREEN)
            # §95: safety poll — recovers fills the SSE stream silently drops
            if self._status_safety_poll_secs > 0:
                self._status_safety_poll_task = self._loop.create_task(
                    self._status_safety_poll_loop()
                )
                self._log.info(
                    f"Started status safety poll (every {self._status_safety_poll_secs:.0f}s) "
                    f"— catches fills missed by a silently-dead SSE stream",
                    LogColor.GREEN,
                )
        else:
            self._fill_poll_task = self._loop.create_task(self._poll_order_fills())
            self._log.info(
                f"Started order fill polling (every {self._fill_poll_interval:.0f}s)",
                LogColor.GREEN,
            )

        self._load_order_map()
        self._log.info("Connected to TradeStation", LogColor.GREEN)

    async def _disconnect(self) -> None:
        """Disconnect from TradeStation."""
        self._log.info("Disconnecting from TradeStation...")
        self._stamp_alive(force=True)  # §151: graceful stop = exact last-alive time

        # Stop fill polling and token keepalive
        if self._fill_poll_task:
            self._fill_poll_task.cancel()
            try:
                await self._fill_poll_task
            except asyncio.CancelledError:
                pass
            self._fill_poll_task = None
        if self._token_keepalive_task:
            self._token_keepalive_task.cancel()
            try:
                await self._token_keepalive_task
            except asyncio.CancelledError:
                pass
            self._token_keepalive_task = None
        if self._status_safety_poll_task:
            self._status_safety_poll_task.cancel()
            try:
                await self._status_safety_poll_task
            except asyncio.CancelledError:
                pass
            self._status_safety_poll_task = None

        await self._client.close()
        self._log.info("Disconnected from TradeStation", LogColor.GREEN)

    # -- EXECUTION REPORTS ----------------------------------------------------------------

    def _is_own_cached_order(self, client_order_id, ts_order_id=None) -> bool:
        """True if NT already tracks this order in its cache (an order we own).

        Reports for such orders are withheld (None / skipped, exactly as when the
        parser could not read the v3 shape): NT's reconciliation would otherwise
        emit status events or FILL-<id> trades that race this adapter's own
        poll/SSE events (different TradeId scheme -> possible double fill).
        Logged at DEBUG once per order.
        """
        own = client_order_id is not None and self._cache.order(client_order_id) is not None
        if not own and ts_order_id:
            # A missing ts->coid mapping makes the caller mint ``TS-<id>``, which is
            # never in the cache: fall back to NT's venue-order-id index so the guard
            # cannot be bypassed for an order NT does track.
            try:
                cached_coid = self._cache.client_order_id(VenueOrderId(str(ts_order_id)))
            except Exception:
                cached_coid = None
            if cached_coid is not None and self._cache.order(cached_coid) is not None:
                client_order_id, own = cached_coid, True
        if not own:
            return False
        seen = self.__dict__.setdefault("_own_order_report_skip_logged", set())
        if client_order_id not in seen:
            seen.add(client_order_id)
            self._log.debug(
                f"Not reporting own cached order {client_order_id} "
                f"(ts_order_id={ts_order_id}) to NT reconciliation"
            )
        return True

    async def generate_order_status_report(
        self,
        command: GenerateOrderStatusReport,
    ) -> OrderStatusReport | None:
        """Generate an order status report for the given order."""
        instrument_id = command.instrument_id
        client_order_id = command.client_order_id
        venue_order_id = command.venue_order_id
        self._log.debug(
            f"Generating order status report for {client_order_id}"
            + (f" (venue_order_id={venue_order_id})" if venue_order_id else ""),
        )

        # Try to find TradeStation order ID
        ts_order_id = self._client_order_id_to_ts_order_id.get(client_order_id)

        if not ts_order_id and venue_order_id:
            ts_order_id = str(venue_order_id)

        if self._is_own_cached_order(client_order_id, ts_order_id):
            # F4: NT's in-flight check queries here. Answering None made it fabricate
            # OrderRejected (SUBMITTED) / OrderCanceled (PENDING_CANCEL) with no broker read.
            return await self._inflight_status_report(
                instrument_id, client_order_id, ts_order_id)

        if not ts_order_id:
            self._log.warning(f"Cannot find TradeStation order ID for {client_order_id}")
            return None

        # Fetch order from TradeStation
        try:
            orders = await self._client.get_orders_by_id(self._account_id, [ts_order_id])

            # Find matching order
            ts_order = None
            for order in orders:
                if order.get("OrderID") == ts_order_id:
                    ts_order = order
                    break

            if not ts_order:
                self._log.warning(f"Order {ts_order_id} not found at TradeStation")
                return None

            return self._parse_order_status_report(ts_order, instrument_id, client_order_id)

        except BrokerReadError as e:
            # §217 M10: the read FAILED (HTTP 200 + Errors / 5xx, after retries) - the order's
            # state is unknown, which is not "not found". Same None, its own line.
            self._log.error(
                f"[ORDER-STATUS-READ-FAILED] {client_order_id} (TS {ts_order_id}): {e} - "
                f"state unknown, no report"
            )
            return None
        except Exception as e:
            self._log.error(f"Failed to generate order status report: {e}")
            return None

    async def _inflight_status_report(
        self,
        instrument_id: InstrumentId,
        client_order_id: ClientOrderId | None,
        ts_order_id: str | None,
    ) -> OrderStatusReport | None:
        """F4: broker-read report for an own cached order that NT is chasing in-flight.

        Returns None (NT retries, ``inflight_check_retries``) when the id is unknown,
        the broker read fails or the order is not in the broker's list. Open-category
        codes (incl. FPR and the held WATCH codes) report ACCEPTED, FLL FILLED, FLP
        PARTIALLY_FILLED with the executed qty, REJ REJECTED. A broker CAN/OUT/EXP is
        reported CANCELED/EXPIRED only if we requested the cancel; otherwise ACCEPTED
        with a ``[CANCEL-MISMATCH]`` log, as the live status paths treat it.
        """
        if not ts_order_id:
            return None
        try:
            orders = await self._client.get_orders_by_id(self._account_id, [ts_order_id])
        except BrokerReadError as e:
            # §217 M10: TS answered with an error (HTTP 200 + Errors / 5xx, after retries):
            # the order's state is unknown - never "absent". NT retries (None).
            self._log.error(
                f"[INFLIGHT-QUERY-READ-FAILED] {client_order_id} (TS {ts_order_id}): {e} - "
                f"state unknown, no report, NT will retry"
            )
            return None
        except Exception as e:
            self._log.error(
                f"[INFLIGHT-QUERY] broker read failed for {client_order_id} "
                f"(TS {ts_order_id}): {e} - no report, NT will retry"
            )
            return None
        ts_order = next((o for o in orders if o.get("OrderID") == ts_order_id), None)
        if ts_order is None:
            self._log.warning(
                f"[INFLIGHT-QUERY] {client_order_id} (TS {ts_order_id}) not in the broker "
                f"order list - no report, NT will retry"
            )
            return None
        if client_order_id is None:
            client_order_id = self._ts_order_id_to_client_order_id.get(ts_order_id)
        status = ts_order.get("Status", "")
        rep = dict(ts_order)
        cached = self._cache.order(client_order_id) if client_order_id is not None else None
        terminal = (status in _st.REJECTED or status in _st.FILLED
                    or (status in _st.CANCELED and status not in _st.WATCH))
        if (not terminal and cached is not None
                and cached.status == OrderStatus.PENDING_UPDATE
                and client_order_id in self._modify_inflight):
            # §241: the modify PUT has not answered yet. ACCEPTED would put NT back on the OLD
            # price; a recovery run would compare against a broker that may not have the
            # amend yet. No report: NT retries, and the PUT's own answer decides.
            self._log.info(
                f"[INFLIGHT-QUERY] {client_order_id} PENDING_UPDATE with a modify PUT in "
                f"flight - no report, NT will retry"
            )
            return None
        if (not terminal and cached is not None
                and cached.status == OrderStatus.PENDING_UPDATE
                and client_order_id in self._pending_modify_trigger_price):
            # I-2: an amend whose PUT failed ambiguously is being recovered (§241).
            # Answering ACCEPTED here would move the order out of PENDING_UPDATE and leave
            # NT on the OLD trigger price. Run the recovery (compares the broker's StopPrice
            # with the requested one) right now instead of waiting for the 60 s safety
            # poll, and let NT retry.
            self._log.info(
                f"[INFLIGHT-QUERY] {client_order_id} PENDING_UPDATE with a pending amend - "
                f"running the amend recovery instead of answering"
            )
            try:
                await self._check_order_statuses()
            except Exception as e:  # e.g. _verify_pending_cancels HTTP: NT just retries
                self._log.error(f"[INFLIGHT-QUERY] modify recovery failed for "
                                f"{client_order_id}: {e} - no report, NT will retry")
            return None
        if status in _st.REJECTED:
            pass
        elif status in _st.FILLED:
            # Never hand NT a FILLED/PARTIALLY_FILLED report with avg_px 0 (it would infer
            # a px-0 fill): resolve the price like every fill path, else no report.
            px = _resolve_fill_price(self, ts_order, cached, client_order_id)
            if px is None:
                self._log.error(
                    f"[FILL-NO-PRICE] {client_order_id} (TS {ts_order_id}) {status} in the "
                    f"in-flight query has no usable price - no report, NT will retry"
                )
                return None
            rep["FilledPrice"] = str(px)
        elif status in _st.CANCELED and status not in _st.WATCH:
            rep = self._requested_cancel_filter(rep, client_order_id, ts_order_id)
            if rep.get("Status") == status:  # still a cancel (requested)
                exec_c = _executed_total(rep)
                try:
                    filled_c = cached.filled_qty.as_decimal() if cached is not None else 0
                except Exception:
                    filled_c = Decimal(0)
                if exec_c is not None and exec_c > filled_c:
                    # NT's CANCELED branch ignores filled qty (it would close the order with
                    # filled_qty 0 and never fabricate-then-fix). Answer FPR-shaped instead:
                    # PARTIALLY_FILLED with the executed qty and the resolver price. NT
                    # applies the fill; the status path delivers the cancel afterwards.
                    px = _resolve_fill_price(self, ts_order, cached, client_order_id)
                    if px is None:
                        self._log.error(
                            f"[FILL-NO-PRICE] {client_order_id} (TS {ts_order_id}) {status} "
                            f"with {exec_c} executed in the in-flight query has no usable "
                            f"price - no report, NT will retry")
                        return None
                    self._log.warning(
                        f"[INFLIGHT-QUERY] {client_order_id} {status} with {exec_c} executed "
                        f"> filled {filled_c} - reporting PARTIALLY_FILLED {exec_c}; the "
                        f"status path delivers the cancel")
                    rep = dict(rep)
                    rep["Status"] = "FLP"
                    rep["FilledPrice"] = str(px)
        else:
            rep["Status"] = "ACK"  # open / held / unknown -> live (never invent a terminal)
        if rep.get("Status") in ("FLL", "FLP"):
            exec_total = _executed_total(rep)
            if exec_total is None or exec_total <= 0:
                self._log.warning(
                    f"[INFLIGHT-QUERY] {client_order_id} {rep.get('Status')} without an "
                    f"executed qty - no report, NT will retry"
                )
                return None
            # I-1: NT reconciles this report SYNCHRONOUSLY while our own generate_order_filled
            # is only ENQUEUED. If we already emitted this executed qty and NT has not applied
            # it yet, a report now makes NT infer the same fill a second time.
            hwm_map = getattr(self, "_emitted_exec_hwm", None)
            emitted = hwm_map.get(client_order_id) if isinstance(hwm_map, dict) else None
            # Gate on ANY emitted-but-unapplied qty (not only emitted >= exec): a second
            # partial can raise exec while the first emitted fill is still queued.
            if emitted is not None and cached is not None:
                try:
                    applied = cached.filled_qty.as_decimal()
                except Exception:
                    applied = Decimal(0)
                if applied < emitted:
                    self._log.info(
                        f"[INFLIGHT-QUERY] {client_order_id} fill for {exec_total} already "
                        f"emitted (queued, applied {applied}) - no report, NT will retry")
                    return None
        return self._parse_order_status_report(rep, instrument_id, client_order_id)

    def _requested_cancel_filter(self, rep: dict, client_order_id, ts_order_id) -> dict:
        """THE requested-cancel filter for every report path that may describe an OWN
        order (F4 in-flight query, shadow mode, any future act mode): a broker
        CAN/OUT/EXP maps to canceled only if THIS process asked for the cancel; otherwise
        the report says ACK (live) with a ``[CANCEL-MISMATCH]`` ERROR. Returns ``rep``
        (possibly a copy with Status ACK)."""
        status = rep.get("Status", "")
        if status in _st.CANCELED and status not in _st.WATCH:
            if not self._cancel_was_requested(client_order_id):
                self._log.error(
                    f"[CANCEL-MISMATCH] {client_order_id} (TS {ts_order_id}) is {status} at "
                    f"the broker but we never asked to cancel it - in-flight report says "
                    f"ACCEPTED, the status path decides"
                )
                rep = dict(rep)
                rep["Status"] = "ACK"
        return rep

    def _remember_shadow_sig(self, client_order_id, sig) -> None:
        sigs = self._shadow_own_sig
        sigs[client_order_id] = sig
        while len(sigs) > 2000:       # bounded: terminal orders age out FIFO
            sigs.pop(next(iter(sigs)))

    def _shadow_own_order(self, ts_order: dict, client_order_id, ts_order_id) -> None:
        """Shadow mode ``[RECON-SHADOW-OWN]`` (E2): called where the own-order guard
        WITHHOLDS a report. When the broker's state differs from NT's cache, log ONE JSON
        line (deduplicated per order until the signature changes). LOGS ONLY: never returns
        a report, never emits an NT event, never raises. ``report_own_orders`` (act mode)
        is intentionally not consulted."""
        try:
            cached = self._cache.order(client_order_id)
            if cached is None and ts_order_id:
                alt = self._cache.client_order_id(VenueOrderId(str(ts_order_id)))
                cached = self._cache.order(alt) if alt is not None else None
                client_order_id = alt or client_order_id
            if cached is None:
                return
            status = ts_order.get("Status", "")
            exec_qty = _executed_total(ts_order)
            try:
                filled = cached.filled_qty.as_decimal()
            except Exception:
                filled = Decimal(0)
            terminal = (status in _st.REJECTED or status in _st.FILLED
                        or (status in _st.CANCELED and status not in _st.WATCH))
            differs = ((exec_qty or Decimal(0)) != filled
                       or (terminal and not cached.is_closed)
                       or (not terminal and cached.is_closed))
            if not differs:
                self._shadow_own_sig.pop(client_order_id, None)
                return
            requested = self._cancel_was_requested(client_order_id)
            rec = {
                "event": "own_order_differs",
                "client_order_id": str(client_order_id),
                "ts_order_id": str(ts_order_id),
                "status": status,
                "cache_status": cached.status.name,
                "exec_qty": None if exec_qty is None else str(exec_qty),
                "filled_qty": str(filled),
                "avg_px_present": ts_fill_price(ts_order) is not None,
                "requested_cancel": bool(requested),
            }
            sig = json.dumps(rec, sort_keys=True, default=str)
            if self._shadow_own_sig.get(client_order_id) == sig:
                return
            self._remember_shadow_sig(client_order_id, sig)
            self._log.info("[RECON-SHADOW-OWN] " + sig)
        except Exception as e:  # shadow mode must never affect reconciliation
            self._log.debug(f"[RECON-SHADOW-OWN] skipped: {e}")

    async def generate_order_status_reports(
        self,
        command: GenerateOrderStatusReports,
    ) -> list[OrderStatusReport]:
        """Generate order status reports for all orders."""
        instrument_id = command.instrument_id
        open_only = command.open_only
        self._log.debug("Generating order status reports")

        try:
            orders = await self._client.get_orders(
                account_keys=self._account_id,
            )

            reports = []
            for ts_order in orders:
                # Map to client order ID if we have it
                ts_order_id = ts_order.get("OrderID")
                client_order_id = self._ts_order_id_to_client_order_id.get(ts_order_id)

                if not client_order_id:
                    # Create a client order ID for unknown orders
                    client_order_id = ClientOrderId(f"TS-{ts_order_id}")

                if self._is_own_cached_order(client_order_id, ts_order_id):
                    self._shadow_own_order(ts_order, client_order_id, ts_order_id)
                    continue

                # Get instrument ID — TS futures orders don't have a top-level Symbol;
                # the symbol is inside Legs[0]['Symbol']. Fall back to Legs if needed.
                # Skip only if genuinely no symbol can be found anywhere.
                symbol = ts_order.get("Symbol", "")
                if not symbol:
                    legs = ts_order.get("Legs", [])
                    if legs:
                        symbol = legs[0].get("Symbol", "")
                if not symbol:
                    self._log.warning(
                        f"Skipping order with empty Symbol during reconciliation: {ts_order}"
                    )
                    continue

                # OBSERVE-ONLY: emit a structured debug line for every broker order that
                # is (a) absent from the order-ID map AND (b) not in the NT cache.
                # This is the "adopted/external order" gap — _check_order_statuses double-
                # skips these orders, so fills on them are invisible to the safety poll.
                # Task 2-1: measure frequency; do NOT register anything or change behavior.
                if ts_order_id not in self._ts_order_id_to_client_order_id:
                    if self._cache.order(client_order_id) is None:
                        import json as _json
                        self._log.debug("[RECON-SHADOW] " + _json.dumps({
                            "event": "adopted_unmapped_order",
                            "ts_order_id": ts_order_id,
                            "minted_coid": str(client_order_id),
                            "symbol": symbol,
                            "status": ts_order.get("Status", ""),
                        }, sort_keys=True, default=str))

                order_instrument_id = instrument_id or InstrumentId.from_str(
                    f"{symbol}.TRADESTATION",
                )

                # Filter by instrument if specified
                if instrument_id and order_instrument_id != instrument_id:
                    continue

                # Filter by open only
                if open_only:
                    status = self._parse_order_status(ts_order.get("Status", ""))
                    if status not in (
                        OrderStatus.ACCEPTED,
                        OrderStatus.SUBMITTED,
                        OrderStatus.PARTIALLY_FILLED,
                    ):
                        continue

                report = self._parse_order_status_report(
                    ts_order,
                    order_instrument_id,
                    client_order_id,
                )
                if report:
                    reports.append(report)

            self._log.debug(f"Generated {len(reports)} order status reports")
            return reports

        except Exception as e:
            # §217 L4: [] is what NT needs on a failed read, but [] also means "the venue has
            # nothing" to NT's reconciliation. Safe only while these run at startup / on the
            # evidence-triggered recon: NEVER enable open_check_interval_secs (or reconnect
            # mass-status) while this returns [] on failure - NT would infer flattening fills.
            self._log.error(f"[RECON-REPORT-READ-FAILED] order status reports: {e} - "
                            f"returning [] (read failed, NOT an empty venue)")
            return []

    async def generate_fill_reports(
        self,
        command: GenerateFillReports,
    ) -> list[FillReport]:
        """Generate fill reports from filled orders.

        TradeStation has no dedicated fills endpoint — fills are embedded in
        order status. This method fetches the order list and builds a FillReport
        for every order with status FLL (fully filled).
        """
        instrument_id = command.instrument_id
        venue_order_id = command.venue_order_id
        start = command.start
        end = command.end

        self._log.debug("Generating fill reports")

        try:
            orders = await self._client.get_orders(
                account_keys=self._account_id,
            )

            ts_now = self._clock.timestamp_ns()
            reports: list[FillReport] = []

            for ts_order in orders:
                # Only process fully-filled orders
                if ts_order.get("Status") != "FLL":
                    continue

                # Resolve instrument ID from symbol / Legs fallback
                symbol = ts_order.get("Symbol", "")
                if not symbol:
                    legs = ts_order.get("Legs", [])
                    symbol = legs[0].get("Symbol", "") if legs else ""
                if not symbol:
                    continue

                order_instrument_id = InstrumentId.from_str(f"{symbol}.TRADESTATION")

                # Filter by instrument if requested
                if instrument_id and order_instrument_id != instrument_id:
                    continue

                # Filter by venue_order_id if requested
                if venue_order_id and ts_order.get("OrderID") != str(venue_order_id):
                    continue

                # Filter by time range if requested
                if start or end:
                    closed_str = ts_order.get("ClosedDateTime", "")
                    if closed_str:
                        order_ts = dt_to_unix_nanos(pd.Timestamp(closed_str, tz="UTC"))
                        if start and order_ts < dt_to_unix_nanos(pd.Timestamp(start)):
                            continue
                        if end and order_ts > dt_to_unix_nanos(pd.Timestamp(end)):
                            continue

                # Look up client order ID from our tracking dict
                ts_order_id = ts_order.get("OrderID")
                coid = self._ts_order_id_to_client_order_id.get(ts_order_id)
                if self._is_own_cached_order(coid, ts_order_id):
                    self._shadow_own_order(ts_order, coid, ts_order_id)
                    continue

                report = parse_fill_report(
                    ts_order,
                    instrument_id=order_instrument_id,
                    account_id=self._account_id_nautilus,
                    ts_now=ts_now,
                    client_order_id=coid,
                )
                if report:
                    reports.append(report)

            self._log.debug(f"Generated {len(reports)} fill report(s)")
            return reports

        except Exception as e:
            # §217 L4: [] is what NT needs on a failed read, but [] also means "the venue has
            # nothing" to NT's reconciliation. Safe only while these run at startup / on the
            # evidence-triggered recon: NEVER enable open_check_interval_secs (or reconnect
            # mass-status) while this returns [] on failure - NT would infer flattening fills.
            self._log.error(f"[RECON-REPORT-READ-FAILED] fill reports: {e} - "
                            f"returning [] (read failed, NOT an empty venue)")
            return []

    async def generate_position_status_reports(
        self,
        command: GeneratePositionStatusReports,
    ) -> list[PositionStatusReport]:
        """Generate position status reports."""
        instrument_id = command.instrument_id

        self._log.debug("Generating position status reports")

        try:
            positions = await self._client.get_positions(
                account_keys=self._account_id,
            )

            reports = []
            for ts_position in positions:
                symbol = ts_position.get("Symbol", "")
                position_instrument_id = InstrumentId.from_str(f"{symbol}.TRADESTATION")

                # Filter by instrument if specified
                if instrument_id and position_instrument_id != instrument_id:
                    continue

                # Parse position
                quantity_str = ts_position.get("Quantity", "0")
                quantity = Decimal(quantity_str)

                if quantity == 0:
                    continue  # Skip zero positions

                # Determine side — PositionSide not OrderSide
                position_side = PositionSide.LONG if quantity > 0 else PositionSide.SHORT

                avg_px_str = ts_position.get("AveragePrice") or "0"
                avg_px_open = Decimal(avg_px_str) if avg_px_str != "0" else None

                report = PositionStatusReport(
                    account_id=self._account_id_nautilus,
                    instrument_id=position_instrument_id,
                    position_side=position_side,
                    quantity=Quantity.from_str(str(abs(quantity))),
                    avg_px_open=avg_px_open,
                    report_id=UUID4(),
                    ts_last=self._clock.timestamp_ns(),
                    ts_init=self._clock.timestamp_ns(),
                )
                reports.append(report)

            self._log.debug(f"Generated {len(reports)} position status reports")
            return reports

        except Exception as e:
            # §217 L4: [] is what NT needs on a failed read, but [] also means "the venue has
            # nothing" to NT's reconciliation. Safe only while these run at startup / on the
            # evidence-triggered recon: NEVER enable open_check_interval_secs (or reconnect
            # mass-status) while this returns [] on failure - NT would infer flattening fills.
            self._log.error(f"[RECON-REPORT-READ-FAILED] position status reports: {e} - "
                            f"returning [] (read failed, NOT an empty venue)")
            return []

    # -- COMMAND HANDLERS -----------------------------------------------------------------

    async def _cancel_if_closed_before_accept(self, client_order_id, ts_order_id) -> bool:
        """A POST that succeeds AFTER NT already closed the order (e.g. the in-flight check
        resolved a slow SUBMITTED order as REJECTED) leaves a live broker order that NT
        believes rejected -> the strategy may enter again (duplicate position). Cancel it
        at the broker at once and alert; never emit OrderAccepted on a closed order.
        Returns True when the order was closed (caller must skip the accept)."""
        cached = self._cache.order(client_order_id) if self._cache is not None else None
        if cached is None or not cached.is_closed:
            return False
        self._log.error(
            f"[LATE-ACCEPT-CANCELLED] {client_order_id} accepted by TradeStation as "
            f"#{ts_order_id} after NT closed it ({cached.status_string()}) - cancelling at "
            f"the broker so no unknown live order remains"
        )
        try:
            await self._client.cancel_order(str(ts_order_id))
        except Exception as e:  # noqa: BLE001
            self._log.error(
                f"[LATE-ACCEPT-CANCELLED] {client_order_id} cancel of #{ts_order_id} "
                f"failed: {e} - cancel it manually (/orders cancel)"
            )
        return True

    def _remember_cancel_after_ack(self, client_order_id) -> None:
        """A cancel came before the TS ack: queue it for _cancel_after_ack (bounded)."""
        pend = getattr(self, "_cancel_after_ack_coids", None)
        if pend is None:
            pend = self._cancel_after_ack_coids = {}
        pend[client_order_id] = None
        while len(pend) > 500:
            pend.pop(next(iter(pend)))
        self._log.info(
            f"[CANCEL-AFTER-ACK] {client_order_id} cancel requested before TradeStation "
            f"acknowledged it - will cancel as soon as the order is accepted")

    def _remember_cancel_after_ack_sent(self, client_order_id) -> None:
        """Orders _cancel_after_ack cancelled (bounded): a fill on one of them is the §166
        residual A race, logged by _emit_fill_delta as [FILL-AFTER-ACK-CANCEL]."""
        sent = getattr(self, "_cancel_after_ack_sent", None)
        if not isinstance(sent, dict):
            sent = self._cancel_after_ack_sent = {}
        sent[client_order_id] = None
        while len(sent) > 500:
            sent.pop(next(iter(sent)))

    def _forget_cancel_after_ack(self, client_order_id) -> None:
        pend = getattr(self, "_cancel_after_ack_coids", None)
        if pend:
            pend.pop(client_order_id, None)

    async def _cancel_after_ack(self, client_order_id, ts_order_id) -> bool:
        """Send a cancel that was requested before the ack. Returns True if one was sent.

        Registered via _note_cancel_requested so the broker's CAN is ours (F4) and §111
        treats it as a requested cancel, never an unrequested one. NT already holds the
        order ACCEPTED (the pre-ack cancel was reported rejected), so the CAN that follows
        is a normal OrderCanceled. A fill that lands between the ack and this cancel is a
        known residual (exit-side twin of §159); the fill path still reports it.
        """
        pend = getattr(self, "_cancel_after_ack_coids", None)
        if not pend or client_order_id not in pend:
            return False
        pend.pop(client_order_id, None)
        self._note_cancel_requested(client_order_id)
        TradeStationExecutionClient._remember_cancel_after_ack_sent(self, client_order_id)
        self._log.info(
            f"[CANCEL-AFTER-ACK] {client_order_id} accepted as #{ts_order_id} - sending the "
            f"cancel that was requested before the ack")
        try:
            await self._client.cancel_order(str(ts_order_id))
        except Exception as e:  # noqa: BLE001
            if "Not an open order" in str(e):
                # Already gone: an OCO sibling cancelled with the first leg, or it filled
                # (the fill path reports that). Nothing left to cancel.
                self._log.info(
                    f"[CANCEL-AFTER-ACK] {client_order_id} #{ts_order_id} already closed")
                return True
            self._log.error(
                f"[CANCEL-AFTER-ACK] {client_order_id} cancel of #{ts_order_id} failed: {e} "
                f"- cancel it manually (/orders cancel)")
        return True

    async def _submit_order(self, command: SubmitOrder) -> None:
        """Submit an order to TradeStation."""
        order = command.order

        self._log.info(f"Submitting order {order.client_order_id}")

        try:
            # Convert Nautilus order to TradeStation format
            ts_order_params = self._convert_order_to_ts_format(order)

            # Submit order
            response = await self._client.place_order(
                **ts_order_params,
            )

            # Extract order ID from response
            ts_order_id = response.get("OrderID") or response.get("Orders", [{}])[0].get("OrderID")

            if not ts_order_id:
                raise ValueError(f"No OrderID in response: {response}")

            # Track order IDs
            self._ts_order_id_to_client_order_id[ts_order_id] = order.client_order_id
            self._client_order_id_to_ts_order_id[order.client_order_id] = ts_order_id
            self._persist_order_map()

            # Emit structured attribution log (Phase 0A)
            self._log.info(
                "[ORDER-SUBMIT] " + json.dumps(
                    {**order_attribution(order), "ts_order_id": ts_order_id},
                    sort_keys=True,
                    default=str,
                )
            )

            if await self._cancel_if_closed_before_accept(order.client_order_id, ts_order_id):
                return

            # Generate order accepted event
            self.generate_order_accepted(
                strategy_id=order.strategy_id,
                instrument_id=order.instrument_id,
                client_order_id=order.client_order_id,
                venue_order_id=VenueOrderId(ts_order_id),
                ts_event=self._clock.timestamp_ns(),
            )

            self._log.info(
                f"Order {order.client_order_id} accepted (TradeStation ID: {ts_order_id})",
                LogColor.GREEN,
            )
            await TradeStationExecutionClient._cancel_after_ack(
                self, order.client_order_id, ts_order_id)

        except Exception as e:
            TradeStationExecutionClient._forget_cancel_after_ack(self, order.client_order_id)
            if _is_ambiguous_submit(e):
                self._log.error(
                    f"[SUBMIT-AMBIGUOUS] {order.client_order_id} {order.instrument_id} "
                    f"{order.side.name} {order.quantity}: {type(e).__name__} after the request "
                    f"was sent - the order may be live at TradeStation; rejected in NT "
                    f"(check /orders)")
            self._log.error(f"Failed to submit order {order.client_order_id}: {e}")
            self.generate_order_rejected(
                strategy_id=order.strategy_id,
                instrument_id=order.instrument_id,
                client_order_id=order.client_order_id,
                reason=_reject_reason(e),
                ts_event=self._clock.timestamp_ns(),
            )

    async def _submit_order_list(self, command: SubmitOrderList) -> None:
        """Submit a list of orders as a TradeStation group order when possible.

        Detects OCO and bracket (OTO + OCO) patterns and routes them to
        ``POST /v3/orderexecution/ordergroups`` for atomic submission.
        Falls back to submitting orders individually when no group pattern
        is recognised (e.g. plain unlinked order lists).
        """
        orders = command.order_list.orders

        # Try group submission first
        group_result = convert_order_list_to_ts_group(orders, self._account_id)
        if group_result is not None:
            group_type, order_payloads = group_result
            await self._submit_order_group(
                command=command,
                group_type=group_type,
                order_payloads=order_payloads,
            )
            return

        # Fallback: submit each order individually
        self._log.info(
            f"OrderList {command.order_list.id} has no recognised group pattern "
            "— submitting orders individually"
        )
        for order in orders:
            submit_order = SubmitOrder(
                trader_id=command.trader_id,
                strategy_id=command.strategy_id,
                order=order,
                command_id=command.id,
                ts_init=command.ts_init,
                position_id=command.position_id,
            )
            await self._submit_order(submit_order)

    async def _submit_order_group(
        self,
        command: SubmitOrderList,
        group_type: str,
        order_payloads: list[dict],
    ) -> None:
        """Submit an OCO or bracket group to TradeStation and register all order IDs."""
        orders = command.order_list.orders
        self._log.info(
            f"Submitting {group_type} group for OrderList {command.order_list.id} "
            f"({len(orders)} legs)"
        )
        try:
            response = await self._client.place_order_group(
                group_type=group_type,
                orders=order_payloads,
            )

            # Extract individual order IDs from response
            # TS returns: {"OrderGroupId": "...", "Orders": [{"OrderID": "...", ...}, ...]}
            # The HTTP client raises OrderGroupPartialException unless every leg landed
            ts_orders = response.get("Orders", [])

            legs = []
            for i, (order, ts_order_resp) in enumerate(zip(orders, ts_orders)):
                ts_order_id = ts_order_resp.get("OrderID")
                if not ts_order_id:
                    self._log.warning(f"No OrderID in group response leg {i}: {ts_order_resp}")
                    continue

                # Register in tracking dicts
                self._ts_order_id_to_client_order_id[ts_order_id] = order.client_order_id
                self._client_order_id_to_ts_order_id[order.client_order_id] = ts_order_id
                self._persist_order_map()
                legs.append((i, order, ts_order_id))

            # Late accept of a group (NT already closed a leg, e.g. in-flight resolved it as
            # REJECTED): a TS cancel of one OCO leg cancels its siblings too, so accepting the
            # open siblings would leave NT believing in exits that no longer exist. The whole
            # group failed: cancel every leg at the broker, reject the open legs in NT, alert.
            late = [(i, o, t) for (i, o, t) in legs
                    if (c := self._cache.order(o.client_order_id)) is not None and c.is_closed]
            if late:
                self._log.error(
                    f"[LATE-ACCEPT-CANCELLED] {late[0][1].client_order_id} accepted by "
                    f"TradeStation as #{late[0][2]} after NT closed it (group of {len(legs)} "
                    f"legs) - cancelling every leg at the broker; open legs rejected in NT")
                await TradeStationExecutionClient._cancel_group_legs_and_reject(
                    self, command, [(o, t) for (_i, o, t) in legs], "LATE-ACCEPT-CANCELLED",
                    "late accept: group cancelled at the broker")
                return

            for i, order, ts_order_id in legs:
                # Generate accepted event for each leg
                self.generate_order_accepted(
                    strategy_id=command.strategy_id,
                    instrument_id=order.instrument_id,
                    client_order_id=order.client_order_id,
                    venue_order_id=VenueOrderId(ts_order_id),
                    ts_event=self._clock.timestamp_ns(),
                )
                self._log.info(
                    f"Order group leg {i} accepted: {order.client_order_id} "
                    f"(TS: {ts_order_id})"
                )
            # A leg cancelled before the ack (e.g. an exit sweep on the fill bar): cancel it
            # now. On an OCO group TradeStation cancels the sibling with it, which is what a
            # sweep wants anyway.
            for _i, order, ts_order_id in legs:
                await TradeStationExecutionClient._cancel_after_ack(
                    self, order.client_order_id, ts_order_id)

        except OrderGroupPartialException as e:
            self._log.error(
                f"[GROUP-PARTIAL] {group_type} group for OrderList {command.order_list.id}: "
                f"{len(e.landed_order_ids)}/{len(orders)} legs landed ({e.ts_message}) - "
                f"cancelling the landed legs at the broker; every leg rejected in NT; "
                f"body={json.dumps(e.body, default=str)[:500] if e.body is not None else 'n/a'}")
            await TradeStationExecutionClient._cancel_group_legs_and_reject(
                self, command,
                [(None, t) for t in e.landed_order_ids] + [(o, None) for o in orders],
                "GROUP-PARTIAL", f"group partially placed and cancelled: {e.ts_message}"[:200],
                reject_uncached=True)
        except Exception as e:
            if _is_ambiguous_submit(e):
                self._log.error(
                    f"[SUBMIT-AMBIGUOUS] {group_type} group OrderList {command.order_list.id} "
                    f"({len(orders)} legs): {type(e).__name__} after the request was sent - "
                    f"legs may be live at TradeStation; rejected in NT (check /orders)")
            self._log.error(
                f"Failed to submit {group_type} order group for "
                f"OrderList {command.order_list.id}: {e}"
            )
            # Reject all legs
            for order in orders:
                TradeStationExecutionClient._forget_cancel_after_ack(self, order.client_order_id)
                self.generate_order_rejected(
                    strategy_id=command.strategy_id,
                    instrument_id=order.instrument_id,
                    client_order_id=order.client_order_id,
                    reason=_reject_reason(e),
                    ts_event=self._clock.timestamp_ns(),
                )

    async def _cancel_group_legs_and_reject(
        self, command, pairs, tag: str, reason: str, reject_uncached: bool = False,
    ) -> None:
        """Cancel each TS id at the broker and reject each still-open NT leg.

        ``pairs`` is ``[(nt_order | None, ts_order_id | None), ...]``: a TS id with no NT
        order is cancelled only (a partial group cannot map ids to legs); an NT order with
        no TS id is rejected only.
        """
        for o, t in pairs:
            if t:
                try:
                    await self._client.cancel_order(str(t))
                except Exception as e:  # noqa: BLE001
                    who = f"{o.client_order_id} " if o is not None else ""
                    self._log.error(
                        f"[{tag}] {who}cancel of #{t} failed: {e} - cancel it manually "
                        f"(/orders cancel)")
            if o is not None:
                TradeStationExecutionClient._forget_cancel_after_ack(self, o.client_order_id)
                c = self._cache.order(o.client_order_id)
                if (c is None and reject_uncached) or (c is not None and not c.is_closed):
                    self.generate_order_rejected(
                        strategy_id=command.strategy_id,
                        instrument_id=o.instrument_id,
                        client_order_id=o.client_order_id,
                        reason=reason,
                        ts_event=self._clock.timestamp_ns(),
                    )

    async def _modify_order(self, command: ModifyOrder) -> None:
        """Modify (replace) an existing order price via TradeStation PUT endpoint.

        On a definitive 4xx rejection from the broker, emits
        ``generate_order_modify_rejected()`` so NT reverts the order out of
        ``PENDING_UPDATE`` and the strategy can react (retry, cancel, hedge).

        On 5xx or network errors the rejection is intentionally suppressed: the
        modify may have already succeeded at the broker before the error was
        returned, and a false rejection event would create state drift in the
        wrong direction. The amend is then recorded for recovery (§241: only after
        the failure, never before the PUT) and nothing is emitted until a broker
        read that started after the failure decides.
        """
        client_order_id = command.client_order_id
        ts_order_id = self._client_order_id_to_ts_order_id.get(client_order_id)

        if not ts_order_id:
            self._log.error(
                f"Cannot modify {client_order_id}: no TradeStation order ID found"
            )
            return

        order = self._cache.order(client_order_id)
        if order is None:
            self._log.error(f"Cannot modify {client_order_id}: not found in cache")
            return

        requested = (command.trigger_price, command.price)
        # §241: what the order carried BEFORE this modify, per field being modified —
        # the recovery's "old price" when the PUT fails ambiguously.
        prev = (getattr(order, "trigger_price", None) if command.trigger_price is not None
                else None,
                getattr(order, "price", None) if command.price is not None else None)
        sent = _mono()
        _modify_inflight_begin(self, client_order_id)
        put_sent = False  # set just before the PUT: an earlier exception is a local error
        try:
            ts_order_type = self._convert_order_type(order)

            # Resolve new prices from the command (None = unchanged)
            if command.trigger_price is not None:
                stop_price = str(command.trigger_price)
            elif hasattr(order, "trigger_price"):
                stop_price = str(order.trigger_price)
            else:
                stop_price = None

            if command.price is not None:
                limit_price = str(command.price)
            elif hasattr(order, "price"):
                limit_price = str(order.price)
            else:
                limit_price = None

            put_sent = True
            response = await self._client.replace_order(
                order_id=ts_order_id,
                quantity=str(order.quantity),
                order_type=ts_order_type,
                limit_price=limit_price,
                stop_price=stop_price,
            )

            # TS returns a new OrderID when the order is replaced
            new_ts_order_id = (
                response.get("OrderID")
                or response.get("Orders", [{}])[0].get("OrderID")
            )
            if new_ts_order_id and new_ts_order_id != ts_order_id:
                # Update our ID maps to point to the new venue order ID
                self._ts_order_id_to_client_order_id.pop(ts_order_id, None)
                self._ts_order_id_to_client_order_id[new_ts_order_id] = client_order_id
                self._client_order_id_to_ts_order_id[client_order_id] = new_ts_order_id
                self._persist_order_map()
                venue_order_id = VenueOrderId(new_ts_order_id)
            else:
                venue_order_id = VenueOrderId(ts_order_id)

            self.generate_order_updated(
                strategy_id=command.strategy_id,
                instrument_id=command.instrument_id,
                client_order_id=client_order_id,
                venue_order_id=venue_order_id,
                quantity=order.quantity,
                price=command.price,
                trigger_price=command.trigger_price,
                ts_event=self._clock.timestamp_ns(),
            )
            _drop_pending_amend(self, client_order_id)  # resolved

            self._log.info(
                f"Order {client_order_id} modified → venue {venue_order_id} "
                f"(requested trigger={command.trigger_price} price={command.price})",
                LogColor.GREEN,
            )

        except Exception as e:
            err = str(e)
            if "HTTP 4" in err or not put_sent:
                # 4xx: broker definitively rejected the modify — safe to surface
                # as a rejection event so NT reverts PENDING_UPDATE and the
                # strategy can react (retry, cancel, hedge). A local error before the
                # PUT left (nothing reached the broker) is just as definitive.
                self._log.error(
                    (f"Modify rejected by broker for {client_order_id} "
                     if put_sent else
                     f"Modify of {client_order_id} failed with a local error before the PUT "
                     f"was sent ")
                    + f"(requested trigger={command.trigger_price} price={command.price}): {err}"
                )
                self.generate_order_modify_rejected(
                    strategy_id=command.strategy_id,
                    instrument_id=command.instrument_id,
                    client_order_id=client_order_id,
                    venue_order_id=VenueOrderId(ts_order_id),
                    reason=(err[:150] if put_sent else f"local error: {err}"[:150]),
                    ts_event=self._clock.timestamp_ns(),
                )
                _drop_pending_amend(self, client_order_id)  # resolved
            else:
                # 5xx / network error / timeout: the broker may have applied it. Record the
                # amend NOW (never before the PUT, §241): the recovery compares the broker's
                # prices only on reads that start after this stamp.
                self._pending_modify_trigger_price[client_order_id] = requested
                self._pending_amend_meta[client_order_id] = _AmendMeta(
                    sent=sent, stamp=_mono(), prev=prev)
                self._log.error(
                    f"Failed to modify order {client_order_id} "
                    f"(ambiguous — broker state unknown): {err} — amend pending "
                    f"(requested trigger={command.trigger_price} price={command.price}, "
                    f"previous trigger={prev[0]} price={prev[1]})"
                )
        finally:
            _modify_inflight_end(self, client_order_id)

    async def _cancel_order(self, command: CancelOrder) -> None:
        """Cancel an order."""
        self._log.info(f"Cancelling order {command.client_order_id}")

        ts_order_id = self._client_order_id_to_ts_order_id.get(command.client_order_id)

        if not ts_order_id:
            # Not an error any more: the cancel is queued and sent right after the ack
            # (_cancel_after_ack, strategies LESSONS §166). Exit sweeps hit this routinely.
            self._log.info(f"Cannot find TradeStation order ID for {command.client_order_id} yet")
            # Reject rather than stay silent: NT otherwise holds the order PENDING_CANCEL
            # until its in-flight check resolves it — possibly by synthesizing OrderCanceled
            # for an order that is live at the broker (2026-09-26).
            self.generate_order_cancel_rejected(
                strategy_id=command.strategy_id,
                instrument_id=command.instrument_id,
                client_order_id=command.client_order_id,
                venue_order_id=None,
                reason="no TradeStation order id yet (not acknowledged)",
                ts_event=self._clock.timestamp_ns(),
            )
            # ...but the strategy still wants it gone: send the cancel once the submit has
            # mapped the TS id. Rejecting alone left the order working at the broker.
            self._remember_cancel_after_ack(command.client_order_id)
            return

        self._note_cancel_requested(command.client_order_id)
        try:
            await self._client.cancel_order(
                order_id=ts_order_id,
            )

            if command.client_order_id in self._partial_orders:
                # Cancel of an FPR remainder: the broker answers FLP (executed qty + cancel,
                # emitted by the FLP terminal path as fill delta then OrderCanceled) or FLL
                # (the cancel lost the race). Emitting OrderCanceled now would close the NT
                # order before the executed qty is applied -> that fill would be dropped.
                self._log.info(
                    f"[PARTIAL-CANCEL] {command.client_order_id}: cancel accepted for a "
                    f"partially filled order - OrderCanceled withheld until the broker's "
                    f"terminal FLP/FLL"
                )
            elif _claim_cancel_emit(self, command.client_order_id):
                self.generate_order_canceled(
                    strategy_id=command.strategy_id,
                    instrument_id=command.instrument_id,
                    client_order_id=command.client_order_id,
                    venue_order_id=VenueOrderId(ts_order_id),
                    ts_event=self._clock.timestamp_ns(),
                )
                self._log.info(f"Order {command.client_order_id} cancelled", LogColor.GREEN)
            else:
                # §225: the stream's OUT arrived during the HTTP call and already emitted it.
                self._log.info(f"Order {command.client_order_id} cancelled "
                               f"(OrderCanceled already emitted by the stream)", LogColor.GREEN)
            self._register_cancel_verify(ts_order_id)  # §111

        except Exception as e:
            if "Not an open order" in str(e):
                # Order already gone at broker — two possible causes:
                # 1. DAY order expired at session close (no event sent by TS) — common
                # 2. Order filled exactly as we tried to cancel it — rare race condition
                #
                # We do NOT generate OrderCanceled here: if it was a fill, the SSE
                # stream will deliver the real OrderFilled event shortly. Generating
                # a synthetic cancel would put the order in a terminal state and cause
                # NT to silently drop the fill → orphan position at broker.
                #
                # Proactively run _check_order_statuses so the fill event is processed
                # within seconds — before NT's ExecEngine times out and auto-generates
                # an OrderCanceled (which would drop any subsequent SSE fill event).
                self._log.warning(
                    f"Order {command.client_order_id} ({ts_order_id}) not found at broker "
                    f"(expired DAY order or concurrent fill) — checking order statuses now"
                )
                await self._check_order_statuses()
            else:
                self._log.error(f"Failed to cancel order {command.client_order_id}: {e}")
                # The order is still working at the broker as far as we know — say so, so
                # NT reverts PENDING_CANCEL immediately instead of guessing later.
                self.generate_order_cancel_rejected(
                    strategy_id=command.strategy_id,
                    instrument_id=command.instrument_id,
                    client_order_id=command.client_order_id,
                    venue_order_id=VenueOrderId(ts_order_id),
                    reason=f"cancel failed: {e}"[:200],
                    ts_event=self._clock.timestamp_ns(),
                )

    async def _cancel_all_orders(self, command: CancelAllOrders) -> None:
        """Cancel all orders for a specific instrument and strategy.

        Uses the NT cache to identify which orders belong to the requesting
        strategy, then cancels only those.  Orders from other strategies or
        processes sharing the same TS account are never touched.

        This follows the same pattern as the official Binance adapter.
        """
        self._log.info(
            f"Cancelling orders for {command.instrument_id} "
            f"(strategy={command.strategy_id})"
        )

        open_orders = self._cache.orders_open(
            instrument_id=command.instrument_id,
            strategy_id=command.strategy_id,
            side=command.order_side,
        )
        inflight_orders = [
            o for o in self._cache.orders_inflight(
                instrument_id=command.instrument_id,
                strategy_id=command.strategy_id,
            )
            if o.status == OrderStatus.SUBMITTED
        ]

        all_orders = list(open_orders) + inflight_orders

        if command.order_side != OrderSide.NO_ORDER_SIDE:
            all_orders = [o for o in all_orders if o.side == command.order_side]

        if not all_orders:
            self._log.info("No matching orders to cancel")
            return

        self._log.info(f"Cancelling {len(all_orders)} order(s)")

        for order in all_orders:
            venue_order_id = order.venue_order_id
            if venue_order_id is None:
                self._log.warning(
                    f"Order {order.client_order_id} has no venue_order_id — skipping"
                )
                continue

            ts_order_id = venue_order_id.value
            self._note_cancel_requested(order.client_order_id)
            try:
                await self._client.cancel_order(order_id=ts_order_id)
                if order.client_order_id in self._partial_orders:
                    # FPR remainder: the terminal FLP/FLL applies the fill, then cancels
                    self._log.info(f"[PARTIAL-CANCEL] {order.client_order_id}: "
                                   f"OrderCanceled withheld until the broker's FLP/FLL")
                elif _claim_cancel_emit(self, order.client_order_id):  # §225
                    self.generate_order_canceled(
                        strategy_id=command.strategy_id,
                        instrument_id=command.instrument_id,
                        client_order_id=order.client_order_id,
                        venue_order_id=venue_order_id,
                        ts_event=self._clock.timestamp_ns(),
                    )
                else:
                    # §225: the stream's OUT arrived during the HTTP call and already emitted it.
                    self._log.info(f"[CANCEL-DUP] {order.client_order_id}: OrderCanceled "
                                   f"already emitted by the stream - not repeated")
                self._register_cancel_verify(ts_order_id)  # §111
                self._log.info(
                    f"Cancelled order {order.client_order_id} ({ts_order_id})"
                )
            except Exception as e:
                if "Not an open order" in str(e):
                    self._log.warning(
                        f"Order {order.client_order_id} ({ts_order_id}) "
                        f"not found at broker — skipping"
                    )
                else:
                    self._log.error(
                        f"Failed to cancel order {order.client_order_id}: {e}"
                    )

    async def _batch_cancel_orders(self, command: BatchCancelOrders) -> None:
        """Cancel a batch of orders."""
        for cancel in command.cancels:
            await self._cancel_order(cancel)

    # -- FILL DETECTION POLLING -----------------------------------------------------------

    async def _poll_order_fills(self) -> None:
        """Background task: poll TradeStation every N seconds for order status changes.

        Detects fills, cancellations, and rejections and generates the corresponding
        NautilusTrader events so strategies receive on_order_filled / on_order_canceled /
        on_order_rejected callbacks.
        """
        self._log.info("Order fill polling loop started")
        while True:
            try:
                await asyncio.sleep(self._fill_poll_interval)
                self._stamp_alive()
                await self._check_order_statuses()
            except asyncio.CancelledError:
                self._log.info("Order fill polling loop stopped")
                break
            except Exception as e:
                self._log.error(f"Error in order fill polling: {e}")
                await asyncio.sleep(5.0)

    async def _status_safety_poll_loop(self) -> None:
        """§95: low-frequency order-status poll alongside the SSE stream.

        The order SSE stream can go one-way-dead: heartbeats keep arriving (so
        the 90s read timeout never fires) while FLL events are silently
        dropped.  Before this loop, `_check_order_statuses()` ran only on the
        reconnect sentinel — missed fills stayed invisible for hours (observed
        3h01m and 2h15m gaps in production paper trading, 2026-06-09/10),
        leaving real broker positions with no exit orders and no ownership
        record.  `_check_order_statuses()` is idempotent (skips unchanged
        statuses and closed orders, no-ops when nothing is tracked), so this
        is a pure safety net.
        """
        self._log.info(
            f"Status safety poll loop started ({self._status_safety_poll_secs:.0f}s interval)"
        )
        while True:
            try:
                await asyncio.sleep(self._status_safety_poll_secs)
                self._stamp_alive()
                await self._check_order_statuses()
            except asyncio.CancelledError:
                self._log.info("Status safety poll loop stopped")
                raise
            except Exception as e:
                self._log.error(f"Error in status safety poll: {e}")

    _STATUS_RECHECK_S = 300.0  # §149: broker recheck delay for an ambiguous status

    def _watch_status(self, ts_order_id: str, client_order_id, status: str,
                      note: str = "") -> None:
        """§149: park an ambiguous/unsolicited status (UCN, LAT, ...) for a broker
        recheck. Never decides anything: the outcome is only logged as
        [TS-STATUS-RECHECK], which the agent forwards to Telegram.  Warns once per
        (order, status): a parked order is not re-logged."""
        if not ts_order_id or ts_order_id in self._status_watch:
            return
        self._status_watch[ts_order_id] = (time.monotonic(), status, str(client_order_id))
        self._log.warning(
            f"[TS-STATUS-WATCH] TS order {ts_order_id} ({client_order_id}) got {status} "
            f"we did not request{' ' + note if note else ''} — no NT action, "
            f"broker recheck in {self._STATUS_RECHECK_S:.0f}s"
        )

    def _recheck_watched_statuses(self, orders: list) -> None:
        """§149: report what became of each watched order. Pure observation."""
        if not self._status_watch:
            return
        now = time.monotonic()
        by_id = {str(o.get("OrderID")): o for o in orders}
        for ts_id in list(self._status_watch):
            first_seen, status, coid = self._status_watch[ts_id]
            if now - first_seen < self._STATUS_RECHECK_S:
                continue
            del self._status_watch[ts_id]
            o = by_id.get(ts_id)
            now_status = "-" if o is None else (o.get("Status") or "UNKNOWN")
            cat = _st.category(now_status)
            cached = None
            inst = "?"
            try:
                cached = self._cache.order(ClientOrderId(coid))
                if cached is not None:
                    inst = cached.instrument_id.symbol.value
            except Exception:
                cached = None
            if o is None:
                verdict = "NOT FOUND in broker order list"
            elif now_status == "UNKNOWN":
                verdict = "no Status reported at broker"
            elif cat == "open":
                verdict = "STILL LIVE - kept, NT state unchanged"
            else:
                verdict = f"CLOSED at broker ({cat})"
                if cat == "canceled" and cached is not None and not cached.is_closed:
                    # R24: the broker confirmed a terminal Canceled status for an
                    # order we held; close NT's side. Never re-cancelled (§149).
                    # R28: only if ts_id is still this order's venue id (a replace
                    # may have remapped it) and the status is a plain cancel.
                    if (self._ts_order_id_to_client_order_id.get(ts_id)
                            != ClientOrderId(coid)):
                        verdict += " - id remapped, NT order untouched"
                    elif now_status not in (_st.CANCELED - _st.WATCH):
                        verdict += f" - still {now_status}, NT order kept open (held)"
                    else:
                        try:
                            self.generate_order_canceled(
                                strategy_id=cached.strategy_id,
                                instrument_id=cached.instrument_id,
                                client_order_id=ClientOrderId(coid),
                                venue_order_id=VenueOrderId(ts_id),
                                ts_event=self._clock.timestamp_ns(),
                            )
                            self._register_cancel_verify(ts_id, requested=False)
                            verdict += " - NT order closed"
                        except Exception as e:
                            self._log.error(
                                f"[TS-STATUS-RECHECK] cancel emit failed for {coid}: {e}"
                            )
            self._log.warning(
                f"[TS-STATUS-RECHECK] TS order {ts_id} ({coid}, {inst}) got {status} "
                f"unsolicited; {now - first_seen:.0f}s later broker status={now_status} "
                f"-> {verdict}"
            )

    def _note_cancel_requested(self, client_order_id) -> None:
        """Remember that this process sent a cancel for ``client_order_id`` (bounded)."""
        reqs = self._cancel_requested_coids
        reqs[client_order_id] = None
        while len(reqs) > 2000:
            reqs.pop(next(iter(reqs)))

    def _cancel_was_requested(self, client_order_id) -> bool:
        """True if THIS process asked the broker to cancel ``client_order_id`` (a cancel
        attempt, even one whose HTTP call failed, or a §111 registry row with
        ``requested=True``). A broker CAN/OUT/EXP for an order we never asked to cancel
        is not ours to report as canceled (F4)."""
        if client_order_id in self._cancel_requested_coids:
            return True
        ts_id = self._client_order_id_to_ts_order_id.get(client_order_id)
        row = self._cancel_verify_pending.get(ts_id) if ts_id else None
        return bool(row and row[2])

    def partial_orders(self) -> dict:
        """Read-only copy of the own orders in FPR: ``{client_order_id: (exec_qty, since_ns)}``."""
        return dict(self._partial_orders)

    def _register_cancel_verify(self, ts_order_id: str, requested: bool = True) -> None:
        """Queue a cancelled order for §111 verification. `requested`: this session
        sent the cancel (only those may be re-issued — §149)."""
        if not ts_order_id:
            return
        prev = self._cancel_verify_pending.get(ts_order_id)
        if prev is None:
            self._cancel_verify_pending[ts_order_id] = (time.monotonic(), 0, requested, False)
        elif requested and not prev[2]:
            self._cancel_verify_pending[ts_order_id] = (prev[0], prev[1], True, prev[3])

    async def _verify_pending_cancels(self, orders: list) -> None:
        """§111: re-check every order reported cancelled against the broker's own
        order list; if still live (common/order_status.LIVE) >=_CANCEL_VERIFY_MIN_AGE_S later,
        re-issue the cancel (max _CANCEL_VERIFY_MAX_ATTEMPTS), loudly.  Only cancels this
        session SENT are re-issued (§149: an observed-but-unrequested cancel that is
        still live is logged as [CANCEL-MISMATCH], never cancelled).  In-memory only: zombies from a
        prior process (before restart) are out of scope here — the agent's own
        orphan scan is the cross-restart backstop.
        """
        if not self._cancel_verify_pending:
            return
        now = time.monotonic()
        live = {
            o.get("OrderID"): o.get("Status")
            for o in orders
            if o.get("Status") in _st.LIVE
        }
        for ts_order_id in list(self._cancel_verify_pending):
            first_seen, attempts, requested, alerted = self._cancel_verify_pending[ts_order_id]
            if now - first_seen < self._CANCEL_VERIFY_MIN_AGE_S:
                continue
            if ts_order_id not in live:
                del self._cancel_verify_pending[ts_order_id]
                continue
            if live[ts_order_id] in _st.CANCEL_PENDING:
                # our cancel still pending / too late — a terminal status normally follows.
                # §150: but if it never does, say so once (never re-cancel).
                if (requested and not alerted
                        and now - first_seen >= self._CANCEL_STUCK_ALERT_S):
                    self._log.error(
                        f"[CANCEL-STUCK] Order {ts_order_id} cancel requested "
                        f"{(now - first_seen) / 60:.0f} min ago but still "
                        f"{live[ts_order_id]} at the broker — position protection "
                        f"may be in doubt (§150)"
                    )
                    self._cancel_verify_pending[ts_order_id] = (
                        first_seen, attempts, requested, True)
                continue
            if not requested:
                self._log.error(
                    f"[CANCEL-MISMATCH] Order {ts_order_id} was reported cancelled "
                    f"(we never asked) but is still {live[ts_order_id]} at the broker — "
                    f"NOT re-cancelling; NT thinks it is gone (§149)"
                )
                del self._cancel_verify_pending[ts_order_id]
                continue
            if attempts >= self._CANCEL_VERIFY_MAX_ATTEMPTS:
                self._log.error(
                    f"[CANCEL-ZOMBIE] Order {ts_order_id} still open after "
                    f"{attempts} re-cancel attempts — giving up (the agent's "
                    f"orphan scan will flag it)"
                )
                del self._cancel_verify_pending[ts_order_id]
                continue
            self._log.warning(
                f"[CANCEL-ZOMBIE] Order {ts_order_id} was reported cancelled but is "
                f"still {live[ts_order_id]} at the broker — re-issuing cancel "
                f"(attempt {attempts + 1}/{self._CANCEL_VERIFY_MAX_ATTEMPTS}, §111)"
            )
            try:
                await self._client.cancel_order(order_id=ts_order_id)
            except Exception as e:
                self._log.warning(f"[CANCEL-ZOMBIE] re-cancel failed for {ts_order_id}: {e}")
            self._cancel_verify_pending[ts_order_id] = (
                first_seen, attempts + 1, requested, alerted)

    def _prune_closed_last_status(self, orders: list) -> None:
        """M-5: drop ``_order_last_status`` entries of orders that left the broker's list
        and that NT shows closed (terminal orders otherwise stay until process end)."""
        try:
            listed = {o.get("OrderID") for o in orders}
            for ts_id in [t for t in self._order_last_status if t not in listed]:
                coid = self._ts_order_id_to_client_order_id.get(ts_id)
                cached = self._cache.order(coid) if coid is not None else None
                if cached is not None and cached.is_closed:
                    self._order_last_status.pop(ts_id, None)
        except Exception:
            pass  # housekeeping only

    async def _check_order_statuses(self) -> None:
        """Fetch current orders and emit events for any status changes.

        Status flow:
          Terminal categories per common/order_status.py: FILLED → fill,
          CANCELED → cancel, REJECTED → reject; OPEN (incl. DON, UCN, LAT, BRO)
          → no event; WATCH codes and unknown codes → _watch_status
        """
        if (not self._ts_order_id_to_client_order_id and not self._cancel_verify_pending
                and not self._status_watch):
            return  # Nothing tracked yet

        read_started = _mono()  # §241: a read started before an amend failed never decides
        try:
            orders = await self._client.get_orders(
                account_keys=self._account_id,
            )
        except Exception as e:
            self._log.error(f"Fill poll: failed to fetch orders: {e}")
            return

        await self._verify_pending_cancels(orders)  # §111
        self._recheck_watched_statuses(orders)      # §149
        self._prune_closed_last_status(orders)      # M-5

        for ts_order in orders:
            ts_order_id = ts_order.get("OrderID")
            if not ts_order_id:
                continue

            client_order_id = self._ts_order_id_to_client_order_id.get(ts_order_id)
            if not client_order_id:
                continue  # Not submitted by us in this session

            status = ts_order.get("Status", "")
            last_status = self._order_last_status.get(ts_order_id, "")
            has_pending_amend = client_order_id in self._pending_modify_trigger_price
            if status == "FPR":
                _own = self._cache.order(client_order_id)
                if _own is not None and not _own.is_closed:  # own, open orders only
                    _track_partial(
                        self, ts_order, client_order_id, self._clock.timestamp_ns())
                else:
                    _clear_partial(self, client_order_id)  # closed/uncached: no stale entry

            # Normal skip: no status change and no pending amend to resolve.
            # If a 5xx left an amend in-flight (has_pending_amend=True) we must
            # process the order even when TS status is unchanged (still OPN/ACK).
            if status == last_status and not has_pending_amend:
                _release_if_terminal_closed(self, status, client_order_id)
                continue

            self._order_last_status[ts_order_id] = status

            # Look up the NautilusTrader order for metadata
            cached_order = self._cache.order(client_order_id)
            if cached_order is None:
                self._log.warning(
                    f"Fill poll: order {client_order_id} not found in cache (status={status})"
                )
                if status in ("FLL", "FLP"):
                    _handle_uncached_fill(self, ts_order, ts_order_id, client_order_id)
                continue
            _stamp_group_if_missing(self, ts_order_id)

            # Skip if NautilusTrader already considers the order closed (idempotency guard).
            # Also clean up any stale pending-amend record (e.g. SSE filled it first).
            if cached_order.is_closed:
                _drop_pending_amend(self, client_order_id)
                _clear_partial(self, client_order_id)
                _clear_emitted_hwm(self, client_order_id)
                _log_fill_after_cancel(
                    self._log, status, ts_order_id, client_order_id, cached_order)
                continue

            # --- amend recovery (§212/§241) ---
            # Only after a modify PUT failed ambiguously (5xx / exception). A PUT still in
            # flight is never "recovered": its own answer decides (the 2026-10-27 FESX race).
            _TS_OPEN_STATUSES = _st.LIVE
            if cached_order.status == OrderStatus.PENDING_UPDATE and has_pending_amend:
                if status not in _TS_OPEN_STATUSES:
                    # Closed/expired at TS (e.g. DAY order at the close): drop the amend and
                    # fall through so the normal CAN/FLL/REJ event is emitted.
                    _drop_pending_amend(self, client_order_id)
                    self._log.info(
                        f"[PENDING_UPDATE recovery] {client_order_id} no longer open "
                        f"(status={status}) — deferring to normal handling"
                    )
                    # fall through — no continue
                else:
                    if client_order_id not in self._modify_inflight:
                        try:
                            _recover_amend(self, cached_order, client_order_id, ts_order_id,
                                           ts_order, read_started)
                        except Exception as rec_e:
                            self._log.error(
                                f"[MODIFY-RECOVERY] comparison failed for "
                                f"{client_order_id}: {rec_e}"
                            )
                    continue  # open order with a pending amend: no other status event

            venue_order_id = VenueOrderId(ts_order_id)
            ts_now = self._clock.timestamp_ns()

            if status == "FLL":
                try:
                    _, _, reason = _emit_fill_delta(
                        self, ts_order, ts_order_id, client_order_id, cached_order,
                        venue_order_id, ts_now, "status-poll", partial=False)
                    if reason == "no_price":
                        self._order_last_status.pop(ts_order_id, None)  # retry next poll
                        continue
                except Exception as e:
                    self._log.error(
                        f"Fill poll: error generating fill event for {client_order_id}: {e}"
                    )
                    self._order_last_status.pop(ts_order_id, None)  # un-advance: retry
                    continue
                _clear_partial(self, client_order_id)
                _drop_pending_amend(self, client_order_id)

            else:
                _apply_status_tail(
                    self, status, ts_order, ts_order_id, client_order_id,
                    cached_order, venue_order_id, ts_now, stream=False)

    async def _token_keepalive_loop(self) -> None:
        """Background task: keep the OAuth access token fresh in streaming mode.

        In polling mode _poll_order_fills calls _check_order_statuses every 5 s, which
        calls _get_headers → _ensure_authenticated — the token is never stale.  In
        streaming mode the SSE loop only calls _check_order_statuses on reconnect, so
        without this task the token expires silently after 20 min when no orders are
        being placed.  Firing every 10 min guarantees _ensure_authenticated sees the
        token within its 5-min refresh window before expiry.
        """
        try:
            while True:
                await asyncio.sleep(self._TOKEN_KEEPALIVE_SECS)
                try:
                    await self._client._ensure_authenticated()
                    self._log.debug("Token keepalive: OK")
                except Exception as e:
                    self._log.error(f"Token keepalive failed: {e}")
        except asyncio.CancelledError:
            pass

    async def _stream_order_fills(self) -> None:
        """SSE streaming task: receive real-time order events and emit NT order events.

        Replaces ``_poll_order_fills`` when ``use_streaming=True``. Processes the same
        status codes (FLL → filled, CAN → canceled, REJ → rejected) but gets notified
        immediately rather than waiting up to ``_fill_poll_interval`` seconds.
        """
        self._log.info("Order fill SSE stream started")
        try:
            async for event in self._stream_client.stream_orders(self._account_id):
                # Reconnect sentinel — run a catch-up HTTP poll to recover any fills
                # that were missed during the SSE gap (SSE does not replay events).
                if event.get("_reconnected"):
                    self._log.warning(
                        "Order fill SSE stream reconnected — running catch-up HTTP poll "
                        "to recover fills missed during the gap"
                    )
                    try:
                        await self._check_order_statuses()
                    except Exception as e:
                        self._log.error(f"Catch-up poll after SSE reconnect failed: {e}")
                    continue
                try:
                    await self._process_order_event(event)
                except Exception as e:
                    self._log.error(f"Error processing streamed order event: {e}")
        except asyncio.CancelledError:
            self._log.info("Order fill SSE stream stopped")

    async def _process_order_event(self, ts_order: dict) -> None:
        """Process a single order event (from streaming or polling) and emit NT events."""
        ts_order_id = ts_order.get("OrderID")
        if not ts_order_id:
            return

        client_order_id = self._ts_order_id_to_client_order_id.get(ts_order_id)
        if not client_order_id:
            return  # Not submitted by us in this session

        status = ts_order.get("Status", "")
        last_status = self._order_last_status.get(ts_order_id, "")

        if status == "FPR":
            _own = self._cache.order(client_order_id)
            if _own is not None and not _own.is_closed:  # own, open orders only
                _track_partial(self, ts_order, client_order_id, self._clock.timestamp_ns())
            else:
                _clear_partial(self, client_order_id)  # closed/uncached: no stale entry

        if status == last_status:
            _release_if_terminal_closed(self, status, client_order_id)
            return  # No change

        self._order_last_status[ts_order_id] = status

        cached_order = self._cache.order(client_order_id)
        if cached_order is None:
            # _order_last_status is already updated above, so the safety poll will
            # not see this change again — this is the only chance to make it loud.
            if status in ("FLL", "FLP"):
                _handle_uncached_fill(self, ts_order, ts_order_id, client_order_id)
            return
        _stamp_group_if_missing(self, ts_order_id)
        if cached_order.is_closed:
            _clear_partial(self, client_order_id)
            _clear_emitted_hwm(self, client_order_id)
            _log_fill_after_cancel(self._log, status, ts_order_id, client_order_id, cached_order)
            return

        venue_order_id = VenueOrderId(ts_order_id)
        ts_now = self._clock.timestamp_ns()

        if status == "FLL":
            try:
                _, _, reason = _emit_fill_delta(
                    self, ts_order, ts_order_id, client_order_id, cached_order,
                    venue_order_id, ts_now, "sse", partial=False)
            except Exception:
                self._order_last_status.pop(ts_order_id, None)  # un-advance: poll retries
                raise
            if reason == "no_price":
                self._order_last_status.pop(ts_order_id, None)  # the safety poll retries
                return
            _clear_partial(self, client_order_id)
            _drop_pending_amend(self, client_order_id)  # prevent stale entry

        else:
            _apply_status_tail(
                self, status, ts_order, ts_order_id, client_order_id,
                cached_order, venue_order_id, ts_now, stream=True)

    # -- INTERNAL METHODS -----------------------------------------------------------------

    async def _update_account_state(self) -> None:
        """Update account state from TradeStation.

        Retries up to 3 times on transient errors (3 s between attempts).
        If all attempts fail the method returns without raising — trading
        continues and the broker enforces margin on every order. A prominent
        warning is logged so the operator is aware the local balance snapshot
        is unavailable until a subsequent successful call.
        """
        last_err: Exception | None = None
        for attempt in range(3):
            if attempt > 0:
                await asyncio.sleep(3.0)
                self._log.info(
                    f"Retrying account state fetch (attempt {attempt + 1}/3)"
                )
            try:
                balances_data = await self._client.get_balances(
                    account_keys=self._account_id,
                )

                cash_balance = Decimal(balances_data.get("CashBalance", "0"))
                equity = Decimal(balances_data.get("Equity", "0"))
                # MarketValue is negative for short positions — clamp to 0.
                # NautilusTrader requires non-negative MarginBalance values.
                # We don't enforce margin limits so 0 is safe when short.
                margin_used = max(
                    Decimal("0"), Decimal(balances_data.get("MarketValue", "0"))
                )

                balances = [
                    AccountBalance(
                        Money(cash_balance, Currency.from_str("USD")),
                        Money(0, Currency.from_str("USD")),
                        Money(cash_balance, Currency.from_str("USD")),
                    ),
                ]
                margins = [
                    MarginBalance(
                        initial=Money(margin_used, Currency.from_str("USD")),
                        maintenance=Money(margin_used, Currency.from_str("USD")),
                    ),
                ]

                if self.account_id is None:
                    self._set_account_id(self._account_id_nautilus)

                self.generate_account_state(
                    balances=balances,
                    margins=margins,
                    reported=True,
                    ts_event=self._clock.timestamp_ns(),
                    info={
                        "equity": str(equity),
                        "cash_balance": str(cash_balance),
                        "account_id": self._account_id,
                    },
                )
                return  # success — exit retry loop

            except Exception as e:
                last_err = e
                self._log.warning(
                    f"Account state fetch failed (attempt {attempt + 1}/3): {e}"
                )

        self._log.warning(
            f"Account state unavailable after 3 attempts ({last_err}). "
            "Trading will continue — the broker enforces margin on every order. "
            "Balance display will be stale until the next successful update."
        )

    def _convert_order_to_ts_format(self, order: Order) -> dict[str, Any]:
        """Convert Nautilus order to TradeStation format.

        For futures: always Buy/Sell (TS rejects SellShort/BuyToCover on futures).
        For equities: use SellShort when opening a short, BuyToCover when closing a short.
        """
        params = convert_order_to_ts_format(order, self._account_id)

        # Check if this is an equity instrument — if so, adjust TradeAction
        instrument = self._cache.instrument(order.instrument_id)
        if instrument is not None:
            from nautilus_trader.model.instruments import Equity
            if isinstance(instrument, Equity):
                # For equities, TS requires SellShort/BuyToCover for short positions
                # Calculate net position from open positions in cache
                open_positions = self._cache.positions_open(
                    instrument_id=order.instrument_id,
                )
                net_pos = sum(
                    p.signed_qty for p in open_positions
                ) if open_positions else 0

                if order.side == OrderSide.SELL:
                    if net_pos <= 0:
                        # No long position to close — this is opening a short
                        params["trade_action"] = "SellShort"
                    else:
                        # Closing an existing long
                        params["trade_action"] = "Sell"
                elif order.side == OrderSide.BUY:
                    if net_pos < 0:
                        # Closing an existing short
                        params["trade_action"] = "BuyToCover"
                    else:
                        # Opening a new long
                        params["trade_action"] = "Buy"

                # When extended_hours is enabled, use DYP (Day Plus) so equity
                # limit orders can fill during pre-market and after-hours sessions.
                # TS rejects Market orders with DYP — use Limit orders instead.
                if self._extended_hours:
                    params["time_in_force"] = "DYP"

        return params

    def _convert_order_type(self, order: Order) -> str:
        """Convert Nautilus order type to TradeStation format."""
        return convert_order_type(order)

    def _convert_time_in_force(self, tif: TimeInForce) -> str:
        """Convert Nautilus TimeInForce to TradeStation format."""
        return convert_time_in_force(tif)

    def _parse_order_status(self, ts_status: str) -> OrderStatus:
        """Parse TradeStation order status to Nautilus OrderStatus."""
        return parse_order_status(ts_status)

    def _parse_order_status_report(
        self,
        ts_order: dict[str, Any],
        instrument_id: InstrumentId,
        client_order_id: ClientOrderId,
    ) -> OrderStatusReport | None:
        """Parse TradeStation order into OrderStatusReport."""
        return parse_order_status_report(
            ts_order,
            instrument_id,
            client_order_id,
            account_id=self._account_id_nautilus,
            ts_now=self._clock.timestamp_ns(),
        )

    def _parse_ts_order_type(self, ts_order_type: str) -> OrderType:
        """Parse TradeStation order type to Nautilus OrderType."""
        return parse_ts_order_type(ts_order_type)

