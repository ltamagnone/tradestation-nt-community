"""BarCloseGate: which TradeStation v3 bar events become NT bars (strategies LESSONS §207)."""

import pandas as pd

from tradestation_nt_community.bar_gate import BarCloseGate, bar_end_ns

T0, T1, T2, T3 = (f"2026-10-16T16:{m}:00Z" for m in ("00", "05", "10", "15"))


def ev(ts, status="Open", realtime=True):
    return {"TimeStamp": ts, "BarStatus": status, "IsRealtime": realtime}


def gate():
    g = BarCloseGate()
    g.reset_connection()
    return g


def feed(g, *events):
    out = []
    for e in events:
        out += [x["TimeStamp"] for x in g.on_event(e, bar_end_ns(e["TimeStamp"]))]
    return out


def test_bar_end_ns():
    assert bar_end_ns(T0) == pd.Timestamp(T0).value
    assert bar_end_ns("") is None
    assert bar_end_ns("not a time") is None


def test_closed_realtime_bar_is_emitted_once():
    assert feed(gate(), ev(T1, "Closed"), ev(T1, "Closed")) == [T1]


def test_open_bar_waits_for_its_closed_event():
    g = gate()
    assert feed(g, ev(T1), ev(T1)) == []
    assert feed(g, ev(T1, "Closed")) == [T1]


def test_fallback_releases_the_pending_bar_when_the_next_bar_starts():
    assert feed(gate(), ev(T1), ev(T2)) == [T1]


def test_fallback_skips_a_pending_bar_the_history_already_covers():
    g = gate()
    feed(g, ev(T2))
    g.seed_from_history(bar_end_ns(T2))
    assert feed(g, ev(T3)) == []


def test_history_before_a_watermark_only_sets_it():
    g = gate()
    assert feed(g, ev(T0, "Closed", False), ev(T1, "Closed", False)) == []
    assert g.last_emitted_ns == bar_end_ns(T1)


def test_history_after_a_watermark_fills_the_gap():
    g = BarCloseGate()
    g.seed_from_history(bar_end_ns(T0))
    g.reset_connection()
    assert feed(g, ev(T0, "Closed", False), ev(T1, "Closed", False), ev(T2, "Open", False)) == [T1]


def test_older_or_equal_bars_are_dropped():
    g = gate()
    feed(g, ev(T2, "Closed"))
    assert feed(g, ev(T1, "Closed"), ev(T2, "Closed"), ev(T2)) == []


def test_watermark_never_moves_back():
    g = BarCloseGate()
    g.seed_from_history(bar_end_ns(T2))
    g.seed_from_history(bar_end_ns(T1))
    assert g.last_emitted_ns == bar_end_ns(T2)


def test_history_seed_after_a_live_emission_is_ignored():
    # race: the stream emitted first; a history read that already holds T3 must not
    # swallow T3's live Closed event
    g = gate()
    feed(g, ev(T2, "Closed"))
    g.seed_from_history(bar_end_ns(T3))
    assert feed(g, ev(T3, "Closed")) == [T3]


def test_reset_connection_forgets_the_forming_bar():
    g = gate()
    feed(g, ev(T1))
    g.reset_connection()
    assert feed(g, ev(T2)) == []


# -- late thin-hour bars: release, divergence, gap, barrier (plan 2026-10-23 Task 1) ----------

S = 10**9
STEP = 5 * 60 * S
GRACE = 10 * S
T4 = "2026-10-16T16:20:00Z"


def bev(ts, status="Open", realtime=True, c=1, v=1):
    return {"TimeStamp": ts, "BarStatus": status, "IsRealtime": realtime,
            "Open": 1, "High": max(1, c), "Low": 1, "Close": c, "TotalVolume": v}


def n(ts):
    return bar_end_ns(ts)


def test_release_due_frees_the_pending_bar_after_grace():
    g = gate()
    g.on_event(bev(T1, "Closed"), n(T1))
    g.on_event(bev(T2), n(T2))
    assert g.pending_end_ns == n(T2)
    assert g.release_due(n(T2) + GRACE - 1, GRACE) == []
    out = g.release_due(n(T2) + GRACE, GRACE)
    assert [x["TimeStamp"] for x in out] == [T2]
    assert g.last_emitted_ns == n(T2) and g.pending_end_ns is None
    assert g.release_due(n(T2) + 2 * GRACE, GRACE) == []  # once


def test_release_due_without_commit_changes_no_emission_state():
    g = gate()
    g.on_event(bev(T1, "Closed"), n(T1))
    g.on_event(bev(T2), n(T2))
    assert [x["TimeStamp"] for x in g.release_due(n(T2) + GRACE, GRACE, commit=False)] == [T2]
    assert g.last_emitted_ns == n(T1) and g.pending_end_ns == n(T2)
    assert feed(g, bev(T2, "Closed")) == [T2]  # the real Closed is still emitted


def test_release_then_late_closed_seed_and_poll_emit_nothing():
    g = gate()
    feed(g, bev(T1, "Closed"), bev(T2))
    assert len(g.release_due(n(T2) + GRACE, GRACE)) == 1
    assert feed(g, bev(T2), bev(T2, "Closed")) == []  # late updates + late Closed
    g.reset_connection()  # reconnect seed carries it
    assert feed(g, bev(T2, "Closed", False)) == []
    assert feed(g, bev(T2, "Closed", False)) == []  # a poll of it
    assert g.release_due(n(T2) + 10 * GRACE, GRACE) == []


def test_release_records_divergence_of_a_later_update():
    g = gate()
    feed(g, bev(T1, "Closed"), bev(T2))
    g.release_due(n(T2) + GRACE, GRACE)
    assert feed(g, bev(T2, "Closed", c=2, v=3)) == []
    assert len(g.diverged) == 1
    ts, snap, event = g.diverged.pop()
    assert ts == n(T2) and snap == (1.0, 1.0, 1.0, 1.0, 1.0) and event["Close"] == 2
    feed(g, bev(T2, "Closed", False, c=2, v=3))  # a seed of the same bar: logged once only
    assert g.diverged == []


def test_release_with_an_equal_closed_records_no_divergence():
    g = gate()
    feed(g, bev(T1, "Closed"), bev(T2))
    g.release_due(n(T2) + GRACE, GRACE)
    feed(g, bev(T2, "Closed"))
    assert g.diverged == []


def test_shadow_candidate_records_divergence_of_the_emitted_closed():
    g = gate()
    feed(g, bev(T1, "Closed"), bev(T2))
    g.release_due(n(T2) + GRACE, GRACE, commit=False)
    assert feed(g, bev(T2, "Closed", c=2)) == [T2]
    assert [d[0] for d in g.diverged] == [n(T2)]


def test_released_snapshots_are_bounded():
    g = gate()
    for i, t in enumerate((T0, T1, T2, T3, T4)):
        g.on_event(bev(t), n(t))
        g.release_due(n(t) + GRACE, GRACE)
    assert len(g._released) == 4 and n(T0) not in g._released


def test_missing_periods_between_watermark_and_next_bar():
    g = gate()
    feed(g, bev(T1, "Closed"))
    assert g.missing_periods(n(T3), STEP) == [n(T2)]
    assert g.missing_periods(n(T2), STEP) == []
    assert g.missing_periods(n(T4), STEP) == [n(T2), n(T3)]


def test_missing_periods_counts_from_a_pending_bar_the_event_will_release():
    g = gate()
    feed(g, bev(T1, "Closed"), bev(T2))  # T2 forming, its Closed never came
    assert g.effective_watermark(n(T4)) == n(T2)
    assert g.missing_periods(n(T4), STEP) == [n(T3)]


def test_missing_periods_empty_when_too_many_or_no_watermark():
    assert gate().missing_periods(n(T3), STEP) == []
    g = gate()
    feed(g, bev(T0, "Closed"))
    assert g.missing_periods(n(T4), STEP, max_periods=3) == [n(T1), n(T2), n(T3)]
    assert g.missing_periods(n(T4), STEP, max_periods=2) == []
    g.begin_backfill([n(T1), n(T2), n(T3)])
    assert g.missing_periods(n(T4), STEP) == []  # one already pending


def test_missing_periods_never_repeats_a_handled_period():
    g = gate()
    feed(g, bev(T1, "Closed"))
    g.begin_backfill([n(T2)])
    g.end_backfill()  # done (skipped or failed): the next update of T3 must not re-trigger
    assert g.missing_periods(n(T3), STEP) == []
    assert g.missing_periods(n(T4), STEP) == [n(T3)]


def test_hold_parks_emissions_above_barrier_and_release_hold_orders_them():
    g = gate()
    feed(g, bev(T1, "Closed"))
    g.hold(n(T1))
    assert feed(g, bev(T4, "Closed")) == []
    assert feed(g, bev(T3, "Closed")) == []  # below the watermark now: a repeat
    g.on_event(bev(T4), n(T4))
    assert [p[1]["TimeStamp"] for p in g.parked] == [T4]
    assert g.backfill_ok(n(T2)) and g.backfill_ok(n(T3)) and not g.backfill_ok(n(T4))
    assert not g.backfill_ok(n(T1))
    out = g.release_hold()
    assert [(r["TimeStamp"], rel) for r, rel in out] == [(T4, False)]
    assert g.parked == [] and g.release_hold() == []


def test_hold_parks_a_release_and_flags_it():
    g = gate()
    feed(g, bev(T1, "Closed"), bev(T3))
    g.hold(n(T1))
    assert g.release_due(n(T3) + GRACE, GRACE) == []
    assert [(r["TimeStamp"], rel) for r, rel in g.release_hold()] == [(T3, True)]


def test_hold_lets_the_pending_fallback_at_the_barrier_through():
    g = gate()
    feed(g, bev(T1, "Closed"), bev(T2))
    g.hold(g.effective_watermark(n(T4)))
    assert feed(g, bev(T4, "Closed")) == [T2]  # T2's fallback is at the barrier: emitted now
    assert [p[1]["TimeStamp"] for p in g.parked] == [T4]


def test_mark_emitted_blocks_a_seed_replay_of_a_backfilled_bar():
    g = gate()
    feed(g, bev(T1, "Closed"))
    g.hold(n(T1))
    g.mark_emitted(n(T2))
    assert not g.backfill_ok(n(T2))
    g.release_hold()
    g.reset_connection()
    assert feed(g, bev(T1, "Closed", False), bev(T2, "Closed", False)) == []
    assert feed(g, bev(T3, "Closed", False)) == [T3]


def test_abort_backfill_drops_parked_bars_for_the_seed_to_replay():
    g = gate()
    feed(g, bev(T1, "Closed"))
    g.begin_backfill([n(T2)])
    g.hold(n(T1))
    feed(g, bev(T3, "Closed"))
    g.abort_backfill()
    assert g.parked == [] and g.last_emitted_ns == n(T1)
    g.reset_connection()
    assert feed(g, bev(T2, "Closed", False), bev(T3, "Closed", False)) == [T2, T3]


def test_abort_backfill_without_hold_keeps_the_watermark():
    g = gate()
    feed(g, bev(T1, "Closed"), bev(T2, "Closed"))
    g.begin_backfill([n(T3)])
    g.abort_backfill()
    assert g.last_emitted_ns == n(T2)
    assert g.missing_periods(n(T4), STEP) == [n(T3)]  # an aborted episode may run again


def test_history_seed_after_a_release_is_ignored():
    g = gate()
    feed(g, bev(T1))
    g.release_due(n(T1) + GRACE, GRACE)
    g.seed_from_history(n(T3))
    assert g.last_emitted_ns == n(T1)
    assert feed(g, bev(T2, "Closed")) == [T2]


def test_snapshots_keep_the_last_update_before_each_offset():
    g = gate()
    end = n(T1)
    g.on_event(bev(T1, c=1), end, now_ns=end - S)
    g.on_event(bev(T1, c=2), end, now_ns=end + S)
    g.on_event(bev(T1, c=3), end, now_ns=end + 5 * S)
    g.on_event(bev(T1, c=4), end, now_ns=end + 20 * S)
    snaps = g.snapshots(end)
    assert {off: s[1]["Close"] for off, s in snaps.items()} == {
        0: 1, 2 * S: 2, 10 * S: 3, 30 * S: 4,
    }
    assert snaps[0][0] == end - S


def test_snapshots_off_by_default():
    g = gate()
    g.on_event(bev(T1), n(T1))
    assert g.snapshots(n(T1)) == {}
