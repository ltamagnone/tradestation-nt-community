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
