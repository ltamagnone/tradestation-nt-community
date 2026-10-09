"""Status categories must match the TradeStation OpenAPI spec verbatim
(docs/api_reference/specificationOpenAPI2026.json in my-nautilus-strategies)."""
from tradestation_nt_community.common import order_status as st

SPEC_OPEN = {"ACK", "ASS", "BRC", "BRF", "BRO", "CHG", "CND", "COR", "DIS", "DOA", "DON",
             "ECN", "EXE", "FPR", "LAT", "OPN", "OSO", "OTHER", "PLA", "REC", "RJC", "RPD",
             "RSN", "STP", "STT", "SUS", "UCN"}
SPEC_CANCELED = {"CAN", "EXP", "OUT", "RJR", "SCN", "TSC", "UCH"}


def test_categories_match_spec():
    assert st.OPEN == SPEC_OPEN
    assert st.CANCELED == SPEC_CANCELED
    assert st.REJECTED == {"REJ"}
    assert st.FILLED == {"FLL", "FLP"}


def test_categories_are_disjoint():
    groups = [st.OPEN, st.CANCELED, st.REJECTED, st.FILLED]
    for i, a in enumerate(groups):
        for b in groups[i + 1:]:
            assert not (a & b)


def test_ucn_is_open_not_canceled():
    assert st.category("UCN") == "open"
    assert "UCN" in st.LIVE


def test_unknown_code():
    assert st.category("XYZ") == "unknown"
    assert st.category("") == "unknown"


def test_live_is_open():
    assert st.LIVE == st.OPEN


def test_cancellable_excludes_pending_and_paused():
    # DON: GTC session pause is never auto-cancelled (§140). UCN: cancel already
    # pending. LAT: too late, a fill is coming.
    assert {"DON", "UCN", "LAT"}.isdisjoint(st.CANCELLABLE)
    assert {"ACK", "OPN", "FPR"} <= st.CANCELLABLE
    assert st.CANCELLABLE <= st.LIVE


def test_flp_is_terminal_fpr_is_live():
    assert "FLP" not in st.LIVE and "FPR" in st.LIVE


def test_watch_set():
    assert st.WATCH == {"UCN", "LAT", "BRO", "RJC", "UCH", "RJR", "SCN", "TSC"}


def test_cancel_pending_is_live_and_not_cancellable():
    assert st.CANCEL_PENDING == {"UCN", "LAT"}
    assert st.CANCEL_PENDING <= st.LIVE
    assert st.CANCEL_PENDING.isdisjoint(st.CANCELLABLE)
