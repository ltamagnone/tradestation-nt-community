"""Status categories must match the TradeStation OpenAPI spec verbatim
(docs/api_reference/specificationOpenAPI2026.json in my-nautilus-strategies)."""
import json
import os
import re
from pathlib import Path

import pytest

from tradestation_nt_community.common import order_status as st

SPEC_PATH_ENV = "TS_OPENAPI_SPEC"
_SIBLING = (Path(__file__).resolve().parents[2]
            / "my-nautilus-strategies/docs/api_reference/specificationOpenAPI2026.json")


def _spec_file():
    env = os.environ.get(SPEC_PATH_ENV)
    for cand in (Path(env) if env else None, _SIBLING):
        if cand is not None and cand.is_file():
            return cand
    return None


def _spec_categories(schema_name):
    """Parse the Status schema description: '- Open:' / '- Canceled:' ... then
    '- CODE - Label' lines (a deprecated code is struck through: '~CSN - ...~')."""
    path = _spec_file()
    schema = json.loads(path.read_text())["components"]["schemas"][schema_name]
    cats, cur = {}, None
    for line in schema["description"].splitlines():
        m = re.match(r"^\s*- (Open|Canceled|Rejected|Filled):\s*$", line)
        if m:
            cur = m.group(1).lower()
            cats[cur] = set()
            continue
        m = re.match(r"^\s+- ~?([A-Z]+)\s+[-\u2013]", line)
        if m and cur:
            cats[cur].add(m.group(1))
    return cats, set(schema["enum"])


@pytest.mark.skipif(
    _spec_file() is None,
    reason=f"TradeStation OpenAPI spec not found: set {SPEC_PATH_ENV} or place it at {_SIBLING}",
)
@pytest.mark.parametrize("schema", ["Status", "HistoricalStatus"])
def test_categories_match_real_spec(schema):
    cats, enum = _spec_categories(schema)
    assert st.OPEN == cats["open"]
    assert st.CANCELED == cats["canceled"]
    assert st.REJECTED == cats["rejected"]
    assert st.FILLED == cats["filled"]
    # The only documented code absent from the enum is the deprecated CSN.
    union = cats["open"] | cats["canceled"] | cats["rejected"] | cats["filled"]
    assert union - enum == {"CSN"}
    assert enum <= union


def test_csn_is_deprecated_cancel_pending_like_ucn():
    assert st.category("CSN") == "open"
    assert "CSN" in st.CANCEL_PENDING and "CSN" in st.WATCH
    assert "CSN" not in st.CANCELLABLE


def test_fixed_categories():
    assert st.REJECTED == {"REJ"}
    assert st.FILLED == {"FLL", "FLP"}
    assert {"CAN", "EXP", "OUT", "RJR", "SCN", "TSC", "UCH"} == st.CANCELED


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
    assert {"DON", "UCN", "CSN", "LAT"}.isdisjoint(st.CANCELLABLE)
    assert {"ACK", "OPN", "FPR"} <= st.CANCELLABLE
    assert st.CANCELLABLE <= st.LIVE


def test_flp_is_terminal_fpr_is_live():
    assert "FLP" not in st.LIVE and "FPR" in st.LIVE


def test_watch_set():
    assert st.WATCH == {"UCN", "CSN", "LAT", "BRO", "RJC", "UCH", "RJR", "SCN", "TSC"}


def test_cancel_pending_is_live_and_not_cancellable():
    assert st.CANCEL_PENDING == {"UCN", "CSN", "LAT"}
    assert st.CANCEL_PENDING <= st.LIVE
    assert st.CANCEL_PENDING.isdisjoint(st.CANCELLABLE)
