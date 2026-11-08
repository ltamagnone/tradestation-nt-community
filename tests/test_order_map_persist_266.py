"""§266 B3: order-map persist writes via json.dumps (one write) and once per OCO group."""
from __future__ import annotations

import json
from unittest.mock import MagicMock

from tradestation_nt_community.execution import TradeStationExecutionClient as C
from tests.test_late_accept_cancel import _group_stub


def test_persist_uses_dumps_single_write(tmp_path, monkeypatch):
    path = str(tmp_path / "om.json")
    stub = MagicMock()
    stub._order_map_path = path
    stub._ts_order_id_to_client_order_id = {"T1": "O-1", "T2": "O-2"}
    stub._ts_id_to_group = {}
    stub._node_group = "g"

    def boom(*a, **k):
        raise AssertionError("json.dump must not be used")

    monkeypatch.setattr(json, "dump", boom)
    C._persist_order_map(stub)
    with open(path) as f:
        data = json.load(f)
    assert data["T1"]["client_order_id"] == "O-1"
    assert data["T2"]["client_order_id"] == "O-2"
    assert data["T1"]["group"] == "g"
    C._write_order_map_file(stub, {"x": {"a": 1}})
    with open(path) as f:
        assert json.load(f) == {"x": {"a": 1}}


async def test_group_persists_once_before_accepts():
    stub, cmd = _group_stub([False, False])
    events = []
    stub._persist_order_map.side_effect = lambda: events.append("persist")
    stub.generate_order_accepted.side_effect = lambda **k: events.append("accept")
    await C._submit_order_group(stub, cmd, "OCO", [{}, {}])
    assert events == ["persist", "accept", "accept"]
