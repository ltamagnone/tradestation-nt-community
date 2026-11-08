"""§266 A: TradeStationStreamClient.request_reconnect lever."""
import asyncio

import pytest

from tradestation_nt_community.streaming import client as streaming_mod
from tradestation_nt_community.streaming.client import TradeStationStreamClient

ORDERS = "https://mock.ts.com/v3/brokerage/stream/accounts/X/orders"
BARS = "https://mock.ts.com/v3/marketdata/stream/barcharts/GCZ26"


def _client():
    return TradeStationStreamClient(lambda: "t", "https://mock.ts.com/v3", reconnect_delay_secs=0.01)


def _fake_httpx(opens):
    """Connection i yields its listed lines then idles forever (an idle stream)."""
    class _Resp:
        status_code = 200

        def __init__(self, i):
            self.i = i

        async def __aenter__(self): return self
        async def __aexit__(self, *a): pass

        async def aiter_lines(self):
            for line in ['{"OrderID": "A%d", "Status": "OPN"}' % self.i]:
                yield line
            await asyncio.Event().wait()

    class _Client:
        def __init__(self, **kw): pass
        async def __aenter__(self): return self
        async def __aexit__(self, *a): pass

        def stream(self, *a, **kw):
            opens.append(1)
            return _Resp(len(opens) - 1)

    return _Client


@pytest.mark.asyncio
async def test_request_reconnect_yields_sentinel(monkeypatch):
    opens = []
    monkeypatch.setattr(streaming_mod.httpx, "AsyncClient", _fake_httpx(opens))
    sc = _client()
    gen = sc._stream(ORDERS)
    assert (await gen.__anext__()) == {"OrderID": "A0", "Status": "OPN"}
    assert sc.request_reconnect("orders") is True
    assert (await asyncio.wait_for(gen.__anext__(), 2)) == {"_reconnected": True}
    assert (await asyncio.wait_for(gen.__anext__(), 2)) == {"OrderID": "A1", "Status": "OPN"}
    assert len(opens) == 2
    await gen.aclose()
    assert sc.request_reconnect("orders") is False


@pytest.mark.asyncio
async def test_request_reconnect_without_stream_returns_false():
    sc = _client()
    assert sc.request_reconnect("orders") is False
    assert sc.request_reconnect("bars") is False
    assert sc.request_reconnect() is False


@pytest.mark.asyncio
async def test_request_reconnect_leaves_bar_streams_alone(monkeypatch):
    opens = []
    monkeypatch.setattr(streaming_mod.httpx, "AsyncClient", _fake_httpx(opens))
    sc = _client()
    gen = sc._stream(BARS)
    assert (await gen.__anext__())["OrderID"] == "A0"
    assert sc.request_reconnect("orders") is False
    nxt = asyncio.ensure_future(gen.__anext__())
    await asyncio.sleep(0.1)
    assert not nxt.done() and len(opens) == 1
    nxt.cancel()
    with pytest.raises(BaseException):
        await nxt
