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


@pytest.mark.asyncio
async def test_request_reconnect_false_when_nothing_armed(monkeypatch):
    opens = []
    monkeypatch.setattr(streaming_mod.httpx, "AsyncClient", _fake_httpx(opens))
    sc = _client()
    gen = sc._stream(ORDERS)
    # generator not started: handle is not even registered / no future armed
    assert sc.request_reconnect("orders") is False
    assert (await gen.__anext__())["OrderID"] == "A0"
    assert sc.request_reconnect("orders") is True
    assert sc.request_reconnect("orders") is False        # already requested, not re-armed
    assert (await asyncio.wait_for(gen.__anext__(), 2)) == {"_reconnected": True}
    # consumer's catch-up after the sentinel: the new connection is not armed yet
    assert sc.request_reconnect("orders") is False
    await gen.aclose()


@pytest.mark.asyncio
async def test_outer_cancel_during_reconnect_request_ends_stream(monkeypatch):
    opens = []

    class _SlowResp:
        status_code = 200

        async def __aenter__(self): return self
        async def __aexit__(self, *a): pass

        async def aiter_lines(self):
            yield '{"OrderID": "A0", "Status": "OPN"}'
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                await asyncio.sleep(0.3)        # slow to die: outer cancel lands meanwhile
                raise

    class _Client:
        def __init__(self, **kw): pass
        async def __aenter__(self): return self
        async def __aexit__(self, *a): pass

        def stream(self, *a, **kw):
            opens.append(1)
            return _SlowResp()

    monkeypatch.setattr(streaming_mod.httpx, "AsyncClient", _Client)
    sc = _client()
    got = []

    async def consume():
        async for ev in sc._stream(ORDERS):
            got.append(ev)

    task = asyncio.ensure_future(consume())
    await asyncio.sleep(0.1)
    assert got == [{"OrderID": "A0", "Status": "OPN"}]
    assert sc.request_reconnect("orders") is True
    await asyncio.sleep(0.1)                    # consumer now awaits the dying read
    task.cancel()
    await asyncio.wait_for(task, 2)              # the client treats a cancel as a clean stop
    assert task.done() and len(opens) == 1                       # no reconnect after the stop
