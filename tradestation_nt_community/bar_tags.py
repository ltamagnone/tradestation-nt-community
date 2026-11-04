"""Side-channel tags for late stream bars (strategies §260).

The data client tags a late stream bar here just before handing it to NautilusTrader, so a
consumer (the strategies runner) can look a received ``Bar`` up by ``(bar.bar_type,
bar.ts_event)``. NT's ``Bar`` has no field for it. A bar is late when TradeStation sent its
Closed with ``IsRealtime`` False, or more than one step + 10 s after its end. The late bars of
one reconnect batch are tagged together: every bar except the newest is ``superseded`` (a newer
bar of the same batch follows at once). ALL of them are superseded when the batch is flushed by
a newer bar of the same type emitted right after it: an on-time Closed bar, or a
``[BAR-RELEASE]`` of a newer bar (only the newest bar of a burst may place entries). An on-time
emission drops any stale tag of its own ``(bar_type, ts)``.

On-time bars, polled bars and DAY bars are never tagged: ``lookup`` answers None, and a reader
must treat None as "not late" (fail-open). Everything runs on the event loop; there is no lock.
Memory is bounded: the newest ``_KEEP`` tags per bar type.
"""

from __future__ import annotations

from dataclasses import dataclass

API = 1  # bumped on an incompatible change of BarTag or of tag/lookup
_KEEP = 64  # tags kept per bar type


@dataclass(frozen=True)
class BarTag:
    late: bool
    # A newer bar was emitted right after this one: the next bar of its late batch (all but the
    # newest), or - for every bar of the batch - a newer on-time Closed bar or [BAR-RELEASE]
    # of the same type that flushed the batch.
    superseded: bool
    is_realtime: bool | None  # the Closed event's IsRealtime; None when TradeStation omitted it
    lag_s: float  # wall clock minus bar end when the Closed arrived
    batch: tuple[int, int]  # (position, size), 1-based, oldest first


_store: dict[str, dict[int, BarTag]] = {}


def tag(bar_type, ts_ns: int, bar_tag: BarTag) -> None:
    """Record ``bar_tag`` for the bar ``(bar_type, ts_ns)`` (ts_ns = the bar's ts_event)."""
    tags = _store.setdefault(str(bar_type), {})
    ts = int(ts_ns)
    tags.pop(ts, None)  # a re-tag moves it to the newest end
    tags[ts] = bar_tag
    while len(tags) > _KEEP:
        del tags[next(iter(tags))]


def lookup(bar_type, ts_ns) -> BarTag | None:
    """The tag of ``(bar_type, ts_ns)``, or None (not tagged, evicted, or unusable input)."""
    try:
        tags = _store.get(str(bar_type))
        return tags.get(int(ts_ns)) if tags else None
    except (TypeError, ValueError):
        return None


def drop(bar_type, ts_ns) -> None:
    """Forget the tag of ``(bar_type, ts_ns)`` if there is one."""
    try:
        tags = _store.get(str(bar_type))
        if tags:
            tags.pop(int(ts_ns), None)
    except (TypeError, ValueError):
        pass


def clear() -> None:
    """Drop every tag (tests)."""
    _store.clear()
