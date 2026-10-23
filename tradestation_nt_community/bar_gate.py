"""Which streamed TradeStation bar events become NautilusTrader bars (strategies LESSONS §207).

TradeStation v3 bar events carry ``BarStatus`` ("Open" while the bar forms, "Closed" once it is
final; the Closed event comes ~0.1 s after the boundary, no trade needed), ``IsRealtime``
(False for the history seed sent on every connect) and ``TimeStamp`` = the bar's END.

One gate per bar type lives on the data client, so its watermark - the newest bar end already
delivered, from warm-up history or live - survives stream reconnects and the watchdog's task
restarts. Anything at or before the watermark is a seed or a repeat and is dropped.

Late thin-hour bars (plan 2026-10-23): in thin hours TradeStation closes a bar only with the
next trade, can merge the next period's trades into it, and can then never send the next bar.
The gate can therefore also
- release the forming bar as it stood at end + grace (``release_due``), remembering what it
  released so a later Closed that differs is recorded in ``diverged``;
- name the periods missing between the watermark and a later bar (``missing_periods``);
- hold a barrier while such a period is fetched from REST history: what would be emitted above
  it is parked (and counts as emitted), backfilled bars are ``mark_emitted``, and
  ``release_hold`` hands the parked bars back in order;
- keep snapshots of the forming bar at end + offsets, so the release rule can be measured.
All of it is pure state; the data client owns the timers and the I/O.
"""

from dataclasses import dataclass, field

import pandas as pd


def bar_end_ns(ts: str) -> int | None:
    """UNIX ns of a TradeStation bar ``TimeStamp`` (bar END); None if missing or unparseable."""
    if not ts:
        return None
    try:
        return int(pd.Timestamp(ts).value)
    except (ValueError, TypeError):
        return None


_OHLCV_KEYS = ("Open", "High", "Low", "Close", "TotalVolume")
_RELEASED_KEEP = 4  # released snapshots kept for the divergence check
_SNAPSHOT_KEEP = 4  # bars whose snapshots are kept


def ohlcv(event: dict) -> tuple:
    """(O, H, L, C, V) of a TradeStation bar event, as floats where they parse."""
    out = []
    for k in _OHLCV_KEYS:
        v = event.get(k)
        try:
            out.append(float(v))
        except (TypeError, ValueError):
            out.append(v)
    return tuple(out)


@dataclass
class BarCloseGate:
    last_emitted_ns: int | None = None  # newest bar end delivered (warm-up or live)
    _pending: dict | None = None  # newest update of the forming bar
    _pending_ns: int = 0
    _replay: bool = False  # this connection may replay history newer than the watermark
    _live_emitted: bool = False
    # end + offset (ns) at which the forming bar's state is snapshotted (on_event with now_ns)
    snapshot_offsets_ns: tuple[int, ...] = (0, 2_000_000_000, 10_000_000_000, 30_000_000_000)
    _released: dict = field(default_factory=dict)  # ts -> OHLCV released (or shadow candidate)
    diverged: list = field(default_factory=list)  # (ts, snapshot, event); the consumer pops
    _snaps: dict = field(default_factory=dict)  # ts -> {offset: (wall_ns, event)}
    _barrier_ns: int | None = None  # held: anything above it is parked
    parked: list = field(default_factory=list)  # (ts, raw, released) while held
    _backfill_active: list | None = None  # periods of the backfill in flight
    _backfill_hwm: int | None = None  # newest period a finished backfill handled

    @property
    def pending_end_ns(self) -> int | None:
        """End of the forming bar waiting for its Closed; None if there is none."""
        return self._pending_ns if self._pending is not None else None

    def seed_from_history(self, ts_ns: int) -> None:
        """Warm-up history delivered bars up to ``ts_ns`` (its last CLOSED bar).

        Ignored once a live bar was emitted: a later history read can hold a bar whose
        live Closed event is still in flight, and must not swallow it.
        """
        if not self._live_emitted:
            self._advance(ts_ns)

    def reset_connection(self) -> None:
        """A (re)connect starts: the new connection re-sends the forming bar in its seed.

        History in that seed is replayed only when a watermark already exists, so a first
        connection never replays bars the strategy did not ask for.
        """
        self._pending = None
        self._replay = self.last_emitted_ns is not None

    def on_event(self, event: dict, ts_ns: int, now_ns: int | None = None) -> list[dict]:
        """Raw bars to emit for one streamed event, oldest first.

        ``now_ns`` (wall clock) keeps snapshots of the forming bar; None keeps none.
        """
        if event.get("BarStatus") == "Closed" and ts_ns in self._released:
            snap = self._released.pop(ts_ns)  # checked once per released bar
            if ohlcv(event) != snap:
                self.diverged.append((ts_ns, snap, event))
        wm = self.last_emitted_ns
        if wm is not None and ts_ns <= wm:
            return []  # a seed or repeat of a delivered bar
        out: list[dict] = []
        if self._pending is not None and self._pending_ns < ts_ns:
            if wm is None or self._pending_ns > wm:
                self._commit(self._pending, self._pending_ns, out)  # no Closed came: fallback
            self._pending = None
        if event.get("BarStatus") != "Closed":
            self._pending, self._pending_ns = event, ts_ns  # still forming: wait for Closed
            if now_ns is not None:
                self._snapshot(event, ts_ns, now_ns)
        elif event.get("IsRealtime") is False and not self._replay:
            self._advance(ts_ns)  # history from before any watermark: not ours to replay
        else:
            self._commit(event, ts_ns, out)
            if self._pending is not None and self._pending_ns <= ts_ns:
                self._pending = None
        return out

    def release_due(self, now_ns: int, grace_ns: int, commit: bool = True) -> list[dict]:
        """The forming bar, once ``now_ns`` >= its end + ``grace_ns`` and no Closed came.

        With ``commit`` it counts as emitted (watermark) and is parked while held. Without, the
        emission state is untouched (shadow) and only the divergence check remembers it.
        """
        raw, ts_ns = self._pending, self._pending_ns
        if raw is None or now_ns < ts_ns + grace_ns:
            return []
        wm = self.last_emitted_ns
        if wm is not None and ts_ns <= wm:
            return []
        if ts_ns not in self._released:
            self._released[ts_ns] = ohlcv(raw)
            while len(self._released) > _RELEASED_KEEP:
                del self._released[next(iter(self._released))]
        if not commit:
            return [raw]
        self._pending = None
        out: list[dict] = []
        self._commit(raw, ts_ns, out, released=True)
        return out

    def snapshots(self, ts_ns: int) -> dict:
        """{offset_ns: (wall_ns, event)}: the bar's last update at or before end + offset."""
        return dict(self._snaps.get(ts_ns, {}))

    # -- gaps and the backfill barrier --------------------------------------------------------

    def effective_watermark(self, ts_ns: int) -> int | None:
        """The newest bar end that is delivered once an event at ``ts_ns`` is fed: the
        watermark, or the forming bar that event will release as a fallback."""
        wm = self.last_emitted_ns
        if wm is None:
            return None
        if self._pending is not None and wm < self._pending_ns < ts_ns:
            return self._pending_ns
        return wm

    def missing_periods(self, ts_ns: int, step_ns: int, max_periods: int = 3) -> list[int]:
        """Bar ends strictly between the (effective) watermark and ``ts_ns`` on the step grid.

        Empty without a watermark, with more than ``max_periods`` (a session break, a long
        outage), while a backfill is pending, and for periods a finished backfill handled.
        """
        if self._backfill_active is not None or self._barrier_ns is not None or step_ns <= 0:
            return []
        low = self.effective_watermark(ts_ns)
        if low is None or ts_ns <= low:
            return []
        periods = list(range(low + step_ns, ts_ns, step_ns))
        if len(periods) > max_periods:
            return []
        hwm = self._backfill_hwm
        return [p for p in periods if hwm is None or p > hwm]

    @property
    def backfill_periods(self) -> list[int] | None:
        """Periods of the backfill in flight; None if there is none."""
        return list(self._backfill_active) if self._backfill_active is not None else None

    def begin_backfill(self, periods: list[int]) -> None:
        self._backfill_active = sorted(periods)

    def end_backfill(self) -> None:
        """The backfill finished (emitted, skipped or failed): its periods never re-trigger."""
        if self._backfill_active:
            last = self._backfill_active[-1]
            if self._backfill_hwm is None or last > self._backfill_hwm:
                self._backfill_hwm = last
        self._backfill_active = None

    def abort_backfill(self) -> None:
        """A reset (reconnect, unsubscribe, disconnect) cancelled the backfill.

        Parked bars never reached a strategy: the watermark goes back to the barrier, so the
        new connection's seed replays them in order. The periods are not marked handled.
        """
        self._backfill_active = None
        b = self._barrier_ns
        if b is None:
            return
        if self.last_emitted_ns is not None and self.last_emitted_ns > b:
            self.last_emitted_ns = b
        for ts in [t for t in self._released if t > b]:
            del self._released[ts]
        self.parked = []
        self._barrier_ns = None

    def hold(self, barrier_ns: int) -> None:
        """Park every emission above ``barrier_ns`` until ``release_hold``."""
        self._barrier_ns = barrier_ns

    def release_hold(self) -> list[tuple[dict, bool]]:
        """The parked bars in ts order as (raw, released); clears the hold."""
        out = [(raw, rel) for _ts, raw, rel in sorted(self.parked, key=lambda p: p[0])]
        self.parked = []
        self._barrier_ns = None
        return out

    def backfill_ok(self, ts_ns: int) -> bool:
        """A backfilled bar at ``ts_ns`` is new and lands before every parked bar."""
        if self._barrier_ns is not None:
            if ts_ns <= self._barrier_ns:
                return False
            return not self.parked or ts_ns < min(p[0] for p in self.parked)
        wm = self.last_emitted_ns
        return wm is None or ts_ns > wm

    def mark_emitted(self, ts_ns: int) -> None:
        """A backfilled bar was emitted outside the gate."""
        self._emitted(ts_ns)
        if self._barrier_ns is not None and ts_ns > self._barrier_ns:
            self._barrier_ns = ts_ns

    def _commit(self, raw: dict, ts_ns: int, out: list, released: bool = False) -> None:
        self._emitted(ts_ns)
        if self._barrier_ns is not None and ts_ns > self._barrier_ns:
            self.parked.append((ts_ns, raw, released))
        else:
            out.append(raw)

    def _snapshot(self, event: dict, ts_ns: int, now_ns: int) -> None:
        snaps = self._snaps.setdefault(ts_ns, {})
        for off in self.snapshot_offsets_ns:
            if now_ns <= ts_ns + off:
                snaps[off] = (now_ns, event)
        while len(self._snaps) > _SNAPSHOT_KEEP:
            del self._snaps[min(self._snaps)]

    def _advance(self, ts_ns: int) -> None:
        if self.last_emitted_ns is None or ts_ns > self.last_emitted_ns:
            self.last_emitted_ns = ts_ns

    def _emitted(self, ts_ns: int) -> None:
        self._advance(ts_ns)
        self._live_emitted = True
