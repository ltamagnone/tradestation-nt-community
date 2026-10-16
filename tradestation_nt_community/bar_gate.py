"""Which streamed TradeStation bar events become NautilusTrader bars (strategies LESSONS §207).

TradeStation v3 bar events carry ``BarStatus`` ("Open" while the bar forms, "Closed" once it is
final; the Closed event comes ~0.1 s after the boundary, no trade needed), ``IsRealtime``
(False for the history seed sent on every connect) and ``TimeStamp`` = the bar's END.

One gate per bar type lives on the data client, so its watermark - the newest bar end already
delivered, from warm-up history or live - survives stream reconnects and the watchdog's task
restarts. Anything at or before the watermark is a seed or a repeat and is dropped.
"""

from dataclasses import dataclass

import pandas as pd


def bar_end_ns(ts: str) -> int | None:
    """UNIX ns of a TradeStation bar ``TimeStamp`` (bar END); None if missing or unparseable."""
    if not ts:
        return None
    try:
        return int(pd.Timestamp(ts).value)
    except (ValueError, TypeError):
        return None


@dataclass
class BarCloseGate:
    last_emitted_ns: int | None = None  # newest bar end delivered (warm-up or live)
    _pending: dict | None = None  # newest update of the forming bar
    _pending_ns: int = 0
    _replay: bool = False  # this connection may replay history newer than the watermark
    _live_emitted: bool = False

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

    def on_event(self, event: dict, ts_ns: int) -> list[dict]:
        """Raw bars to emit for one streamed event, oldest first."""
        wm = self.last_emitted_ns
        if wm is not None and ts_ns <= wm:
            return []  # a seed or repeat of a delivered bar
        out: list[dict] = []
        if self._pending is not None and self._pending_ns < ts_ns:
            if wm is None or self._pending_ns > wm:
                out.append(self._pending)  # its Closed never came: fallback
                self._emitted(self._pending_ns)
            self._pending = None
        if event.get("BarStatus") != "Closed":
            self._pending, self._pending_ns = event, ts_ns  # still forming: wait for Closed
        elif event.get("IsRealtime") is False and not self._replay:
            self._advance(ts_ns)  # history from before any watermark: not ours to replay
        else:
            out.append(event)
            self._emitted(ts_ns)
            if self._pending is not None and self._pending_ns <= ts_ns:
                self._pending = None
        return out

    def _advance(self, ts_ns: int) -> None:
        if self.last_emitted_ns is None or ts_ns > self.last_emitted_ns:
            self.last_emitted_ns = ts_ns

    def _emitted(self, ts_ns: int) -> None:
        self._advance(ts_ns)
        self._live_emitted = True
