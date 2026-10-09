"""TradeStation order status categories — the single source of truth.

Copied verbatim from the TradeStation OpenAPI spec (schemas `Status` /
`HistoricalStatus`, "Status filters can be used according to the order category"),
saved at my-nautilus-strategies/docs/api_reference/specificationOpenAPI2026.json.

Do not relabel codes from memory: on 2026-10-08 `UCN` turned out to be
"Cancel Sent" (Open), not "User Canceled" — treating it as final made the §111
verifier cancel 8 live exits.  No imports beyond the stdlib: the agent's test
conftest loads this file standalone.
"""
from __future__ import annotations

OPEN: frozenset[str] = frozenset({
    "ACK",    # Received
    "ASS",    # Option Assignment
    "BRC",    # Bracket Canceled
    "BRF",    # Bracket Filled
    "BRO",    # Broken
    "CHG",    # Change
    "CND",    # Condition Met
    "COR",    # Fill Corrected
    "DIS",    # Dispatched
    "DOA",    # Dead
    "DON",    # Queued (GTC session pause at the daily close)
    "ECN",    # Expiration Cancel Request
    "EXE",    # Option Exercise
    "FPR",    # Partial Fill (Alive)
    "LAT",    # Too Late to Cancel
    "OPN",    # Sent
    "OSO",    # OSO Order
    "OTHER",  # OrderStatus not mapped
    "PLA",    # Sending
    "REC",    # Big Brother Recall Request
    "RJC",    # Cancel Request Rejected
    "RPD",    # Replace Pending
    "RSN",    # Replace Sent
    "STP",    # Stop Hit
    "STT",    # OrderStatus Message
    "SUS",    # Suspended
    "UCN",    # Cancel Sent  (NOT a cancel — the request is still pending)
})
CANCELED: frozenset[str] = frozenset({
    "CAN",  # Canceled
    "EXP",  # Expired
    "OUT",  # UROut
    "RJR",  # Change Request Rejected
    "SCN",  # Big Brother Recall
    "TSC",  # Trade Server Canceled
    "UCH",  # Replaced
})
REJECTED: frozenset[str] = frozenset({"REJ"})
FILLED: frozenset[str] = frozenset({
    "FLL",  # Filled
    "FLP",  # Partial Fill (UROut) — terminal: rest of the order is out
})

# Still on the broker's book and able to fill.
LIVE: frozenset[str] = OPEN
# Safe for automation to cancel. Excludes DON (GTC session pause, §140 — never
# auto-cancelled), UCN (cancel already pending) and LAT (too late, a fill is coming).
CANCELLABLE: frozenset[str] = OPEN - {"DON", "UCN", "LAT"}
# Ambiguous or never observed in production: the adapter parks these for a broker
# recheck: it logs [TS-STATUS-WATCH] on arrival and [TS-STATUS-RECHECK] with the
# outcome, so the operator learns what they mean.
WATCH: frozenset[str] = frozenset({"UCN", "LAT", "BRO", "RJC", "UCH", "RJR", "SCN", "TSC"})
# A cancel is already in flight (UCN) or too late (LAT): never re-cancel, never treat as gone.
CANCEL_PENDING: frozenset[str] = frozenset({"UCN", "LAT"})


def category(code: str) -> str:
    """'open' | 'canceled' | 'rejected' | 'filled' | 'unknown' for a TS status code."""
    if code in OPEN:
        return "open"
    if code in CANCELED:
        return "canceled"
    if code in REJECTED:
        return "rejected"
    if code in FILLED:
        return "filled"
    return "unknown"
