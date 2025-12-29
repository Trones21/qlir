"""
Binance REST API contracts for the /api/v3/klines endpoint.

This module is OBSERVATIONAL ONLY.

It verifies documented (or assumed) invariants of Binance kline responses
and emits diagnostics when those invariants are violated.

This code MUST NOT:
- mutate data
- sort data
- normalize data
- enforce policy
- affect control flow

Its sole purpose is to answer:
    "Did the Binance REST API behave as expected for this response?"
"""

from __future__ import annotations

from typing import Any
import logging


def verify_binance_rest_kline_invariants(
    data: list[list[Any]],
    *,
    interval_ms: int,
) -> dict[str, Any]:
    """
    Verify low-cost, high-signal invariants of a Binance REST kline response.

    Returns a dict of observational facts. No side effects.

    Invariants checked:
    - ordering by openTime (ascending)
    - edge contiguity (first + (n-1)*interval == last)
    - closeTime consistency for the first candle (spot check)

    NOTE:
    - This does NOT check internal gaps via iteration.
    - This does NOT validate schema shape beyond required indices.
    """

    n_items = len(data)

    invariants: dict[str, Any] = {
        "n_items": n_items,
        "ordered": True,
        "edge_contiguous": True,
        "close_time_consistent": True,
    }

    if n_items == 0:
        return invariants

    try:
        first_open = int(data[0][0])
        last_open = int(data[-1][0])
    except Exception:
        # If this fails, the response is fundamentally malformed
        invariants["ordered"] = False
        invariants["edge_contiguous"] = False
        invariants["close_time_consistent"] = False
        return invariants

    invariants["first_open"] = first_open
    invariants["last_open"] = last_open

    # Ordering invariant (cheap check, no sort)
    if n_items >= 2:
        try:
            second_open = int(data[1][0])
            if second_open < first_open:
                invariants["ordered"] = False
        except Exception:
            invariants["ordered"] = False

    # Edge contiguity invariant (detects truncation or gaps without looping)
    if n_items >= 2:
        expected_last_open = first_open + (n_items - 1) * interval_ms
        if last_open != expected_last_open:
            invariants["edge_contiguous"] = False

    # closeTime sanity check (spot check first candle only)
    try:
        first_close_time = int(data[0][6])
        expected_close_time = first_open + interval_ms - 1
        if first_close_time != expected_close_time:
            invariants["close_time_consistent"] = False
            invariants["expected_close_time"] = expected_close_time
            invariants["actual_close_time"] = first_close_time
    except Exception:
        invariants["close_time_consistent"] = False

    return invariants


def audit_binance_rest_kline_invariants(
    data: list[list[Any]],
    *,
    interval_ms: int,
    log: logging.Logger,
) -> dict[str, Any]:
    """
    Wrapper around verify_binance_rest_kline_invariants() that emits
    diagnostics for violated invariants.

    This function:
    - logs warnings
    - returns invariant facts unchanged
    - does NOT alter program behavior
    """

    invariants = verify_binance_rest_kline_invariants(
        data,
        interval_ms=interval_ms,
    )

    if not invariants.get("ordered", True):
        log.warning(
            "Binance REST klines not ordered by openTime",
            extra=invariants,
        )

    if not invariants.get("edge_contiguous", True):
        log.warning(
            "Binance REST klines not contiguous at edges",
            extra=invariants,
        )

    if not invariants.get("close_time_consistent", True):
        log.warning(
            "Binance REST kline closeTime invariant violated",
            extra=invariants,
        )

    return invariants
