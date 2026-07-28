"""
Equity-aware slice inspection.

IMPORTANT divergence from Binance: crypto klines are 24/7, so Binance judges completeness
by contiguous interval spacing (any gap => sparsity). Equities are closed nights, weekends,
and holidays, so interval-spacing gap detection would flag almost every stock slice.

Instead, a stock window is judged purely by *time*:
  - fully elapsed window  -> COMPLETE   (whatever bars IBKR returned are all there are)
  - not yet elapsed        -> PARTIAL    (still forming)
  - fully elapsed + no bars -> COMPLETE (legitimately empty: closed market / pre-listing;
                                         marked HISTORICAL_SPARSITY so it is not retried forever)
"""

from __future__ import annotations

from dataclasses import dataclass

from qlir.data.sources.common.slices.slice_status import SliceStatus
from qlir.data.sources.common.slices.slice_status_reason import SliceStatusReason


@dataclass
class InspectionResult:
    slice_status: SliceStatus
    slice_status_reason: SliceStatusReason
    n_items: int
    requested_first_open: int
    requested_last_open_implicit: int
    received_first_open: int | None
    received_last_open: int | None


def inspect_bars(
    rows: list[list] | None,
    *,
    requested_first_open: int,
    requested_last_open_implicit: int,
    interval_ms: int,
    now_ms: int,
) -> InspectionResult:
    # A window is "closed" once its last expected bar has fully elapsed.
    window_closed = (requested_last_open_implicit + interval_ms) <= now_ms

    n = len(rows) if rows else 0

    if n == 0:
        if window_closed:
            # Legitimately empty (market closed / before listing) — terminal, don't retry.
            return InspectionResult(
                slice_status=SliceStatus.COMPLETE,
                slice_status_reason=SliceStatusReason.HISTORICAL_SPARSITY,
                n_items=0,
                requested_first_open=requested_first_open,
                requested_last_open_implicit=requested_last_open_implicit,
                received_first_open=None,
                received_last_open=None,
            )
        return InspectionResult(
            slice_status=SliceStatus.PARTIAL,
            slice_status_reason=SliceStatusReason.STILL_FORMING,
            n_items=0,
            requested_first_open=requested_first_open,
            requested_last_open_implicit=requested_last_open_implicit,
            received_first_open=None,
            received_last_open=None,
        )

    opens = sorted(int(r[0]) for r in rows)
    first_open, last_open = opens[0], opens[-1]

    status = SliceStatus.COMPLETE if window_closed else SliceStatus.PARTIAL
    reason = SliceStatusReason.NONE if window_closed else SliceStatusReason.STILL_FORMING

    return InspectionResult(
        slice_status=status,
        slice_status_reason=reason,
        n_items=n,
        requested_first_open=requested_first_open,
        requested_last_open_implicit=requested_last_open_implicit,
        received_first_open=first_open,
        received_last_open=last_open,
    )
