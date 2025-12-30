from __future__ import annotations
from typing import Iterable, Optional, Tuple
from dataclasses import dataclass, replace
from enum import Enum, auto
from typing import Optional


from qlir.data.sources.binance.endpoints.klines.time_range import _now_ms
from qlir.data.sources.common.slices.slice_status import SliceStatus
from qlir.data.sources.common.slices.slice_status_reason import SliceStatusReason
from qlir.data.sources.common.slices.slice_status_policy import SliceStatusPolicy

class InspectionMode(Enum):
    QUICK = auto()
    FULL = auto()

@dataclass
class InspectionResult:
    slice_status: SliceStatus
    
    n_items: int
    received_eq_requested: bool

    requested_first_open: int
    requested_last_open_implicit: int

    received_first_open: Optional[int]
    received_last_open: Optional[int]

    inspection_mode: InspectionMode

    slice_status_reason: SliceStatusReason | None = None
    integrity: SliceIntegrityDetails | None = None


    def mark_complete(self, *, reason: SliceStatusReason) -> None:
        if reason not in (
            SliceStatusReason.NONE,
            SliceStatusReason.HISTORICAL_SPARSITY,
        ):
            raise ValueError(f"Invalid completion reason: {reason}")

        self.slice_status = SliceStatus.COMPLETE
        self.slice_status_reason = reason

    def mark_partial(self, *, reason: SliceStatusReason) -> None:
        if reason not in (
            SliceStatusReason.STILL_FORMING,
            SliceStatusReason.AWAITING_UPSTREAM,
        ):
            raise ValueError(f"Invalid partial reason: {reason}")

        self.slice_status = SliceStatus.PARTIAL
        self.slice_status_reason = reason
    
    def mark_failed(self, *, reason: SliceStatusReason) -> None:
        if reason not in (
            SliceStatusReason.HTTP_ERROR,
            SliceStatusReason.EXCEPTION,
        ):
            raise ValueError(f"Invalid failure reason: {reason}")

        self.slice_status = SliceStatus.FAILED
        self.slice_status_reason = reason




@dataclass(frozen=True)
class SliceIntegrityDetails:
    n: int
    uniq_open: int
    first_open: int
    last_open: int
    min_delta: Optional[int]
    max_delta: Optional[int]
    n_gaps: int
    max_gap_ms: int

    @property
    def has_gaps(self) -> bool:
        return self.n_gaps > 0

    @property
    def is_contiguous(self) -> bool:
        return self.n_gaps == 0 and self.uniq_open == self.n



def fast_inspect_slice(
    *,
    n_items: int,
    interval_ms: int,
    requested_first_open: int,
    requested_last_open_implicit: int,
    received_first_open: int | None,
    received_last_open: int | None,
) -> InspectionResult:

    expected_n = (
        (requested_last_open_implicit - requested_first_open) // interval_ms
    ) + 1

    received_eq_requested = n_items == expected_n

    return InspectionResult(
        slice_status=(
            SliceStatus.COMPLETE if received_eq_requested else SliceStatus.PARTIAL
        ),
        n_items=n_items,
        received_eq_requested=received_eq_requested,
        requested_first_open=requested_first_open,
        requested_last_open_implicit=requested_last_open_implicit,
        received_first_open=received_first_open,
        received_last_open=received_last_open,
        inspection_mode=InspectionMode.QUICK,
    )


def extract_slice_receipt(
    data: list[list] | None,
) -> tuple[int, Optional[int], Optional[int]]:
    """
    Extract receipt facts from a kline slice.

    Returns:
        n_items: number of rows received
        received_first_open: first open_time (ms) or None
        received_last_open: last open_time (ms) or None

    Notes:
        - Assumes exchange ordering if data is non-empty
        - Does NOT sort
        - Does NOT validate continuity
        - Pure fact extraction
    """
    if not data:
        return 0, None, None

    return (
        len(data),
        int(data[0][0]),
        int(data[-1][0]),
    )


from typing import Optional


def inspect_slice_integrity(
    raw: list[list],
    interval_ms: int,
) -> SliceIntegrityDetails:
    """
    Perform a full integrity analysis on a kline slice.

    Notes:
    - O(n)
    - Sorts opens defensively
    - Does NOT decide policy or status
    - Pure fact extraction
    """

    # This function should never be called on empty slices,
    # but we still guard for correctness.
    if not raw:
        return SliceIntegrityDetails(
            n=0,
            uniq_open=0,
            first_open=0,
            last_open=0,
            min_delta=None,
            max_delta=None,
            n_gaps=0,
            max_gap_ms=0,
        )

    opens = [int(r[0]) for r in raw]  # open_time ms
    opens_sorted = sorted(opens)

    uniq = len(set(opens_sorted))

    deltas = [
        opens_sorted[i + 1] - opens_sorted[i]
        for i in range(len(opens_sorted) - 1)
    ]

    gaps = [d for d in deltas if d != interval_ms]

    return SliceIntegrityDetails(
        n=len(opens_sorted),
        uniq_open=uniq,
        first_open=opens_sorted[0],
        last_open=opens_sorted[-1],
        min_delta=min(deltas) if deltas else None,
        max_delta=max(deltas) if deltas else None,
        n_gaps=len(gaps),
        max_gap_ms=max(gaps) if gaps else 0,
    )



def inspect_res(
    raw: list[list],
    *,
    requested_first_open: int,
    requested_last_open_implicit: int,
    interval_ms: int,
    limit: int,
) -> InspectionResult:
    """
    inspect_res invariants:
    - fast path returns via replace()
    - slow path always rebuilds InspectionResult
    - InspectionResult is never partially mutated and returned
    """

    # ---- receipt facts (always) ----
    n_items, r_first, r_last = extract_slice_receipt(raw)

    # ---- fast path (completeness only) ----
    inspection = fast_inspect_slice(
        n_items=n_items,
        interval_ms=interval_ms,
        requested_first_open=requested_first_open,
        requested_last_open_implicit=requested_last_open_implicit,
        received_first_open=r_first,
        received_last_open=r_last,
    )

    # 🔒 single escalation gate
    if inspection.received_eq_requested:
        
        return replace(
            inspection,
            slice_status=SliceStatus.COMPLETE,
            slice_status_reason=SliceStatusReason.NONE,
            inspection_mode = InspectionMode.QUICK
        )

    # ---- slow path (integrity facts) ----
    inspection.inspection_mode = InspectionMode.FULL
    integrity = inspect_slice_integrity(raw, interval_ms)


    reason = determine_slice_status_reason_from_integrity(integrity=integrity, 
                                                          requested_last_open_implicit=requested_last_open_implicit,
                                                          now_ms=_now_ms())
 
    slice_status = SliceStatusPolicy.from_success_reason(reason)

    return InspectionResult(
        slice_status=slice_status,
        slice_status_reason=reason,
        n_items=n_items,
        received_eq_requested=False,
        requested_first_open=requested_first_open,
        requested_last_open_implicit=requested_last_open_implicit,
        received_first_open=integrity.first_open,
        received_last_open=integrity.last_open,
        inspection_mode=InspectionMode.FULL,
        integrity=integrity,
    )


def determine_slice_status_reason_from_integrity(
    *,
    integrity: SliceIntegrityDetails,
    requested_last_open_implicit: int,
    now_ms: int,
) -> SliceStatusReason:
    """
    Assumes:
    - HTTP 200
    - parsing succeeded
    - received_eq_requested == False
    """

    # 1. No data at all → terminal
    if integrity.n == 0:
        return SliceStatusReason.HISTORICAL_SPARSITY

    # 2. Internal structural gaps → terminal
    if integrity.has_gaps:
        return SliceStatusReason.HISTORICAL_SPARSITY

    # 3. Trailing incompleteness → temporal
    if integrity.last_open < requested_last_open_implicit:
        if now_ms < requested_last_open_implicit:
            return SliceStatusReason.STILL_FORMING
        else:
            return SliceStatusReason.AWAITING_UPSTREAM

    # 4. Impossible state
    raise ValueError(
        "Incomplete slice with no gaps and no trailing deficit; "
        "this violates invariants"
    )


# def determine_slice_status_on_success(
#     *,
#     reason: SliceStatusReason,
# ) -> SliceStatus:
#     """
#     Determine the SliceStatus for a successful (HTTP 200, parsed) fetch.

#     Raises:
#         ValueError if the (received_eq_requested, reason) combination
#         violates expected invariants.
#     """

#     if reason == SliceStatusReason.HISTORICAL_SPARSITY:
#         return SliceStatus.COMPLETE

#     if reason in (
#         SliceStatusReason.STILL_FORMING,
#         SliceStatusReason.AWAITING_UPSTREAM,
#     ):
#         return SliceStatus.PARTIAL

#     raise ValueError(
#         f"Unhandled success-path SliceStatusReason: {reason}"
#     )

# def slice_status_from_error_reason(
#     reason: SliceStatusReason,
# ) -> SliceStatus:
#     if reason in (
#         SliceStatusReason.HTTP_ERROR,
#         SliceStatusReason.EXCEPTION,
#     ):
#         return SliceStatus.FAILED

#     raise ValueError(f"Invalid error-path reason: {reason}")
