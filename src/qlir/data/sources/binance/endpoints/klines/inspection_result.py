from __future__ import annotations

from dataclasses import dataclass, replace
from enum import Enum, auto
import logging
from typing import Optional

from qlir.utils.str.color import Ansi, colorize
from qlir.utils.time.fmt import format_ts_human

log = logging.getLogger(__name__)

from qlir.data.sources.binance.endpoints.klines.time_range import _now_ms
from qlir.data.sources.common.slices.slice_status import SliceStatus
from qlir.data.sources.common.slices.slice_status_policy import SliceStatusPolicy
from qlir.data.sources.common.slices.slice_status_reason import SliceStatusReason


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
    raw: list[list] | None,
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

    # handle when we receive empty data
    if not raw:
        return InspectionResult(
            n_items=0,
            requested_first_open=requested_first_open,
            requested_last_open_implicit=requested_last_open_implicit,
            received_eq_requested=False,
            received_first_open=None,
            received_last_open=None,
            slice_status=SliceStatus.EMPTY,
            slice_status_reason=SliceStatusReason.NO_DATA,
            inspection_mode=InspectionMode.QUICK,
        )

    # ---- receipt facts ----
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
    inspection.integrity = inspect_slice_integrity(raw, interval_ms)

    _partial_slice_logging(inspection, limit=limit)
    reason = determine_slice_status_reason_from_integrity(integrity=inspection.integrity, 
                                                          requested_last_open_implicit=requested_last_open_implicit,
                                                          limit=limit,
                                                          interval_ms=interval_ms,
                                                          now_ms=_now_ms())
 
    slice_status = SliceStatusPolicy.from_success_reason(reason)

    return InspectionResult(
        slice_status=slice_status,
        slice_status_reason=reason,
        n_items=n_items,
        received_eq_requested=False,
        requested_first_open=requested_first_open,
        requested_last_open_implicit=requested_last_open_implicit,
        received_first_open=inspection.integrity.first_open,
        received_last_open=inspection.integrity.last_open,
        inspection_mode=InspectionMode.FULL,
        integrity=inspection.integrity,
    )


def determine_slice_status_reason_from_integrity(
    *,
    integrity: SliceIntegrityDetails,
    requested_last_open_implicit: int,
    limit: int,
    interval_ms: int,
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

    # 4. No "Gaps" but missing data on either ends of the range (and this was from a long time ago (slice closed several slice intervals ago))
    # In this case we just set a hard boundary - using offset from now
    MAX_HISTORICAL_LAG_MULTIPLIER = 2.5  # policy constant
    slice_span_ms = limit * interval_ms
    historical_cutoff_ms = now_ms - (
        slice_span_ms * MAX_HISTORICAL_LAG_MULTIPLIER
    )
    
    if integrity.last_open < historical_cutoff_ms:
        n_intervals_ago = (now_ms - historical_cutoff_ms) / slice_span_ms
        log.info(
            "Historical sparsity inferred", extra={"last_open_received": integrity.last_open, 
                                                   "last_open_human": format_ts_human(integrity.last_open), 
                                                   "current_ms": now_ms,
                                                   "current_ms_human": format_ts_human(now_ms),
                                                   "slice_span_ms":slice_span_ms,
                                                    "from_n_intervals_ago": n_intervals_ago}
        )
        return SliceStatusReason.HISTORICAL_SPARSITY

            
    return SliceStatusReason.AWAITING_UPSTREAM

def _partial_slice_logging(inspection: InspectionResult, limit: int):
    try:

        log.debug(
            f"Partial slice received Limit={limit} n_items={inspection.n_items}. (Note: This may be expected)"
        )

        rec_first =inspection.received_first_open
        rec_last =inspection.received_last_open
        req_first =inspection.requested_first_open
        req_last =inspection.requested_last_open_implicit  

        if any(v is None for v in (
                                  )):
            log.debug(
                colorize(
                    "Partial slice diagnostics incomplete "
                    "(missing requested/received bounds in InspectionResult object).",
                    Ansi.BOLD,
                )
            )

        if inspection.received_first_open != inspection.requested_first_open:
            log.debug(
                "first open requested != first open recived"  
            )

        if inspection.received_last_open != inspection.requested_last_open_implicit:
            log.debug(
                "last open requested (implicit) != last open recived"  
            )
        
        log.debug(
            "Slice Requested: "
            f"{format_ts_human(req_first if req_first is not None else 'KeyError')} - "
            f"{format_ts_human(req_last  if req_last  is not None else 'KeyError')}"
        )

        log.debug(
            "Slice Received:  "
            f"{format_ts_human(rec_first if rec_first is not None else 'KeyError')} - "
            f"{format_ts_human(rec_last  if rec_last  is not None else 'KeyError')}"
        )

        if inspection.integrity.has_gaps: #type: ignore (this is only on the slow path, so we know it wont be none)
            log.debug(
                "Gaps in slice received"  
            )
        
        log.debug(inspection)

    except Exception as exc:
        log.exception(exc)
        pass



        

# log.debug(colorize(reason_msg, Ansi.BOLD))
# log.debug(f"Partial reason: {partial_reason}")

# log.debug(f"Request url was: {meta.get('url')}")


# log.debug("Time Delta (startTime param to endTime param)")
# log_requested_slice_size(meta.get("url"))  # type: ignore
