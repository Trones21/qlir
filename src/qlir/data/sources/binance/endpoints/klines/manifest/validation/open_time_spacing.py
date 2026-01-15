import logging
from typing import Any

from qlir.data.sources.binance.endpoints.klines.manifest.validation.slice_structure import (
    isolate_open_time_from_composite_key,
    isolate_open_time_from_request_url,
)
from qlir.data.sources.binance.intervals import interval_to_ms
from qlir.data.sources.common.slices.slice_status import SliceStatus

from .violations import ManifestViolation

log = logging.getLogger(__name__)

SliceParseViolations = dict[str, list[ManifestViolation]]
OpenSpacingViolations = dict[str, list[ManifestViolation]]

def validate_slice_open_spacing_wrapper(manifest) -> tuple[SliceParseViolations, OpenSpacingViolations]:
    # Determine the expeted gap
    limit = manifest['limit'] 
    interval_str = manifest['interval']
    interval = interval_to_ms(interval_str)
    expected_gap = interval * limit
    
    slices = manifest['slices']

    slice_parse_violations: dict[str, list[ManifestViolation]] = {}
    open_spacing_violations: dict[str, list[ManifestViolation]] = {}

    # get pairs (key, slice) - extract open time from composite key and also via requested_url
    pairs_bycompkey, composite_key_structure_violations = extract_open_pairs_by_composite_key_segment(slices)
    pairs_byrequrl, url_parse_violations = extract_open_pairs_by_url_starttime(slices)
    
    slice_parse_violations['composite_key_structure_violations'] = composite_key_structure_violations
    slice_parse_violations['url_structure_violations'] = url_parse_violations
    
    # Get the open to open spacing violations (again by both keys) 
    open_space_viols_bycompkey = validate_slice_open_spacing(pairs_bycompkey, expected_gap)
    req_url_space_violations = validate_slice_open_spacing(pairs_byrequrl, expected_gap)

    open_spacing_violations['via_composite_key_parse'] = open_space_viols_bycompkey
    open_spacing_violations['via_req_url_parse'] = req_url_space_violations

    return slice_parse_violations, open_spacing_violations


def validate_slice_open_spacing(
    pairs: list[tuple[int, Any]],
    expected_gap_ms: int,
) -> list[ManifestViolation]:
    
    violations: list[ManifestViolation] = []

    if not pairs:
        return violations

    pairs.sort(key=lambda x: x[0])

    for i in range(1, len(pairs)):
        prev_ts = pairs[i - 1][0]
        curr_ts = pairs[i][0]
        delta = curr_ts - prev_ts

        if delta != expected_gap_ms:
            violations.append(
                ManifestViolation(
                    rule="open_time_spacing",
                    slice_key=None,
                    message="Gap (of non-interval length) detected",
                    extra={
                    "index": i,
                    "expected_gap_ms": expected_gap_ms,
                    "actual_gap_ms": delta,
                    "prev_ts": prev_ts,
                    "curr_ts": curr_ts,
                    })
                )
            
            log.warning(
                f"Slice gap violation at index {i}: "
                f"expected {expected_gap_ms}ms, got {delta}ms "
                f"(prev={prev_ts}, curr={curr_ts})",
                extra={"tag": ("MANIFEST","VALIDATION","OPEN_TIME_SPACING")}
            )

    return violations


def extract_open_pairs_by_composite_key_segment(
    slices: dict[str, Any],
) -> tuple[list[tuple[int, Any]], list[ManifestViolation]]:
    
    pairs: list[tuple[int, Any]] = []
    violations: list[ManifestViolation] = []
    for composite_key, slice_entry in slices.items():
        res = isolate_open_time_from_composite_key(composite_key)
        if type(res) is ManifestViolation:
            violations.append(res)
        elif type(res) is int:
            pairs.append((res, slice_entry))
        else:
            log.debug(f"extract_open_pairs_by_composite_key_segment returned an unexpected value: {res}")

    return pairs, violations




def extract_open_pairs_by_url_starttime(
    slices: dict[str, Any],
) -> tuple[list[tuple[int, Any]], list[ManifestViolation]]:
    
    pairs: list[tuple[int, Any]] = []
    violations: list[ManifestViolation] = []
    for slice_key, slice_entry in slices.items():
        if SliceStatus.try_parse(slice_entry['slice_status']) == SliceStatus.MISSING.value:
            # We cannot parse the requested url if we havent made the request yet
            continue

        res = isolate_open_time_from_request_url(
            slice_key=slice_key,
            slice_entry=slice_entry,
        )
        if type(res) is ManifestViolation:
            violations.append(res)
        elif type(res) is int:
            pairs.append((res, slice_entry))
        else:
            log.debug(f"extract_open_pairs_by_url_starttime returned an unexpected value: {res}")

    return pairs, violations

