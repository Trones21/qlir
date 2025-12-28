import re
from typing import Any
from urllib.parse import parse_qs, urlparse
import logging

from qlir.data.sources.binance.endpoints.klines.manifest.validation.slice_structure import isolate_open_time_from_composite_key, isolate_open_time_from_request_url 
from .violations import ManifestViolation
log = logging.getLogger(__name__)

def validate_slice_open_spacing_wrapper(manifest):
    # Determine the expeted gap
    limit = manifest['limit'] 
    interval_str = manifest['interval']
    interval = _interval_to_ms(interval_str)
    expected_gap = interval * limit
    
    slices = manifest['slices']

    violations: dict = {}
    slice_structure_violations: dict[str, list[ManifestViolation]] = {}
    open_spacing_violations: dict[str, list[ManifestViolation]] = {}

    # get pairs (key, slice) - extract open time from composite key - and collect issues
    pairs_bycompkey, composite_key_structure_violations = extract_open_pairs_by_composite_key_segment(slices)
    slice_structure_violations['composite_key_structure_violations'] = composite_key_structure_violations
    
    open_space_viols_bycompkey = validate_slice_open_spacing(pairs_bycompkey, expected_gap)
    open_spacing_violations['via_composite_key_parse'] = open_space_viols_bycompkey

    # get pairs (key, slice) - extract open time from requested url - and collect issues
    pairs_byrequrl, url_parse_violations = extract_open_pairs_by_url_starttime(slices)
    slice_structure_violations['url_structure_violations'] = url_parse_violations
    
    req_url_space_violations = validate_slice_open_spacing(pairs_byrequrl, expected_gap)
    open_spacing_violations['via_req_url_parse'] = req_url_space_violations

    # Collect Violations 
    violations['open_spacing_violations'] = open_spacing_violations
    violations['slice_structure_violations'] = slice_structure_violations

    return violations


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
        if res is ManifestViolation:
            violations.append(res)
        elif res is int:
            pairs.append((res, slice_entry))
        else:
            log.debug("extract_open_pairs_by_composite_key_segment returned an unexpected value")

    return pairs, violations




def extract_open_pairs_by_url_starttime(
    slices: dict[str, Any],
) -> tuple[list[tuple[int, Any]], list[ManifestViolation]]:
    
    pairs: list[tuple[int, Any]] = []
    violations: list[ManifestViolation] = []
    for slice_key, slice_entry in slices.items():
        res = isolate_open_time_from_request_url(
            slice_key=slice_key,
            slice_entry=slice_entry,
        )
        if res is ManifestViolation:
            violations.append(res)
        elif res is int:
            pairs.append((res, slice_entry))
        else:
            log.debug("extract_open_pairs_by_url_starttime returned an unexpected value")

    return pairs, violations



_INTERVAL_RE = re.compile(r"^(?P<value>\d+)(?P<unit>[sm])$")

def _interval_to_ms(interval: str) -> int:
    """
    Convert an interval string to milliseconds.

    Supported formats:
      - <int>s  (seconds)
      - <int>m  (minutes)

    Examples:
      - "5s" -> 5000
      - "2m" -> 120000

    Raises:
        ValueError: If the interval format is invalid.
    """
    if not isinstance(interval, str):
        raise TypeError(f"interval must be str, got {type(interval).__name__}")

    match = _INTERVAL_RE.match(interval.strip())
    if not match:
        raise ValueError(
            f"Invalid interval format: {interval!r} "
            "(expected '<int>s' or '<int>m')"
        )

    value = int(match.group("value"))
    unit = match.group("unit")

    if unit == "s":
        return value * 1_000
    if unit == "m":
        return value * 60_000

    # defensive: regex already constrains this
    raise ValueError(f"Unsupported interval unit: {unit}")
