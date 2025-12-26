import re
from typing import Any
from urllib.parse import parse_qs, urlparse
import logging 
log = logging.getLogger(__name__)

def validate_slice_open_spacing_wrapper(manifest):
    # Determine the expeted gap
    limit = manifest['limit'] 
    interval_str = manifest['interval']
    interval = _interval_to_ms(interval_str)
    expected_gap = interval * limit
    
    slices = manifest['slices']

    # get pairs (key, slice) - extract open time from composite key
    pairs_bycompkey = extract_open_pairs_by_composite_key_segment(slices)
    validate_slice_open_spacing(pairs_bycompkey, expected_gap)

    # get pairs (key, slice) - extract open time from requested url 
    pairs_byrequrl = extract_open_pairs_by_url_starttime(slices)
    validate_slice_open_spacing(pairs_byrequrl, expected_gap)



def validate_slice_open_spacing(
    pairs: list[tuple[int, Any]],
    expected_gap_ms: int,
) -> None:
    if not pairs:
        return

    pairs.sort(key=lambda x: x[0])

    for i in range(1, len(pairs)):
        prev_ts = pairs[i - 1][0]
        curr_ts = pairs[i][0]
        delta = curr_ts - prev_ts

        if delta != expected_gap_ms:
            raise ValueError(
                f"Slice gap violation at index {i}: "
                f"expected {expected_gap_ms}ms, got {delta}ms "
                f"(prev={prev_ts}, curr={curr_ts})"
            )



def extract_open_pairs_by_composite_key_segment(
    slices: dict[str, Any],
) -> list[tuple[int, Any]]:
    pairs: list[tuple[int, Any]] = []

    for composite_key, slice_entry in slices.items():
        unix_ts, returned_slice = isolate_open_time_from_composite_key(
            composite_key,
            slice_entry,
        )
        pairs.append((unix_ts, returned_slice))

    return pairs



def extract_open_pairs_by_url_starttime(
    slices,
) -> list[tuple[int, Any]]:
    pairs: list[tuple[int, Any]] = []

    for slice in slices:
        unix_ts, slice_entry = isolate_open_time_from_request_url(slice)
        pairs.append((unix_ts, slice_entry))

    return pairs



def isolate_open_time_from_composite_key(
    composite_key: str,
    slice_entry: dict
) -> tuple[int, dict]:
    """
    Extract the open timestamp from a composite slice key.

    Expected format:
        SYMBOL:INTERVAL:START_TIME:LIMIT

    Example:
        BTCUSDT:1m:1503122400000:1000

    Returns:
        (open_ts_ms, slice_entry)
    """
    parts = composite_key.split(":")

    if len(parts) < 4:
        raise ValueError(
            f"Invalid composite key format: {composite_key}"
        )

    start_time_str = parts[2]

    try:
        open_ts_ms = int(start_time_str)
    except ValueError as exc:
        raise ValueError(
            f"Invalid startTime in composite key: {composite_key}"
        ) from exc

    return open_ts_ms, slice_entry


def isolate_open_time_from_request_url(
    slice_entry: dict,
) -> tuple[int, dict]:
    """
    Extract the open timestamp (startTime) from the slice's requested_url.

    Contract:
    - requested_url must exist
    - startTime query param must exist
    - startTime must be parseable as int (ms)

    Returns:
        (open_ts_ms, slice_entry)
    """
    try:
        url = slice_entry["requested_url"]
    except KeyError as exc:
        raise KeyError("Slice entry missing 'requested_url'") from exc

    parsed = urlparse(url)
    qs = parse_qs(parsed.query)

    try:
        start_time_str = qs["startTime"][0]
    except (KeyError, IndexError) as exc:
        raise ValueError(
            f"requested_url missing startTime param: {url}"
        ) from exc

    try:
        open_ts_ms = int(start_time_str)
    except ValueError as exc:
        raise ValueError(
            f"Invalid startTime value: {start_time_str}"
        ) from exc

    return open_ts_ms, slice_entry

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
