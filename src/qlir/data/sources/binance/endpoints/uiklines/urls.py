from __future__ import annotations

from typing import Generator, Tuple, Optional
import logging 
log = logging.getLogger(__name__)
BASE_URL = "https://api.binance.com/api/v3/uiKlines"


def interval_to_ms(interval: str) -> int:
    if interval == "1s":
        return 1_000
    if interval == "1m":
        return 60_000
    raise ValueError(f"Unsupported interval (only '1s' and '1m' allowed): {interval!r}")


def generate_UIKline_slices(
    symbol: str,
    interval: str,
    start_ms: int,
    end_ms: int,
    limit: int = 1000,
) -> Generator[Tuple[int, int], None, None]:
    if start_ms > end_ms:
        log.critical("generate_UIKline_slices received a start_ms > end_ms, this is not a valid range, please ensure start_ms is less than end_ms")
        raise ValueError("generate_UIKline_slices received a start_ms > end_ms, this is not a valid range, please ensure start_ms is less than end_ms")

    interval_ms = interval_to_ms(interval)
    span = interval_ms * limit

    current_start = start_ms
    while current_start <= end_ms:
        slice_end = current_start + span - 1
        if slice_end > end_ms:
            slice_end = end_ms
        yield current_start, slice_end
        current_start += span


def build_UIKline_url(
    symbol: str,
    interval: str,
    start_ms: Optional[int] = None,
    end_ms: Optional[int] = None,
    limit: int = 1000,
) -> str:
    """
    Construct a /api/v3/UIKlines URL for the given parameters.

    If start_ms or end_ms is None, the corresponding query parameter is omitted.
    """
    params = [
        f"symbol={symbol}",
        f"interval={interval}",
        f"limit={limit}",
    ]
    if start_ms is not None:
        params.append(f"startTime={start_ms}")
    if end_ms is not None:
        params.append(f"endTime={end_ms}")

    return f"{BASE_URL}?{'&'.join(params)}"
