"""
Slice-window generation for IBKR historical bars.

Identical window math to the Binance slicer (span = interval_ms * limit), so the shared
SliceKey/manifest/claims machinery behaves the same. The only difference downstream is how
a window is *fetched*: Binance uses startTime+limit over REST; IBKR uses endDateTime+duration
over the socket API (see request_spec.py).
"""

from __future__ import annotations

import logging
from typing import Generator, Tuple

log = logging.getLogger(__name__)


def generate_bar_slices(
    interval_ms: int,
    start_ms: int,
    end_ms: int,
    limit: int = 1000,
) -> Generator[Tuple[int, int], None, None]:
    if start_ms > end_ms:
        raise ValueError(
            "generate_bar_slices received start_ms > end_ms; start must be <= end"
        )

    span = interval_ms * limit
    current_start = start_ms
    while current_start <= end_ms:
        slice_end = current_start + span - 1
        if slice_end > end_ms:
            slice_end = end_ms
        yield current_start, slice_end
        current_start += span
