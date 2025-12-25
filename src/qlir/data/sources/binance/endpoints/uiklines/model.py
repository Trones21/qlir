from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class SliceStatus(str, Enum):
    """
    Status of a single uikline slice fetch.

    - PENDING: not yet fetched or scheduled
    - OK:      successfully fetched and stored
    - FAILED:  attempted but failed (eligible for retry)
    - IN_PROGRESS: Some thread is currently fetching 
    """
    PENDING = "pending"
    OK = "ok"
    FAILED = "failed"
    IN_PROGRESS= "in_progress"


@dataclass(frozen=True)
class UIKlineSliceKey:
    """
    Canonical identifier for a single /api/v3/uiklines slice.

    Each slice maps 1:1 to:
      - a specific Binance URL
      - a single raw response file
      - a manifest entry keyed by composite_key()

    Attributes:
        symbol:   Trading pair, e.g. "BTCUSDT".
        interval: Interval string, e.g. "1s" or "1m".
        start_ms: Inclusive start timestamp in milliseconds since epoch.
        end_ms:   Inclusive end timestamp in milliseconds since epoch.
        limit:    Max candles per request (1000 in this module).
    """
    symbol: str
    interval: str
    start_ms: int
    end_ms: int
    limit: int = 1000

    def canonical_slice_composite_key(self) -> str:
        """
        Produce a canonical string that uniquely identifies the logical slice.

        Note:
        - end_ms is intentionally excluded so that rolling updates overwrite
        the same canonical slice on disk.
        """
        return f"{self.symbol}:{self.interval}:{self.start_ms}:{self.limit}"

    def request_slice_composite_key(self) -> str:
        return f"{self.symbol}:{self.interval}:{self.start_ms}-{self.end_ms}:{self.limit}"