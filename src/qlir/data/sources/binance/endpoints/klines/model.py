from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class SliceStatus(str, Enum):
    """
    Status of a single kline slice fetch.

    - PENDING: not yet fetched or scheduled
    - OK:      successfully fetched and stored
    - FAILED:  attempted but failed (eligible for retry)
    """
    PENDING = "pending"
    OK = "ok"
    FAILED = "failed"


@dataclass(frozen=True)
class KlineSliceKey:
    """
    Canonical identifier for a single /api/v3/klines slice.

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

    def composite_key(self) -> str:
        """
        Produce a canonical string that uniquely identifies this slice.

        Example:
            "BTCUSDT:1m:1609459200000-1609465199999:1000"
        """
        return f"{self.symbol}:{self.interval}:{self.start_ms}-{self.end_ms}:{self.limit}"
