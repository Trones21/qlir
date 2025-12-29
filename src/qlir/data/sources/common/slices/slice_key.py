from dataclasses import dataclass


@dataclass(frozen=True)
class SliceKey:
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

    def canonical_slice_composite_key(self) -> str:
        """
        Produce a canonical string that uniquely identifies the logical slice.

        Note:
        - end_ms is intentionally excluded so that rolling updates overwrite
        the same canonical slice on disk.
        """
        return f"{self.symbol}:{self.interval}:{self.start_ms}:{self.limit}"

    def request_slice_composite_key(self) -> str:
        '''Excluding limit'''
        return f"{self.symbol}:{self.interval}:{self.start_ms}-{self.end_ms}"


