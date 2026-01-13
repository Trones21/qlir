from dataclasses import dataclass
import time

from qlir.data.sources.binance.generate_urls import interval_to_ms


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



def slice_key_from_canonical(key: str) -> SliceKey:
    try:
        symbol, interval, start_ms, limit = key.split(":")
        interval_ms = interval_to_ms(interval)
        span_ms = interval_ms * int(limit)

        start_ms = int(start_ms)
        end_ms = start_ms + span_ms - 1

        return SliceKey(
            symbol=symbol,
            interval=interval,
            start_ms=start_ms,
            end_ms=end_ms,
            limit=int(limit),
        )
    except Exception as e:
        raise ValueError(f"Invalid canonical slice key: {key}") from e



def get_current_slice_key(prior_key: str) -> str:
    prior = slice_key_from_canonical(prior_key)

    interval_ms = interval_to_ms(prior.interval)
    span_ms = interval_ms * prior.limit
    now_ms = int(time.time() * 1000)

    delta = now_ms - prior.start_ms
    if delta < 0:
        raise RuntimeError("Clock precedes prior slice")

    offset = (delta // span_ms) * span_ms
    current_start = prior.start_ms + offset
    current_end = current_start + span_ms - 1

    current = SliceKey(
        symbol=prior.symbol,
        interval=prior.interval,
        start_ms=current_start,
        end_ms=current_end,
        limit=prior.limit,
    )

    return current.canonical_slice_composite_key()

