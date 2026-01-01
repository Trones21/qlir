# ---------------------------------------------------------------------------
# Configuration models
# ---------------------------------------------------------------------------

from dataclasses import dataclass
from enum import Enum

# ---------------------------------------------------------------------------
# Job Config Classes (specify all the data that the job needs to run)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class KlinesJobConfig:
    """
    Configuration for a single kline ingestion job.

    One job corresponds to a single (symbol, interval, limit) triplet.
    The worker is responsible for:
      - determining the time range
      - slicing it into KlineSliceKey windows
      - fetching missing slices
      - writing raw responses under the data root
    """
    symbol: str           # e.g. "BTCUSDT"
    interval: str         # e.g. "1s" or "1m"
    limit: int = 1000     # fixed for now in our design


@dataclass(frozen=True)
class UIKlinesJobConfig:
    """Only separate from klines job for clarity/readability reasons"""
    symbol: str           # e.g. "BTCUSDT"
    interval: str         # e.g. "1s" or "1m"
    limit: int = 1000     # fixed for now in our design

