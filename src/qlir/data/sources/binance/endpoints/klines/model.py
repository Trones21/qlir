from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import logging
from typing import Optional
log = logging.getLogger(__name__)


# The meta fields we expect fetch_and_persist_slice to 
# we dont want a class because we dont want to break the system when changes are made 
# We just want to add an annotation using apply_contract() - basically this tells us that the object is usable, but it was produced under an older contract.
# The system can then decided how to handle this 
REQUIRED_FIELDS = [
    "slice_id",
    "relative_path",
    "status",
    "http_status",
    "n_items",
    "first_ts",
    "last_ts",
    "requested_url",
    "requested_at",
    "completed_at",
]


class SliceStatus(str, Enum):
    """
    Status of a single kline slice fetch.

    - MISSING:      not yet fetched or scheduled
    - COMPLETE:     Successfully fetched and stored full slice
    - PARTIAL:      Successfully fetched and stored a parital slice (e.g. when full 1000 candle slice isnt available yet)
    - NEEDS_REFRESH: Generic label, see clasiify slices and the __contract object to see the full reason 
    - FAILED:       attempted but failed (eligible for retry)
    - IN_PROGRESS:  A worker is currently fetching 
    """
    MISSING = "missing"
    COMPLETE = "complete"
    PARTIAL = "partial"
    NEEDS_REFRESH = "needs_refresh"
    FAILED = "failed"
    IN_PROGRESS= "in_progress"

    @classmethod
    def try_parse(cls, raw: Optional[str]) -> SliceStatus | None:
        if raw is None:
            return None
        try:
            return cls(raw)
        except ValueError:
            return None
    
    @classmethod
    def is_valid(cls, raw) -> "SliceStatus":
        try:
            return cls(raw)
        except ValueError as exc:
            raise ValueError(
                f"Invalid SliceStatus literal: {raw!r}"
            ) from exc

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


@dataclass
class SliceClassification:
    missing: list[KlineSliceKey]
    partial: list[KlineSliceKey]
    needs_refresh: list[KlineSliceKey]
    complete: list[KlineSliceKey]
    failed: list[KlineSliceKey]


def classify_slices(
    expected: list[KlineSliceKey],
    manifest: dict,
) -> SliceClassification:

    slices = manifest.get("slices", {})
    result = SliceClassification([], [], [], [], [])

    for slice_key in expected:
        key = slice_key.canonical_slice_composite_key()
        entry = slices.get(key)

        if not entry:
            result.missing.append(slice_key)
            continue
        
        status = SliceStatus(entry.get("status"))
        if status == SliceStatus.MISSING:
            result.missing.append(slice_key)
            continue

        # worker.add_or_update_entry_meta_contract adds these fields
        has_contract = "__meta_contract" in entry
        if not has_contract:
            log.debug(f"Metadata contract object in manifest for slice {key} is out of date, couldnt find __meta_contract field")
        
        has_status = has_contract and "status" in entry["__meta_contract"]
        if not has_status:
            log.debug(f"Metadata contract object in manifest for slice {key} is out of date, couldnt find __meta_contract.status field")
            
        is_out_of_sync = (
            not has_contract or
            not has_status or
            entry["__meta_contract"]["status"] == "out_of_sync"
        )
        if is_out_of_sync:
            log.debug(f"Metadata contract in manifest for slice {key} is out of date, setting SliceStatus to NEEDS_REFRESH")
            result.needs_refresh.append(slice_key)
            continue

        if status == SliceStatus.PARTIAL:
            result.partial.append(slice_key)
        elif status == SliceStatus.COMPLETE:
            result.complete.append(slice_key)
        elif status == SliceStatus.FAILED:
            result.failed.append(slice_key)

    log.info(
        {"Slice Classification Summary": 
            {
            k: len(v)
            for k, v in vars(result).items()
            }
        }
    )
    
    return result
