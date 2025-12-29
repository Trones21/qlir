from __future__ import annotations
from enum import Enum
from typing import Optional
import logging
log = logging.getLogger(__name__)


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
    def is_valid(cls, raw) -> SliceStatus:
        try:
            return cls(raw)
        except ValueError as exc:
            raise ValueError(
                f"Invalid SliceStatus literal: {raw!r}"
            ) from exc


