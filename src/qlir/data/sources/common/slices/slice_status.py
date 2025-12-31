from __future__ import annotations
from enum import StrEnum, auto
from typing import Any, Optional
import logging
log = logging.getLogger(__name__)


class SliceStatus(StrEnum):
    """
    Status of a single kline slice fetch.

    - MISSING:      not yet fetched or scheduled
    - COMPLETE:     Successfully fetched and stored full slice
    - PARTIAL:      Successfully fetched and stored a parital slice (e.g. when full 1000 candle slice isnt available yet)
    - NEEDS_REFRESH: Generic label, see clasiify slices and the __contract object to see the full reason 
    - FAILED:       attempted but failed (eligible for retry)
    - IN_PROGRESS:  A worker is currently fetching 
    """
    MISSING = auto()
    COMPLETE = auto()
    PARTIAL = auto()
    NEEDS_REFRESH = auto()
    FAILED = auto()
    IN_PROGRESS= auto()

    @classmethod
    def try_parse(cls, raw: Optional[str]) -> SliceStatus | None:
        if raw is None:
            return None
        try:
            return cls(raw)
        except ValueError:
            return None
    
    @classmethod
    def is_valid(cls, raw: Any) -> SliceStatus:
        if isinstance(raw, cls):
            return raw
        if isinstance(raw, str):
            try:
                return cls(raw)
            except ValueError as exc:
                raise ValueError(f"Invalid SliceStatus literal: {raw!r}") from exc
        raise ValueError(f"Invalid SliceStatus type: {type(raw).__name__}")


