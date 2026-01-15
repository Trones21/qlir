# qlir/data/sources/base_symbol_map.py

from __future__ import annotations

from abc import ABC
from typing import Any, Dict

from qlir.data.core.instruments import CanonicalInstrument


class BaseSymbolMap(ABC):
    """
    Shared implementation for venue symbol maps, with override-friendly methods.

    Subclasses MUST define:
        FORWARD_MAP: Dict[CanonicalInstrument, Any]

    They MAY override:
        to_venue(), to_canonical(), all_canonical(), all_venue()
    """

    FORWARD_MAP: Dict[CanonicalInstrument, Any] = {}

    def __new__(cls, *args, **kwargs):
        """Prevent instantiation (python doesnt have true static classes)"""
        raise TypeError(f"{cls.__name__} may not be instantiated")

    # ---- Required for reverse map construction ------------------------------

    @classmethod
    def reverse_map(cls) -> Dict[Any, CanonicalInstrument]:
        """Default reverse lookup implementation (can be overridden)."""
        return {v: k for k, v in cls.FORWARD_MAP.items()}

    # ---- Core mapping functions ---------------------------------------------

    @classmethod
    def to_venue(cls, instrument: CanonicalInstrument) -> Any:
        try:
            return cls.FORWARD_MAP[instrument]
        except KeyError:
            raise KeyError(
                f"{instrument} has no mapping in {cls.__name__}.FORWARD_MAP"
            )

    @classmethod
    def to_canonical(cls, venue_id: Any) -> CanonicalInstrument:
        reverse = cls.reverse_map()
        try:
            return reverse[venue_id]
        except KeyError:
            raise KeyError(
                f"Venue ID {venue_id!r} has no mapping in {cls.__name__}.FORWARD_MAP"
            )

    # ---- Convenience (also override-friendly) -------------------------------

    @classmethod
    def all_canonical(cls) -> list[CanonicalInstrument]:
        return list(cls.FORWARD_MAP.keys())

    @classmethod
    def all_venue(cls) -> list[Any]:
        return list(cls.FORWARD_MAP.values())
