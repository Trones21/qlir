from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional


class Aggregation(str, Enum):
    MIN = "min"
    MAX = "max"
    FIRST = "first"
    LAST = "last"
    SUM = "sum"
    MEDIAN = "median"
    COUNT_TRUE = "count_true"


@dataclass(frozen=True)
class MetricSpec:
    """
    Defines a single event-level output column derived from
    one input column using one aggregation operation.
    """
    col: str
    agg: Aggregation
    out: Optional[str] = None

    def resolve_out_name(self) -> str:
        if self.out:
            return self.out
        return f"{self.col}_{self.agg.value}"
