# -------------------------------------------------------------------
#  Time frequency representation
# -------------------------------------------------------------------
from dataclasses import dataclass
from enum import Enum
from typing import Optional

import pandas as pd

from qlir.data.core.naming_constants import CANONICAL_RESOLUTION_UNIT_MAP, REVERSE_CANONICAL_RESOLUTION_UNIT_MAP


class TimeUnit(Enum):
    SECOND = "second"
    MINUTE = "minute"
    HOUR = "hour"
    DAY = "day"

    @property
    def pandas_symbol(self) -> str:
        """Return the pandas frequency symbol for this time unit."""
        return {
            TimeUnit.SECOND: "sec",
            TimeUnit.MINUTE: "min",
            TimeUnit.HOUR: "h",
            TimeUnit.DAY: "D",
        }[self]


@dataclass
class TimeFreq:
    count: int
    unit: TimeUnit               # 'second' | 'minute' | 'hour' | 'day'
    pandas_offset: Optional[pd.tseries.offsets.BaseOffset] = None


    @property
    def as_pandas_str(self) -> str:
        """Convert to pandas frequency string (e.g., '1sec', '5min', '1H', '1D')"""
        symbol = {
            "second": "sec",
            "minute": "min",
            "hour": "h",
            "day": "D",
        }.get(self.unit.value)
        return f"{self.count}{symbol}"

    def __str__(self) -> str:
        return f"count: {self.count} unit: {self.unit.value} pandas_offset: {self.pandas_offset}"

    def to_dict(self, include_offset: bool = False) -> dict:
        """Convert to a dictionary representation.

        Parameters
        ----------
        include_offset : bool
            Whether to include `pandas_offset` (which may not be JSON-serializable).
        """
        d = {
            "count": self.count,
            "unit": self.unit.value,
            "as_pandas_str": self.as_pandas_str,
        }
        if include_offset and self.pandas_offset is not None:
            d["pandas_offset"] = str(self.pandas_offset)
        return d


    def to_canonical_resolution_str(self) -> str:
        """
        Return the canonical frequency representation used in filenames, 
        network requests, and cache keys (e.g. '1m', '5m', '1h', '1D').
        """
        symbol = CANONICAL_RESOLUTION_UNIT_MAP[self.unit]
        return f"{self.count}{symbol}"


    
    @staticmethod
    def from_canonical_resolution_str(s: str) -> "TimeFreq":
        """
        Parse canonical resolution strings ('1m', '5m', '1h', '1D') into TimeFreq objects.
        """
        if not isinstance(s, str) or len(s) < 2:
            raise ValueError(f"Invalid canonical resolution string: {s}")

        # Identify numeric prefix
        i = 0
        while i < len(s) and s[i].isdigit():
            i += 1

        if i == 0:
            raise ValueError(f"Missing count in canonical resolution string: {s}")

        count = int(s[:i])
        unit_symbol = s[i:]

        if unit_symbol not in REVERSE_CANONICAL_RESOLUTION_UNIT_MAP:
            raise ValueError(f"Unknown unit '{unit_symbol}' in canonical resolution string: {s}")

        unit = REVERSE_CANONICAL_RESOLUTION_UNIT_MAP[unit_symbol]

        return TimeFreq(count=count, unit=unit)
