# -------------------------------------------------------------------
#  Time frequency representation
# -------------------------------------------------------------------
from dataclasses import dataclass
from typing import Optional

import pandas as pd


@dataclass
class TimeFreq:
    count: int
    unit: str               # 'second' | 'minute' | 'hour' | 'day'
    pandas_offset: Optional[pd.tseries.offsets.BaseOffset] = None


    @property
    def as_pandas_str(self) -> str:
        """Convert to pandas frequency string (e.g., '1sec', '5min', '1H', '1D')"""
        symbol = {
            "second": "sec",
            "minute": "min",
            "hour": "h",
            "day": "D",
        }.get(self.unit)
        return f"{self.count}{symbol}"

    def __str__(self) -> str:
        return f"count: {self.count} unit: {self.unit} pandas_offset: {self.pandas_offset}"

    def to_dict(self, include_offset: bool = False) -> dict:
        """Convert to a dictionary representation.

        Parameters
        ----------
        include_offset : bool
            Whether to include `pandas_offset` (which may not be JSON-serializable).
        """
        d = {
            "count": self.count,
            "unit": self.unit,
            "as_pandas_str": self.as_pandas_str,
        }
        if include_offset and self.pandas_offset is not None:
            d["pandas_offset"] = str(self.pandas_offset)
        return d
