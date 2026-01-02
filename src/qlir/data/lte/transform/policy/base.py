from dataclasses import dataclass
from typing import Sequence

import pandas as pd


@dataclass
class FillContext:
    left: pd.Series
    right: pd.Series
    timestamps: pd.DatetimeIndex
    interval_s: int

    # volatility context (real candles only)
    left_window: Sequence[pd.Series]
    right_window: Sequence[pd.Series]


class FillPolicy:
    name: str

    def generate(self, ctx: FillContext) -> pd.DataFrame:
        raise NotImplementedError
