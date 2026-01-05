from dataclasses import dataclass
from typing import Sequence

import pandas as _pd


@dataclass
class FillContext:
    left: _pd.Series
    right: _pd.Series
    timestamps: _pd.DatetimeIndex
    interval_s: int

    # context (real candles only)
    left_window: Sequence[_pd.Series]
    right_window: Sequence[_pd.Series]


class FillPolicy:
    name: str

    def generate(self, ctx: FillContext) -> _pd.DataFrame:
        raise NotImplementedError
