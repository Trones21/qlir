from dataclasses import dataclass
from typing import List, Optional

import pandas as _pd

from qlir.data.quality.candles.models.candle_gap import CandleGap
from qlir.time.timefreq import TimeFreq


@dataclass
class CandlesDQReport:
    freq: TimeFreq
    n_rows: int
    n_dupes_dropped: int

    first_ts: _pd.Timestamp
    final_ts: _pd.Timestamp

    missing_starts: List[_pd.Timestamp]
    n_gaps: int
    gaps: list[CandleGap]
    gaps_df: _pd.DataFrame
    gap_sizes_dict: Optional[list[dict]]
    gap_sizes_df: _pd.DataFrame

    ohlc_zeros: Optional[_pd.DataFrame] = None
    n_ohlc_zeros: int = 0

    ohlc_inconsistencies: Optional[_pd.DataFrame] = None
    n_ohlc_inconsistencies: int = 0

    unrealistically_large_candles: Optional[_pd.DataFrame] = None
    n_unrealistically_large_candles: int = 0


