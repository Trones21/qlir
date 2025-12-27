from dataclasses import dataclass
from typing import List, Optional

import pandas as pd
from qlir.data.quality.candles.models.candle_gap import CandleGap
from qlir.time.timefreq import TimeFreq


@dataclass
class CandlesDQReport:
    freq: TimeFreq
    n_rows: int
    n_dupes_dropped: int

    first_ts: pd.Timestamp
    final_ts: pd.Timestamp

    missing_starts: List[pd.Timestamp]
    n_gaps: int
    gaps: list[CandleGap]
    gaps_df: pd.DataFrame
    gap_sizes_dict: Optional[list[dict]]
    gap_sizes_df: pd.DataFrame

    ohlc_zeros: Optional[pd.DataFrame] = None
    n_ohlc_zeros: int = 0

    ohlc_inconsistencies: Optional[pd.DataFrame] = None
    n_ohlc_inconsistencies: int = 0

    unrealistically_large_candles: Optional[pd.DataFrame] = None
    n_unrealistically_large_candles: int = 0


