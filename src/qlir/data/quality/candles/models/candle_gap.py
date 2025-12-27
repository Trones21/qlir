from dataclasses import dataclass
import pandas as pd

from qlir.time.timefreq import TimeFreq


@dataclass(frozen=True)
class CandleGap:
    start: pd.Timestamp
    end: pd.Timestamp
    missing_count: int


def detect_contiguous_gaps(
    missing: list[pd.Timestamp],
    freq: TimeFreq,
) -> list[CandleGap]:
    """
    Group missing candle timestamps into contiguous gaps.

    Input:
      - missing: output of detect_missing_candles
      - freq: candle frequency

    Output:
      - list of CandleGap objects
    """
    if not missing:
        return []

    idx = pd.DatetimeIndex(sorted(missing))
    step = pd.Timedelta(freq.as_pandas_str)

    gaps: list[CandleGap] = []

    gap_start = idx[0]
    prev = idx[0]
    count = 1

    for ts in idx[1:]:
        if ts - prev == step:
            count += 1
        else:
            gaps.append(CandleGap(gap_start, prev, count))
            gap_start = ts
            count = 1
        prev = ts

    gaps.append(CandleGap(gap_start, prev, count))
    return gaps

def candle_gaps_to_df(
    gaps: list[CandleGap],
) -> pd.DataFrame:
    """
    Convert CandleGap objects into a DataFrame.

    Columns:
      - gap_start
      - gap_end
      - missing_count
    """
    if not gaps:
        return pd.DataFrame(
            columns=["gap_start", "gap_end", "missing_count"]
        )

    return pd.DataFrame(
        {
            "gap_start": [g.start for g in gaps],
            "gap_end": [g.end for g in gaps],
            "missing_count": [g.missing_count for g in gaps],
        }
    )
