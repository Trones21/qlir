import pandas as _pd
from typing import Dict

def generate_offset_candles(
    df: _pd.DataFrame,
    *,
    period: int,
    unit: str = "minute",
    dt_col: str = "timestamp",
) -> Dict[str, _pd.DataFrame]:
    """
    From base-frequency data (usually 1m), generate all phase-shifted
    versions of a single period, e.g. all 7-minute alignments.

    Returns dict like:
      {
        "7min@0": df0,
        "7min@1": df1,
        ...
        "7min@6": df6,
      }
    """
    if df.index.name != dt_col:
        df = df.set_index(_pd.DatetimeIndex(df[dt_col], name=dt_col))
    df = df.sort_index()

    # standard OHLCV
    ohlc_map = {
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "sum",
    }

    # turn (period, unit) into pandas freq
    if unit == "minute":
        freq_str = f"{period}min"
        step = _pd.Timedelta(minutes=1)
    elif unit == "second":
        freq_str = f"{period}S"
        step = _pd.Timedelta(seconds=1)
    else:
        raise ValueError("for offsets we usually want minute/second base")

    out: Dict[str, _pd.DataFrame] = {}

    for offset in range(period):
        shifted = df.copy()
        shifted.index = shifted.index + step * offset
        candles = shifted.resample(freq_str).agg(ohlc_map).dropna(how="any")
        candles["candle_freq"] = freq_str
        candles["offset"] = offset
        key = f"{freq_str}@{offset}"
        out[key] = candles

    return out
