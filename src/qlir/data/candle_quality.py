from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, List
import pandas as pd
import logging

log = logging.getLogger(__name__)

OHLCV_COLS = ["open", "high", "low", "close", "volume"]


# -------------------------------------------------------------------
#  Time frequency representation
# -------------------------------------------------------------------
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
            "hour": "H",
            "day": "D",
        }.get(self.unit)
        return f"{self.count}{symbol}"

    def __str__(self) -> str:
        return f"{self.count} {self.unit}{'s' if self.count != 1 else ''}"


# -------------------------------------------------------------------
#  UTC normalization
# -------------------------------------------------------------------
def _ensure_utc(s: pd.Series) -> pd.Series:
    out = pd.to_datetime(s, utc=True, errors="coerce", format="%Y-%m-%d %H:%M:%S")
    if out.isna().any():
        raise ValueError("Invalid timestamps in tz_start/tz_end.")
    return out


# -------------------------------------------------------------------
#  Sorting & deduplication
# -------------------------------------------------------------------
def _sort_dedupe(df: pd.DataFrame, time_col: str = "tz_start") -> tuple[pd.DataFrame, int]:
    before = len(df)
    if time_col not in df.columns:
        raise ValueError(f"Missing required time column '{time_col}'")

    out = df.copy()
    out[time_col] = _ensure_utc(out[time_col])
    out = out.sort_values(time_col)

    dup_mask = out.duplicated(subset=[time_col], keep=False)
    if dup_mask.any():
        dup_df = out[dup_mask]
        to_drop_idx = []

        for ts, group in dup_df.groupby(time_col, sort=False):
            present_cols = [c for c in OHLCV_COLS if c in group.columns]
            group_no_exact = group.drop_duplicates()

            if len(group_no_exact) == 1:
                drop_these = [i for i in group.index if i != group_no_exact.index[0]]
                to_drop_idx.extend(drop_these)
            else:
                if not present_cols:
                    raise ValueError(f"Conflicting duplicates for {time_col}={ts}")

                base = group_no_exact.iloc[0][present_cols]
                if not group_no_exact[present_cols].eq(base).all(axis=None):
                    log.error(
                        "Conflicting duplicate rows for %s=%s: %s",
                        time_col, ts, group_no_exact.to_dict(orient="records")
                    )
                    raise ValueError(f"Conflicting duplicates for {time_col}={ts}")

                drop_these = [i for i in group.index if i != group_no_exact.index[0]]
                to_drop_idx.extend(drop_these)

        out = out.drop(index=to_drop_idx)

    out = out.reset_index(drop=True)
    return out, before - len(out)


# -------------------------------------------------------------------
#  Frequency inference (returns structured object)
# -------------------------------------------------------------------
def infer_freq(df: pd.DataFrame) -> Optional[TimeFreq]:
    if df.empty or "tz_start" not in df.columns:
        return None

    s = pd.to_datetime(df["tz_start"], utc=True).sort_values().drop_duplicates()
    if len(s) < 2:
        return None

    delta = s.iloc[1] - s.iloc[0]
    seconds = delta.total_seconds()

    # detect major time units
    if seconds % 86400 == 0:
        return TimeFreq(count=int(seconds // 86400), unit="day")
    elif seconds % 3600 == 0:
        return TimeFreq(count=int(seconds // 3600), unit="hour")
    elif seconds % 60 == 0:
        return TimeFreq(count=int(seconds // 60), unit="minute")
    elif seconds >= 1:
        return TimeFreq(count=int(seconds), unit="second")
    else:
        try:
            off = pd.tseries.frequencies.to_offset(delta)
            return TimeFreq(count=off.n, unit=off.name, pandas_offset=off)
        except ValueError:
            return None


# -------------------------------------------------------------------
#  Candle gap detection
# -------------------------------------------------------------------
def detect_candle_gaps(df: pd.DataFrame, freq: Optional[TimeFreq] = None) -> list[pd.Timestamp]:
    if df.empty or freq is None or len(df) < 2:
        return []

    req = {"tz_start", "open", "high", "low", "close"}
    miss = req - set(df.columns)
    if miss:
        raise ValueError(f"Missing required columns: {sorted(miss)}")

    fixed, _ = _sort_dedupe(df)
    s = pd.to_datetime(fixed["tz_start"], utc=True)

    expected = pd.date_range(
        s.iloc[0],
        s.iloc[-1],
        freq=freq.as_pandas_str,
        inclusive="both",
        tz="UTC",
    )
    missing = expected.difference(s)
    return list(missing)


# -------------------------------------------------------------------
#  Candle validation
# -------------------------------------------------------------------
@dataclass
class CandlesDQReport:
    freq: TimeFreq
    n_rows: int
    n_dupes_dropped: int
    n_gaps: int
    missing_starts: List[pd.Timestamp]


def validate_candles(
    df: pd.DataFrame,
    freq: TimeFreq,
) -> tuple[pd.DataFrame, CandlesDQReport]:
    """
    Validate candles for a *known* frequency.

    - sort + dedupe (strict OHLCV match on dupes)
    - detect gaps using the provided `freq`

    We do **not** infer frequency here (that could return unintended results since infer freq is a simple df[1]["tz_start"] - df[0]["tz_start"])
    """
    fixed, deduped = _sort_dedupe(df)
    missing = detect_candle_gaps(fixed, freq=freq)
    report = CandlesDQReport(
        freq=freq,
        n_rows=len(fixed),
        n_dupes_dropped=deduped,
        n_gaps=len(missing),
        missing_starts=missing,
    )
    return fixed, report


def ensure_homogeneous_timeframe(df: pd.DataFrame) -> None:
    """
    Raises ValueError if the DataFrame contains inconsistent candle spacing.

    This version is strict — *all* deltas between consecutive tz_start
    timestamps must be exactly equal. Any deviation is considered invalid.
    """
    if df.empty:
        return

    if "tz_start" not in df.columns:
        raise ValueError("Missing tz_start column")

    s = pd.to_datetime(df["tz_start"], utc=True).sort_values().drop_duplicates()
    if len(s) < 3:
        return

    deltas = s.diff().dropna()

    # check if all deltas are identical
    if not deltas.eq(deltas.iloc[0]).all():
        raise ValueError(
            f"Heterogeneous candle spacing detected: {len(deltas.unique())} unique deltas found."
        )
