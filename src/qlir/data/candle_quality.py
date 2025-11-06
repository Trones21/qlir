# qlir/io/candles_dq.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, List
import pandas as pd

@dataclass
class CandlesDQReport:
    freq: Optional[str]
    n_rows: int
    n_dupes_dropped: int
    n_gaps: int
    missing_starts: List[pd.Timestamp]

DRIFT_TOKEN_TO_FREQ = {
    "1": "T", "5": "5T", "15": "15T", "60": "H", "240": "4H",
    "D": "D", "W": "W", "M": "MS",
}

def _ensure_utc(s: pd.Series) -> pd.Series:
    out = pd.to_datetime(s, utc=True, errors="coerce")
    if out.isna().any():
        raise ValueError("Invalid timestamps in tz_start/tz_end.")
    return out

def _sort_dedupe(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    before = len(df)
    out = df.copy()
    out["tz_start"] = _ensure_utc(out["tz_start"])
    out = out.sort_values("tz_start")
    out = out.drop_duplicates(subset=["tz_start"], keep="last").reset_index(drop=True)
    return out, before - len(out)



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


def infer_freq(df: pd.DataFrame) -> Optional[str]:
    """
    Infer the frequency of a time-indexed candle DataFrame.

    - Uses the first two unique timestamps to estimate delta-based frequency.
    - If there are < 2 unique timestamps, returns None.
    - Returns a pandas frequency string (e.g., '5T', 'H', '4H') if convertible,
      otherwise None.
    """
    if df.empty or "tz_start" not in df.columns:
        return None

    s = pd.to_datetime(df["tz_start"], utc=True).sort_values().drop_duplicates()
    if len(s) < 2:
        return None

    # use iloc[1] - iloc[0] for a simple delta
    delta = s.iloc[1] - s.iloc[0]

    try:
        return pd.tseries.frequencies.to_offset(delta).freqstr # type: ignore
    except ValueError:
        # happens if delta can't be converted to a standard pandas frequency
        return None


def detect_candle_gaps(df: pd.DataFrame, freq: Optional[str] = None) -> list[pd.Timestamp]:
    """
    Return missing tz_start timestamps given a fixed frequency.
    Does NOT fill or infer freq — pass freq explicitly.
    """
    if df.empty or freq is None or len(df) < 2:
        return []

    req = {"tz_start", "open", "high", "low", "close"}
    miss = req - set(df.columns)
    if miss:
        raise ValueError(f"Missing required columns: {sorted(miss)}")

    fixed = _sort_dedupe(df)[0]
    s = pd.to_datetime(fixed["tz_start"], utc=True)
    expected = pd.date_range(s.iloc[0], s.iloc[-1], freq=freq, inclusive="both", tz="UTC")
    missing = expected.difference(s)
    return list(missing)


def validate_candles(df: pd.DataFrame) -> tuple[pd.DataFrame, CandlesDQReport]:
    fixed, deduped = _sort_dedupe(df)
    freq = infer_freq(fixed)
    missing, freq = detect_candle_gaps(fixed)
    report = CandlesDQReport(
        freq=freq, n_rows=len(fixed), n_dupes_dropped=deduped,
        n_gaps=len(missing), missing_starts=missing
    )
    return fixed, report
