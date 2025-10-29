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

def infer_freq(df: pd.DataFrame, token: Optional[str] = None) -> Optional[str]:
    s = pd.to_datetime(df["tz_start"], utc=True).sort_values().drop_duplicates()
    if len(s) < 3:
        return DRIFT_TOKEN_TO_FREQ.get(str(token)) if token else None
    freq = pd.infer_freq(s)
    if freq is None and token:
        freq = DRIFT_TOKEN_TO_FREQ.get(str(token))
    return freq

def detect_candle_gaps(df: pd.DataFrame, token: Optional[str] = None) -> tuple[List[pd.Timestamp], Optional[str]]:
    """
    Returns (missing_tz_starts, freq). Does NOT fill.
    """
    if df.empty:
        return [], DRIFT_TOKEN_TO_FREQ.get(str(token)) if token else None

    # sanity
    req = {"tz_start", "open", "high", "low", "close"}
    miss = req - set(df.columns)
    if miss:
        raise ValueError(f"Missing required columns: {sorted(miss)}")

    fixed, deduped = _sort_dedupe(df)
    f = infer_freq(fixed, token=token)
    if f is None or len(fixed) < 2:
        return [], f

    s = pd.to_datetime(fixed["tz_start"], utc=True)
    exp = pd.date_range(s.iloc[0], s.iloc[-1], freq=f, inclusive="both", tz="UTC")
    missing = exp.difference(pd.DatetimeIndex(s))
    return list(missing), f

def validate_candles(df: pd.DataFrame, token: Optional[str] = None) -> tuple[pd.DataFrame, CandlesDQReport]:
    fixed, deduped = _sort_dedupe(df)
    freq = infer_freq(fixed, token=token)
    missing, freq = detect_candle_gaps(fixed, token=token)
    report = CandlesDQReport(
        freq=freq, n_rows=len(fixed), n_dupes_dropped=deduped,
        n_gaps=len(missing), missing_starts=missing
    )
    return fixed, report
