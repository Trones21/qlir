from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, List
import logging

import numpy as np
import pandas as pd

from qlir.time.ensure_utc import ensure_utc_series
from qlir.time.timefreq import TimeFreq, TimeUnit

log = logging.getLogger(__name__)

OHLCV_COLS = ["open", "high", "low", "close", "volume"]


# -------------------------------------------------------------------
#  Sorting & deduplication
# -------------------------------------------------------------------

def _sort_dedupe(
    df: pd.DataFrame,
    time_col: str = "tz_start",
) -> tuple[pd.DataFrame, int, Optional[pd.DataFrame]]:
    """
    Returns:
        out: de-duplicated df
        n_dropped: number of rows dropped
        conflicts: df of conflicting duplicates, or None if none
    """
    before = len(df)
    if time_col not in df.columns:
        raise ValueError(f"Missing required time column '{time_col}'")

    out = df.copy()
    out[time_col] = ensure_utc_series(out[time_col])
    out = out.sort_values(time_col)

    conflicts: list[pd.DataFrame] = []

    dup_mask = out.duplicated(subset=[time_col], keep=False)
    if dup_mask.any():
        dup_df = out[dup_mask]
        to_drop_idx: list[int] = []

        for ts, group in dup_df.groupby(time_col, sort=False):
            present_cols = [c for c in OHLCV_COLS if c in group.columns]
            group_no_exact = group.drop_duplicates()

            # Only one unique row → safe: drop exact dupes, keep one
            if len(group_no_exact) == 1:
                drop_these = [i for i in group.index if i != group_no_exact.index[0]]
                to_drop_idx.extend(drop_these)
                continue

            # Multiple non-identical rows at same timestamp → potential conflict
            if not present_cols:
                # Nothing to compare on – definitely a conflict
                conflicts.append(
                    group_no_exact.assign(
                        _conflict_time=ts,
                        _conflict_reason="no_present_ohlcv_cols",
                    )
                )
                continue

            base = group_no_exact.iloc[0][present_cols]
            same_as_base = group_no_exact[present_cols].eq(base).all(axis=1)

            if same_as_base.all():
                # All OHLCV columns match base → safe, keep first, drop rest
                keep_idx = group_no_exact.index[0]
                drop_these = [i for i in group.index if i != keep_idx]
                to_drop_idx.extend(drop_these)
            else:
                # True content conflict on OHLCV columns
                conflicts.append(
                    group_no_exact.assign(
                        _conflict_time=ts,
                        _conflict_reason="ohlcv_mismatch",
                    )
                )

        # Drop all safe duplicates
        if to_drop_idx:
            out = out.drop(index=to_drop_idx)

    out = out.reset_index(drop=True)
    n_dropped = before - len(out)

    conflicts_df: Optional[pd.DataFrame]
    if conflicts:
        conflicts_df = pd.concat(conflicts, ignore_index=True)
        # You can also log summary here if you want
        log.error(
            "Found %d conflicting duplicate groups on '%s' (total %d rows)",
            conflicts_df["_conflict_time"].nunique(),
            time_col,
            len(conflicts_df),
        )
    else:
        conflicts_df = None

    return out, n_dropped, conflicts_df


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

    if seconds % 86400 == 0:
        return TimeFreq(count=int(seconds // 86400), unit=TimeUnit.DAY)
    elif seconds % 3600 == 0:
        return TimeFreq(count=int(seconds // 3600), unit=TimeUnit.HOUR)
    elif seconds % 60 == 0:
        return TimeFreq(count=int(seconds // 60), unit=TimeUnit.MINUTE)
    elif seconds >= 1:
        return TimeFreq(count=int(seconds), unit=TimeUnit.SECOND)
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
    if df.empty or freq is None or 

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

    if seconds % 86400 == 0:
        return TimeFreq(count=int(seconds // 86400), unit=TimeUnit.DAY)
    elif seconds % 3600 == 0:
        return TimeFreq(count=int(seconds // 3600), unit=TimeUnit.HOUR)
    elif seconds % 60 == 0:
        return TimeFreq(count=int(seconds // 60), unit=TimeUnit.MINUTE)
    elif seconds >= 1:
        return TimeFreq(count=int(seconds), unit=TimeUnit.SECOND)
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
#  Value-level checks
# -------------------------------------------------------------------
def find_ohlc_zeros(df: pd.DataFrame) -> pd.DataFrame:
    """
    Return rows where ANY of open/high/low/close is exactly 0.
    """
    req = {"open", "high", "low", "close"}
    miss = req - set(df.columns)
    if miss:
        raise ValueError(f"Missing required columns for OHLC zero check: {sorted(miss)}")

    mask = (df[["open", "high", "low", "close"]] == 0).any(axis=1)
    return df.loc[mask].copy()


def find_unrealistic_ranges(
    df: pd.DataFrame,
    *,
    max_abs_range: float | None = None,
    max_rel_range: float | None = None,
) -> pd.DataFrame:
    """
    Return rows where the candle range (high-low) looks unrealistically large.

    - If max_abs_range is provided, flag rows where (high - low) > max_abs_range.
    - If max_rel_range is provided, flag rows where (high - low) / mid > max_rel_range,
      where mid = 0.5 * (high + low).

    At least one of max_abs_range or max_rel_range must be provided.
    """
    if max_abs_range is None and max_rel_range is None:
        raise ValueError("Provide at least one of max_abs_range or max_rel_range")

    req = {"high", "low"}
    miss = req - set(df.columns)
    if miss:
        raise ValueError(f"Missing required columns for range check: {sorted(miss)}")

    high = df["high"].astype(float)
    low = df["low"].astype(float)
    _range = high - low

    mask = pd.Series(False, index=df.index)

    if max_abs_range is not None:
        mask |= _range > max_abs_range

    if max_rel_range is not None:
        mid = 0.5 * (high + low)
        # avoid div-by-zero if someone sends nonsense data
        rel = _range / np.where(mid == 0.0, np.nan, mid)
        mask |= rel > max_rel_range

    return df.loc[mask].copy()


def find_ohlc_inconsistencies(df: pd.DataFrame) -> pd.DataFrame:
    """
    Return rows that violate basic OHLC ordering rules:

        low <= open <= high
        low <= close <= high
        high >= low
    """
    req = {"open", "high", "low", "close"}
    miss = req - set(df.columns)
    if miss:
        raise ValueError(f"Missing required columns: {sorted(miss)}")

    o = df["open"].astype(float)
    h = df["high"].astype(float)
    l = df["low"].astype(float)
    c = df["close"].astype(float)

    # Identify invalid rows
    mask_invalid = ~((l <= o) & (o <= h) &
                     (l <= c) & (c <= h) &
                     (h >= l))

    bad = df.loc[mask_invalid].copy()

    if bad.empty:
        return bad  # nothing to annotate

    # Add diagnostics
    bad["viol_open_lt_low"]   = o.loc[bad.index] < l.loc[bad.index]
    bad["viol_open_gt_high"]  = o.loc[bad.index] > h.loc[bad.index]
    bad["viol_close_lt_low"]  = c.loc[bad.index] < l.loc[bad.index]
    bad["viol_close_gt_high"] = c.loc[bad.index] > h.loc[bad.index]
    bad["viol_high_lt_low"]   = h.loc[bad.index] < l.loc[bad.index]

    return bad



# -------------------------------------------------------------------
#  Candle validation & report
# -------------------------------------------------------------------
@dataclass
class CandlesDQReport:
    freq: TimeFreq
    n_rows: int
    n_dupes_dropped: int
    n_gaps: int
    missing_starts: List[pd.Timestamp]
    first_ts: pd.Timestamp
    final_ts: pd.Timestamp

    ohlc_zeros: Optional[pd.DataFrame] = None
    n_ohlc_zeros: int = 0

    ohlc_inconsistencies: Optional[pd.DataFrame] = None
    n_ohlc_inconsistencies: int = 0

    large_ranges: Optional[pd.DataFrame] = None
    n_large_ranges: int = 0


def validate_candles(
    df: pd.DataFrame,
    freq: TimeFreq,
    *,
    max_abs_range: float | None = None,
    max_rel_range: float | None = None,
) -> tuple[pd.DataFrame, CandlesDQReport]:
    """
    Validate candles for a *known* frequency.

    - sort + dedupe (strict OHLCV match on dupes)
    - detect gaps using the provided `freq`
    - flag:
        * rows with OHLC zeros
        * rows with inconsistent OHLC ordering
        * rows with unrealistically large ranges (if thresholds provided)

    We do **not** infer frequency here.
    """
    fixed, count_dups_removed = _sort_dedupe(df)
    missing = detect_candle_gaps(fixed, freq=freq)

    # value-level diagnostics
    zeros_df = find_ohlc_zeros(fixed)
    inconsist_df = find_ohlc_inconsistencies(fixed)
    large_ranges_df: Optional[pd.DataFrame] = None
    if max_abs_range is not None or max_rel_range is not None:
        large_ranges_df = find_unrealistic_ranges(
            fixed,
            max_abs_range=max_abs_range,
            max_rel_range=max_rel_range,
        )

    first_candle = fixed["tz_start"].min()
    last_candle = fixed["tz_start"].max()

    report = CandlesDQReport(
        freq=freq,
        n_rows=len(fixed),
        n_dupes_dropped=count_dups_removed,
        n_gaps=len(missing),
        missing_starts=missing,
        first_ts=first_candle,
        final_ts=last_candle,
        ohlc_zeros=zeros_df if not zeros_df.empty else None,
        n_ohlc_zeros=len(zeros_df),
        ohlc_inconsistencies=inconsist_df if not inconsist_df.empty else None,
        n_ohlc_inconsistencies=len(inconsist_df),
        large_ranges=large_ranges_df if (large_ranges_df is not None and not large_ranges_df.empty) else None,
        n_large_ranges=0 if large_ranges_df is None else len(large_ranges_df),
    )
    return fixed, report


def ensure_homogeneous_candle_size(df: pd.DataFrame) -> None:
    """
    Raises ValueError if the DataFrame contains inconsistent candle spacing.

    This version is strict — *all* deltas between consecutive tz_start
    timestamps must be exactly equal. Any deviation is considered invalid.
    
    This is not part of the candles dq report because missing data would show up as non-homogenous sizes

    """
    if df.empty:
        return

    if "tz_start" not in df.columns:
        raise ValueError("Missing tz_start column")

    s = pd.to_datetime(df["tz_start"], utc=True).sort_values().drop_duplicates()
    if len(s) < 3:
        return

    deltas = s.diff().dropna()

    if not deltas.eq(deltas.iloc[0]).all():
        raise ValueError(
            f"Heterogeneous candle spacing detected: {len(deltas.unique())} unique deltas found."
        )


def run_candle_quality(
    df: pd.DataFrame,
    freq: TimeFreq,
    *,
    max_abs_range: float | None = None,
    max_rel_range: float | None = None,
):
    """
    High-level 'one call does all' data-quality validation for candles.

    Returns
    -------
    clean_df : pd.DataFrame
        Sorted, deduped, frequency-consistent candle data.
    report : CandlesDQReport
        Full summary of diagnostics.
    issues : dict[str, Optional[pd.DataFrame]]
        Dictionary mapping issue name → filtered DataFrame of affected rows
        (or None if no such issue).
    """

    # Run the full validator (dedupe + gaps + all OHLC checks)
    clean_df, report = validate_candles(
        df,
        freq=freq,
        max_abs_range=max_abs_range,
        max_rel_range=max_rel_range
    )

    # Construct the issues dictionary
    issues = {
        "ohlc_zeros": report.ohlc_zeros,
        "ohlc_inconsistencies": report.ohlc_inconsistencies,
        "large_ranges": report.large_ranges,
        "missing_candles": pd.DataFrame({"tz_start": report.missing_starts})
            if report.n_gaps > 0 else None,
        "deduped_rows": None,  # optional: populate if needed
    }

    return clean_df, report, issues


def log_candle_dq_issues(
    report: CandlesDQReport,
    *,
    context: str = "",
) -> None:
    """
    Log a summary of candle data-quality issues.

    `context` is something like:
        "BINANCE:BTCUSDT:1m"
        "file:/path/to/file.parquet"
    so logs are easy to attribute.
    """
    ctx = f"[{context}] " if context else ""

    # Always log a basic summary
    log.info(
        "%sCandles DQ: n_rows=%d freq=%s range=[%s -> %s]",
        ctx,
        report.n_rows,
        report.freq,
        report.first_ts,
        report.final_ts,
    )

    # Issues → warnings
    if report.n_dupes_dropped:
        log.warning(
            "%sDropped %d duplicate candles",
            ctx,
            report.n_dupes_dropped,
        )

    if report.n_gaps:
        first_miss = report.missing_starts[0]
        last_miss = report.missing_starts[-1]
        log.warning(
            "%sDetected %d missing candles (first_missing=%s, last_missing=%s)",
            ctx,
            report.n_gaps,
            first_miss,
            last_miss,
        )

    if report.n_ohlc_zeros:
        log.warning(
            "%sFound %d candles with OHLC zeros",
            ctx,
            report.n_ohlc_zeros,
        )

    if report.n_ohlc_inconsistencies:
        log.warning(
            "%sFound %d candles with OHLC ordering inconsistencies",
            ctx,
            report.n_ohlc_inconsistencies,
        )

    if report.n_large_ranges:
        log.warning(
            "%sFound %d candles with unrealistically large ranges",
            ctx,
            report.n_large_ranges,
        )

    # If nothing bad was found, say so explicitly
    if (
        report.n_dupes_dropped == 0
        and report.n_gaps == 0
        and report.n_ohlc_zeros == 0
        and report.n_ohlc_inconsistencies == 0
        and report.n_large_ranges == 0
    ):
        log.info("%sCandles passed all DQ checks", ctx)
