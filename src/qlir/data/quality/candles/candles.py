from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, List
import logging

import numpy as _np
import pandas as _pd

from qlir.data.quality.candles.models.candles_dq_report import CandlesDQReport
from qlir.data.quality.candles.models.candle_gap import CandleGap, candle_gaps_to_df, detect_contiguous_gaps
from qlir.time.ensure_utc import ensure_utc_series, assert_not_epoch_drift
from qlir.time.timefreq import TimeFreq, TimeUnit
from qlir.logging.logdf import logdf
from qlir.utils.str.color import colorize, Ansi
log = logging.getLogger(__name__)


OHLCV_COLS = ["open", "high", "low", "close", "volume"]


# -------------------------------------------------------------------
#  Sorting & deduplication
# -------------------------------------------------------------------

def _sort_dedupe(
    df: _pd.DataFrame,
    time_col: str = "tz_start",
) -> tuple[_pd.DataFrame, int, Optional[_pd.DataFrame]]:
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
    assert_not_epoch_drift(out[time_col])
    log.debug(f"Sorting {len(df)} records")
    out = out.sort_values(time_col)

    conflicts: list[_pd.DataFrame] = []

    log.debug("Finding duplicates")
    dup_mask = out.duplicated(subset=[time_col], keep=False)
    

    if dup_mask.any():
        dup_df = out[dup_mask]
        to_drop_idx: list[int] = []
        log.debug("Attempting to safely deduplicate. This can be slow. Note to Self todo: add TRACE logging to expose details")
        
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
            log.debug(f"Safely dropping {len(to_drop_idx)} records")
            out = out.drop(index=to_drop_idx)

    out = out.reset_index(drop=True)
    n_dropped = before - len(out)

    conflicts_df: Optional[_pd.DataFrame]
    if conflicts:
        conflicts_df = _pd.concat(conflicts, ignore_index=True)
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
def infer_freq(df: _pd.DataFrame) -> Optional[TimeFreq]:
    if df.empty or "tz_start" not in df.columns:
        return None

    s = _pd.to_datetime(df["tz_start"], utc=True).sort_values().drop_duplicates()
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
            off = _pd.tseries.frequencies.to_offset(delta)
            return TimeFreq(count=off.n, unit=off.name, pandas_offset=off)
        except ValueError:
            return None

# -------------------------------------------------------------------
#  Candle gap detection
# -------------------------------------------------------------------
def detect_missing_candles(df: _pd.DataFrame, freq: Optional[TimeFreq] = None) -> list[_pd.Timestamp]:
    if df.empty or freq is None or len(df) < 2:
        return []

    req = {"tz_start", "open", "high", "low", "close"}
    miss = req - set(df.columns)
    if miss:
        raise ValueError(f"Missing required columns: {sorted(miss)}")

    fixed, _, _ = _sort_dedupe(df)
    s = _pd.to_datetime(fixed["tz_start"], utc=True)

    expected = _pd.date_range(
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
def find_ohlc_zeros(df: _pd.DataFrame) -> _pd.DataFrame:
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
    df: _pd.DataFrame,
    *,
    max_abs_range: float | None = None,
    max_rel_range: float | None = None,
) -> _pd.DataFrame:
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

    mask = _pd.Series(False, index=df.index)

    if max_abs_range is not None:
        mask |= _range > max_abs_range

    if max_rel_range is not None:
        mid = 0.5 * (high + low)
        # avoid div-by-zero if someone sends nonsense data
        rel = _range / _np.where(mid == 0.0, _np.nan, mid)
        mask |= rel > max_rel_range

    return df.loc[mask].copy()


def find_ohlc_inconsistencies(df: _pd.DataFrame) -> _pd.DataFrame:
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

def format_missing_str(missing: list, *, max_items: int = 10) -> str:
    """
    Format a human-readable summary of missing items.

    If the list is longer than `max_items`, only the first `max_items`
    are shown and the remainder is summarized.

    Examples:
        - [] -> "none"
        - [a, b] -> "a, b"
        - [a, b, c, ...] -> "a, b, c, ... (+7 more)"
    """
    if not missing:
        return "none"

    n = len(missing)

    if n <= max_items:
        return ", ".join(map(str, missing))

    shown = ", ".join(map(str, missing[:max_items]))
    remaining = n - max_items
    more_str = colorize("+ more", Ansi.BOLD, Ansi.RED)
    return f"{shown}, … {more_str}"

def ensure_homogeneous_candle_size(df: _pd.DataFrame) -> None:
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

    s = _pd.to_datetime(df["tz_start"], utc=True).sort_values().drop_duplicates()
    if len(s) < 3:
        return

    deltas = s.diff().dropna()

    if not deltas.eq(deltas.iloc[0]).all():
        raise ValueError(
            f"Heterogeneous candle spacing detected: {len(deltas.unique())} unique deltas found."
        )


def summarize_gap_sizes_df(gaps: list[CandleGap]) -> _pd.DataFrame:
    """
    Return gap-size distribution as a DataFrame.
    """
    df = _pd.DataFrame(
        {"gap_size": [g.missing_count for g in gaps]}
    )

    return (
        df.value_counts("gap_size")
          .rename("gap_count")
          .reset_index()
          .sort_values("gap_size", ascending=False)
    )

def gap_sizes_df_to_records(
    df: _pd.DataFrame,
) -> list[dict]:
    """
    View adapter: convert gap-size summary DF into
    logging / JSON friendly records.
    """
    if df.empty:
        return []

    expected_cols = ["gap_size", "gap_count"]
    missing = set(expected_cols) - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    # Preserve ordering from DF (already sorted there)
    return df[expected_cols].to_dict(orient="records")


# -------------------------------------------------------------------
#  Candle validation
# -------------------------------------------------------------------

def validate_candles(
    df: _pd.DataFrame,
    freq: TimeFreq,
    *,
    max_abs_range: float | None = None,
    max_rel_range: float | None = None,
) -> tuple[_pd.DataFrame, CandlesDQReport]:
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
    fixed, count_dups_removed, conflicts = _sort_dedupe(df)
    
    # Missing Candles & Further Grouping/Agg/Views 
    missing = detect_missing_candles(fixed, freq=freq)
    gaps = detect_contiguous_gaps(missing, freq)
    gaps_df = candle_gaps_to_df(gaps)
    gap_sizes_df = summarize_gap_sizes_df(gaps)
    gap_sizes_dict = gap_sizes_df_to_records(gap_sizes_df)
    
    # value-level diagnostics
    zeros_df = find_ohlc_zeros(fixed)
    inconsist_df = find_ohlc_inconsistencies(fixed)
    gigantic_candles_df: Optional[_pd.DataFrame] = None
    if max_abs_range is not None or max_rel_range is not None:
        gigantic_candles_df = find_unrealistic_ranges(
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
        n_gaps=len(gaps),
        gaps=gaps,
        gaps_df=gaps_df,
        gap_sizes_dict=gap_sizes_dict,
        gap_sizes_df=gap_sizes_df,
        missing_starts=missing,
        first_ts=first_candle,
        final_ts=last_candle,
        ohlc_zeros=zeros_df if not zeros_df.empty else None,
        n_ohlc_zeros=len(zeros_df),
        ohlc_inconsistencies=inconsist_df if not inconsist_df.empty else None,
        n_ohlc_inconsistencies=len(inconsist_df),
        unrealistically_large_candles=gigantic_candles_df if (gigantic_candles_df is not None and not gigantic_candles_df.empty) else None,
        n_unrealistically_large_candles=0 if gigantic_candles_df is None else len(gigantic_candles_df),
    )
    return fixed, report


def run_candle_quality(
    df: _pd.DataFrame,
    freq: TimeFreq,
    *,
    max_abs_range: float | None = None,
    max_rel_range: float | None = None,
):
    """
    High-level 'one call does all' data-quality validation for candles.

    Returns
    -------
    clean_df : _pd.DataFrame
        Sorted, deduped, frequency-consistent candle data.
    report : CandlesDQReport
        Full summary of diagnostics.
    issues : dict[str, Optional[_pd.DataFrame]]
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
        "large_ranges": report.unrealistically_large_candles,
        "missing_candles": _pd.DataFrame({"tz_start": report.missing_starts})
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
        "%sCandles Summary: \n" \
        "   n_rows=%d \n" \
        "   freq=%s \n" \
        "   range=[%s -> %s] \n",
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
        log.warning(
            f"{ctx} Detected {colorize(str(report.n_gaps), Ansi.BOLD, Ansi.RED)} gaps which represent {colorize(str(len(report.missing_starts)), Ansi.BOLD, Ansi.RED)} missing candles"
        )
        report.gaps_df = report.gaps_df.sort_values("missing_count", ascending=False)
        logdf(report.gaps_df, name="Candle Gaps")
        report.gap_sizes_df = report.gap_sizes_df.sort_values("gap_count", ascending=False)
        logdf(report.gap_sizes_df, name="Gap Sizes")

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

    if report.n_unrealistically_large_candles:
        log.warning(
            "%sFound %d candles with unrealistically large ranges",
            ctx,
            report.n_unrealistically_large_candles,
        )

    # If nothing bad was found, say so explicitly
    if (
        report.n_dupes_dropped == 0
        and report.n_gaps == 0
        and report.n_ohlc_zeros == 0
        and report.n_ohlc_inconsistencies == 0
        and report.n_unrealistically_large_candles == 0
    ):
        log.info("%sCandles passed all DQ checks", ctx)
