
import pandas as _pd

# ============================================================
#  Series-level utilities
# ============================================================

def ensure_utc_series(s: _pd.Series) -> _pd.Series:
    """
    Normalize a timestamp Series to timezone-aware UTC datetimes.

    This function is designed for data pipelines where timestamps may appear
    in multiple common representations, especially market / event data.

    Supported input types
    ---------------------
    - datetime64[ns] (tz-aware or tz-naive)
        * tz-naive values are assumed to be UTC and localized
        * tz-aware values are converted to UTC
    - integer epochs
        * interpreted as **milliseconds** if max(value) > 1e12
        * otherwise interpreted as **seconds**
    - strings or mixed object dtype
        * parsed using pandas' flexible datetime parser

    Guarantees
    ----------
    - Output is always dtype: datetime64[ns, UTC]
    - No silent failures: unparseable values raise ValueError
    - Numeric epochs are never interpreted as nanoseconds

    Notes
    -----
    This function intentionally applies a heuristic for numeric timestamps
    (ms vs s) because many external data sources do not specify units.
    It is **not** suitable for strict format enforcement.

    For strict string-only parsing with an exact format, use a dedicated
    strict parser instead.

    Parameters
    ----------
    s : _pd.Series
        Series containing timestamps.

    Returns
    -------
    _pd.Series
        Timezone-aware UTC timestamps.

    Raises
    ------
    ValueError
        If any timestamps cannot be parsed or normalized.
    """
    if _pd.api.types.is_datetime64_any_dtype(s):
        if s.dt.tz is None:
            return s.dt.tz_localize("UTC")
        return s.dt.tz_convert("UTC")

    if _pd.api.types.is_integer_dtype(s):
        unit = "ms" if s.max() > 1e12 else "s"
        return _pd.to_datetime(s, unit=unit, utc=True)

    # strings or mixed objects
    out = _pd.to_datetime(s, utc=True, errors="coerce")

    if out.isna().any():
        raise ValueError("Invalid timestamps during UTC normalization")

    return out



def ensure_utc_series_strict_string(s: _pd.Series) -> _pd.Series:
    """
    Convert a Series to UTC, enforcing the exact format "%Y-%m-%d %H:%M:%S".

    This function will *not* fall back to any alternative parsing.
    Raises ValueError if any rows cannot be parsed.

    Examples
    --------
    >>> s = _pd.Series(["2025-01-05 12:30:00", "2025-01-06 00:00:00"])
    >>> ensure_utc_series_strict(s)
    0   2025-01-05 12:30:00+00:00
    1   2025-01-06 00:00:00+00:00
    dtype: datetime64[ns, UTC]
    """
    out = _pd.to_datetime(
        s,
        utc=True,
        errors="coerce",
        format="%Y-%m-%d %H:%M:%S",
    )

    if out.isna().any():
        raise ValueError(
            "Invalid or non-matching timestamps detected during strict UTC normalization. "
            "Expected format: '%Y-%m-%d %H:%M:%S'."
        )
    return out


def assert_not_epoch_drift(s: _pd.Series, *, min_year=2000) -> None:
    if s.min().year < min_year:
        raise ValueError(
            f"Timestamp drift detected (min year={s.min().year}). "
            "Likely unit mismatch (ms vs ns)."
        )

# ============================================================
#  DataFrame-level utilities
# ============================================================

def ensure_utc_df(df: _pd.DataFrame, col: str) -> _pd.DataFrame:
    """
    Return a copy of the DataFrame with df[col] normalized to UTC (loose).
    """
    if col not in df.columns:
        raise KeyError(f"Column '{col}' not found in DataFrame.")
    out = df.copy()
    out[col] = ensure_utc_series(out[col])
    return out


def ensure_utc_df_strict(df: _pd.DataFrame, col: str) -> _pd.DataFrame:
    """
    Return a copy of the DataFrame with df[col] normalized to UTC (strict).
    Requires exact match to '%Y-%m-%d %H:%M:%S'.
    """
    if col not in df.columns:
        raise KeyError(f"Column '{col}' not found in DataFrame.")
    out = df.copy()
    out[col] = ensure_utc_series_strict_string(out[col])
    return out


# ============================================================
#  Public interface
# ============================================================

__all__ = [
    "ensure_utc_series",
    "ensure_utc_series_strict_string",
    "ensure_utc_df",
    "ensure_utc_df_strict",
]

# default alias for backward compatibility
ensure_utc = ensure_utc_df
