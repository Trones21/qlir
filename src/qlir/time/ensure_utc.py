import pandas as pd
from typing import Union
from qlir.time.constants import DEFAULT_TS_COL

# ============================================================
#  Series-level utilities
# ============================================================

def ensure_utc_series(s: pd.Series) -> pd.Series:
    """
    Convert a Series to UTC using best-effort parsing (loose).

    Accepts mixed formats and coerces values into datetime64[ns, UTC].
    Raises ValueError if any rows cannot be parsed.
    """
    if not pd.api.types.is_datetime64_any_dtype(s):
        out = pd.to_datetime(s, utc=True, errors="coerce")
    elif s.dt.tz is None:
        out = s.dt.tz_localize("UTC")
    else:
        out = s.dt.tz_convert("UTC")

    if out.isna().any():
        raise ValueError("Invalid timestamps found during UTC normalization.")
    return out


def ensure_utc_series_strict(s: pd.Series) -> pd.Series:
    """
    Convert a Series to UTC, enforcing the exact format "%Y-%m-%d %H:%M:%S".

    This function will *not* fall back to any alternative parsing.
    Raises ValueError if any rows cannot be parsed.

    Examples
    --------
    >>> s = pd.Series(["2025-01-05 12:30:00", "2025-01-06 00:00:00"])
    >>> ensure_utc_series_strict(s)
    0   2025-01-05 12:30:00+00:00
    1   2025-01-06 00:00:00+00:00
    dtype: datetime64[ns, UTC]
    """
    out = pd.to_datetime(
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


# ============================================================
#  DataFrame-level utilities
# ============================================================

def ensure_utc_df(df: pd.DataFrame, col: str) -> pd.DataFrame:
    """
    Return a copy of the DataFrame with df[col] normalized to UTC (loose).
    """
    if col not in df.columns:
        raise KeyError(f"Column '{col}' not found in DataFrame.")
    out = df.copy()
    out[col] = ensure_utc_series(out[col])
    return out


def ensure_utc_df_strict(df: pd.DataFrame, col: str) -> pd.DataFrame:
    """
    Return a copy of the DataFrame with df[col] normalized to UTC (strict).
    Requires exact match to '%Y-%m-%d %H:%M:%S'.
    """
    if col not in df.columns:
        raise KeyError(f"Column '{col}' not found in DataFrame.")
    out = df.copy()
    out[col] = ensure_utc_series_strict(out[col])
    return out


# ============================================================
#  Public interface
# ============================================================

__all__ = [
    "ensure_utc_series",
    "ensure_utc_series_strict",
    "ensure_utc_df",
    "ensure_utc_df_strict",
]

# default alias for backward compatibility
ensure_utc = ensure_utc_df
