from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Literal, Optional
import pandas as pd

OHLCV_REQ = ("open", "high", "low", "close")
TIME_REQ  = ("tz_start", "tz_end")  # tz_end may be NaT for partials

@dataclass(frozen=True)
class OhlcvContract:
    require_volume: bool = False           # set True if you want 'volume' mandatory
    allow_partials: bool = True            # if False: tz_end must be non-null for all rows
    index_by: Optional[Literal["tz_end","tz_start"]] = None  # loader convenience

def _tz_aware(series: pd.Series) -> bool:
    return pd.api.types.is_datetime64tz_dtype(series)

def validate_ohlcv(df: pd.DataFrame, *, cfg: OhlcvContract = OhlcvContract()) -> None:
    """
    Validate canonical OHLCV schema:
      - tz_start, tz_end exist and are tz-aware UTC datetimes (tz_end may be NaT if allow_partials)
      - open/high/low/close exist (float)
      - volume optional (or required if cfg.require_volume)
      - for closed bars (tz_end notna): tz_start < tz_end
      - for partial bars (tz_end isna): close must be NaN (prevents accidental use)
    Raises ValueError/TypeError with actionable messages.
    """
    missing = [c for c in (*TIME_REQ, *OHLCV_REQ) if c not in df.columns]
    if cfg.require_volume and "volume" not in df.columns:
        missing.append("volume")
    if missing:
        raise ValueError(f"Missing required columns: {missing}. "
                         f"Expected at least {[*TIME_REQ, *OHLCV_REQ]}" +
                         (", volume" if cfg.require_volume else ""))

    # time columns tz-aware
    if not _tz_aware(df["tz_start"]):
        raise TypeError("tz_start must be timezone-aware UTC datetimes (datetime64[ns, UTC]).")
    if df["tz_end"].notna().any() and not _tz_aware(df["tz_end"]):
        raise TypeError("tz_end must be timezone-aware UTC datetimes when present (datetime64[ns, UTC]).")

    # enforce partials policy
    if not cfg.allow_partials and df["tz_end"].isna().any():
        n = int(df["tz_end"].isna().sum())
        raise ValueError(f"Found {n} partial rows (tz_end is NaT) but allow_partials=False.")

    # closed bar ordering
    closed = df["tz_end"].notna()
    if closed.any():
        bad = ~(df.loc[closed, "tz_start"] < df.loc[closed, "tz_end"])
        if bad.any():
            i = df.index[closed][bad][0]
            raise ValueError(f"tz_start must be < tz_end for closed bars (bad row index: {i}).")

    # partials must not carry a final close
    if "close" in df.columns:
        bad_close = df["tz_end"].isna() & df["close"].notna()
        if bad_close.any():
            i = df.index[bad_close][0]
            raise ValueError(f"Partial bars must have close=NaN (row index: {i}).")

    # numeric sanity (don’t cast—just check)
    for c in OHLCV_REQ + (("volume",) if "volume" in df.columns else ()):
        if c in df.columns and not pd.api.types.is_numeric_dtype(df[c]):
            raise TypeError(f"{c} must be numeric dtype (got {df[c].dtype}).")