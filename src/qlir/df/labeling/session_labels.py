# qlir/df/labeling/session_labels.py

from __future__ import annotations

import pandas as pd

from .utils import ensure_utc, DEFAULT_TS_COL
from qlir.df.filtering import session as fsession


def _left_mark(
    df: pd.DataFrame,
    subset: pd.DataFrame,
    *,
    key: str,
    out_col: str,
    value=True,
) -> pd.DataFrame:
    """
    Generic "filter then self-join" marker.
    df: full dataframe
    subset: filtered dataframe (same key col)
    key: name of the timestamp column (usually tz_start)
    out_col: name of the column to add
    value: value to write for matching rows
    """
    df = df.copy()
    df[out_col] = False if isinstance(value, bool) else None

    # use index-based set for speed instead of an actual merge
    match_idx = df[key].isin(subset[key])
    df.loc[match_idx, out_col] = value
    return df


def mark_ny_cash(
    df: pd.DataFrame,
    col: str = DEFAULT_TS_COL,
    out_col: str = "in_ny_cash",
) -> pd.DataFrame:
    """
    Mark rows that are inside NY cash session (09:30–16:00 America/New_York).
    Uses filtering.session.ny_cash_session(...) under the hood.
    """
    df = ensure_utc(df, col)
    ny_df = fsession.ny_cash_session(df, col=col, add_label=False)
    return _left_mark(df, ny_df, key=col, out_col=out_col, value=True)


def mark_ny_premarket(
    df: pd.DataFrame,
    col: str = DEFAULT_TS_COL,
    out_col: str = "in_ny_premarket",
) -> pd.DataFrame:
    df = ensure_utc(df, col)
    pre_df = fsession.ny_premarket(df, col=col, add_label=False)
    return _left_mark(df, pre_df, key=col, out_col=out_col, value=True)


def mark_ny_afterhours(
    df: pd.DataFrame,
    col: str = DEFAULT_TS_COL,
    out_col: str = "in_ny_afterhours",
) -> pd.DataFrame:
    df = ensure_utc(df, col)
    aft_df = fsession.ny_afterhours(df, col=col, add_label=False)
    return _left_mark(df, aft_df, key=col, out_col=out_col, value=True)


def mark_ny_extended(
    df: pd.DataFrame,
    col: str = DEFAULT_TS_COL,
    out_col: str = "in_ny_extended",
) -> pd.DataFrame:
    df = ensure_utc(df, col)
    ext_df = fsession.ny_extended(df, col=col, add_label=False)
    return _left_mark(df, ext_df, key=col, out_col=out_col, value=True)


def mark_london(
    df: pd.DataFrame,
    col: str = DEFAULT_TS_COL,
    out_col: str = "in_london_session",
) -> pd.DataFrame:
    df = ensure_utc(df, col)
    ldn_df = fsession.london_session(df, col=col, add_label=False)
    return _left_mark(df, ldn_df, key=col, out_col=out_col, value=True)


def mark_frankfurt(
    df: pd.DataFrame,
    col: str = DEFAULT_TS_COL,
    out_col: str = "in_frankfurt_session",
) -> pd.DataFrame:
    df = ensure_utc(df, col)
    ffm_df = fsession.frankfurt_session(df, col=col, add_label=False)
    return _left_mark(df, ffm_df, key=col, out_col=out_col, value=True)


def mark_london_ny_overlap(
    df: pd.DataFrame,
    col: str = DEFAULT_TS_COL,
    out_col: str = "in_ldn_ny_overlap",
) -> pd.DataFrame:
    df = ensure_utc(df, col)
    ov_df = fsession.london_newyork_overlap(df, col=col, add_label=False)
    return _left_mark(df, ov_df, key=col, out_col=out_col, value=True)


def mark_all_common_sessions(
    df: pd.DataFrame,
    col: str = DEFAULT_TS_COL,
) -> pd.DataFrame:
    """
    Convenience: add a bunch of common session flags in one call.
    """
    out = df
    out = mark_ny_cash(out, col=col)
    out = mark_ny_premarket(out, col=col)
    out = mark_ny_afterhours(out, col=col)
    out = mark_ny_extended(out, col=col)
    out = mark_london(out, col=col)
    out = mark_frankfurt(out, col=col)
    out = mark_london_ny_overlap(out, col=col)
    return out
