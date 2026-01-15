# qlir/df/filtering/session.py

"""
Trading/session-style dataframe filters.

All data is assumed stored in UTC, but sessions are defined in local market time.
We:
1. ensure UTC
2. convert to target tz
3. compare local time to a time window
4. return the original rows (still UTC) and optionally tag them with `session`.

Includes:
- NY cash (09:30–16:00)
- NY premarket (04:00–09:30)
- NY after-hours (16:00–20:00)
- NY extended (premarket + cash + after-hours)
- Frankfurt
- London
- Tokyo
- London–NY overlap
"""

from __future__ import annotations

from datetime import time
from zoneinfo import ZoneInfo

import pandas as _pd

from qlir.time.constants import DEFAULT_TS_COL
from qlir.time.ensure_utc import ensure_utc_df_strict


def _add_session_label(df: _pd.DataFrame, label: str) -> _pd.DataFrame:
    out = df.copy()
    out["session"] = label
    return out


def in_session(
    df: _pd.DataFrame,
    *,
    start_t: time,
    end_t: time,
    tz: str,
    col: str = DEFAULT_TS_COL,
    add_label: str | None = None,
) -> _pd.DataFrame:
    """
    Generic session filter with wraparound support.
    """
    df = ensure_utc_df_strict(df, col)
    local = df[col].dt.tz_convert(ZoneInfo(tz))
    local_time = local.dt.time

    if start_t < end_t:
        mask = (local_time >= start_t) & (local_time < end_t)
    else:
        # overnight
        mask = (local_time >= start_t) | (local_time < end_t)

    res = df[mask]
    if add_label is not None:
        res = _add_session_label(res, add_label)
    return res


def ny_cash_session(df: _pd.DataFrame, col: str = DEFAULT_TS_COL, *, add_label: bool = True) -> _pd.DataFrame:
    return in_session(
        df,
        start_t=time(9, 30),
        end_t=time(16, 0),
        tz="America/New_York",
        col=col,
        add_label="ny_cash" if add_label else None,
    )


def ny_premarket(df: _pd.DataFrame, col: str = DEFAULT_TS_COL, *, add_label: bool = True) -> _pd.DataFrame:
    return in_session(
        df,
        start_t=time(4, 0),
        end_t=time(9, 30),
        tz="America/New_York",
        col=col,
        add_label="ny_premarket" if add_label else None,
    )


def ny_afterhours(df: _pd.DataFrame, col: str = DEFAULT_TS_COL, *, add_label: bool = True) -> _pd.DataFrame:
    return in_session(
        df,
        start_t=time(16, 0),
        end_t=time(20, 0),
        tz="America/New_York",
        col=col,
        add_label="ny_afterhours" if add_label else None,
    )


def ny_extended(df: _pd.DataFrame, col: str = DEFAULT_TS_COL, *, add_label: bool = True) -> _pd.DataFrame:
    pre = ny_premarket(df, col=col, add_label=False)
    cash = ny_cash_session(df, col=col, add_label=False)
    aft = ny_afterhours(df, col=col, add_label=False)
    merged = (
        _pd.concat([pre, cash, aft], ignore_index=True)
        .drop_duplicates(subset=[col])
        .sort_values(col)
        .reset_index(drop=True)
    )
    if add_label:
        merged = _add_session_label(merged, "ny_extended")
    return merged


def frankfurt_session(df: _pd.DataFrame, col: str = DEFAULT_TS_COL, *, add_label: bool = True) -> _pd.DataFrame:
    return in_session(
        df,
        start_t=time(9, 0),
        end_t=time(17, 0),
        tz="Europe/Berlin",
        col=col,
        add_label="ffm" if add_label else None,
    )


def london_session(df: _pd.DataFrame, col: str = DEFAULT_TS_COL, *, add_label: bool = True) -> _pd.DataFrame:
    return in_session(
        df,
        start_t=time(8, 0),
        end_t=time(16, 30),
        tz="Europe/London",
        col=col,
        add_label="london" if add_label else None,
    )


def tokyo_session(df: _pd.DataFrame, col: str = DEFAULT_TS_COL, *, add_label: bool = True) -> _pd.DataFrame:
    return in_session(
        df,
        start_t=time(9, 0),
        end_t=time(15, 0),
        tz="Asia/Tokyo",
        col=col,
        add_label="tokyo" if add_label else None,
    )


def london_newyork_overlap(df: _pd.DataFrame, col: str = DEFAULT_TS_COL, *, add_label: bool = True) -> _pd.DataFrame:
    df = ensure_utc_df_strict(df, col)

    london_local = df[col].dt.tz_convert(ZoneInfo("Europe/London"))
    l_mask = (london_local.dt.time >= time(8, 0)) & (london_local.dt.time < time(16, 30))

    ny_local = df[col].dt.tz_convert(ZoneInfo("America/New_York"))
    ny_mask = (ny_local.dt.time >= time(8, 0)) & (ny_local.dt.time < time(17, 0))

    res = df[l_mask & ny_mask]
    if add_label:
        res = _add_session_label(res, "ldn_ny_overlap")
    return res
