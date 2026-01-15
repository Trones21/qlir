from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Iterable

import pandas as _pd

from qlir.time.constants import DEFAULT_TS_COL
from qlir.time.ensure_utc import ensure_utc_df_strict


def _as_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


# ---------------- calendar-style ---------------- #

def in_year(df: _pd.DataFrame, year: int, col: str = DEFAULT_TS_COL, *, add_labels: bool = False) -> _pd.DataFrame:
    df = ensure_utc_df_strict(df, col)
    res = df[df[col].dt.year == year]
    return add_date_labels(res, col) if add_labels else res


def in_quarter(df: _pd.DataFrame, year: int, quarter: int, col: str = DEFAULT_TS_COL, *, add_labels: bool = False) -> _pd.DataFrame:
    if quarter not in (1, 2, 3, 4):
        raise ValueError("quarter must be 1..4")
    df = ensure_utc_df_strict(df, col)
    q = ((df[col].dt.month - 1) // 3) + 1
    res = df[(df[col].dt.year == year) & (q == quarter)]
    return add_date_labels(res, col) if add_labels else res


def in_month(df: _pd.DataFrame, month: int, col: str = DEFAULT_TS_COL, *, add_labels: bool = False) -> _pd.DataFrame:
    df = ensure_utc_df_strict(df, col)
    res = df[df[col].dt.month == month]
    return add_date_labels(res, col) if add_labels else res


def in_month_of_year(df: _pd.DataFrame, year: int, month: int, col: str = DEFAULT_TS_COL, *, add_labels: bool = False) -> _pd.DataFrame:
    df = ensure_utc_df_strict(df, col)
    res = df[(df[col].dt.year == year) & (df[col].dt.month == month)]
    return add_date_labels(res, col) if add_labels else res


def in_week_of_year(df: _pd.DataFrame, year: int, week: int, col: str = DEFAULT_TS_COL, *, add_labels: bool = False) -> _pd.DataFrame:
    df = ensure_utc_df_strict(df, col)
    iso = df[col].dt.isocalendar()
    res = df[(iso.year == year) & (iso.week == week)]
    return add_date_labels(res, col) if add_labels else res


def in_day_of_week(df: _pd.DataFrame, dows: Iterable[int], col: str = DEFAULT_TS_COL, *, add_labels: bool = False) -> _pd.DataFrame:
    df = ensure_utc_df_strict(df, col)
    dows = set(dows)
    res = df[df[col].dt.dayofweek.isin(dows)]
    return add_date_labels(res, col) if add_labels else res


# ---------------- intraday ---------------- #

def in_hour_of_day(df: _pd.DataFrame, hours: Iterable[int], col: str = DEFAULT_TS_COL, *, add_labels: bool = False) -> _pd.DataFrame:
    df = ensure_utc_df_strict(df, col)
    hours = set(hours)
    res = df[df[col].dt.hour.isin(hours)]
    return add_date_labels(res, col) if add_labels else res


def in_minute_of_hour(df: _pd.DataFrame, minutes: Iterable[int], col: str = DEFAULT_TS_COL, *, add_labels: bool = False) -> _pd.DataFrame:
    df = ensure_utc_df_strict(df, col)
    minutes = set(minutes)
    res = df[df[col].dt.minute.isin(minutes)]
    return add_date_labels(res, col) if add_labels else res


# ---------------- ranges ---------------- #

def between(df: _pd.DataFrame, start: datetime | str, end: datetime | str, col: str = DEFAULT_TS_COL, *, add_labels: bool = False) -> _pd.DataFrame:
    df = ensure_utc_df_strict(df, col)
    start_dt = _pd.to_datetime(start, utc=True) if isinstance(start, str) else _as_utc(start)
    end_dt = _pd.to_datetime(end, utc=True) if isinstance(end, str) else _as_utc(end)
    res = df[(df[col] >= start_dt) & (df[col] < end_dt)]
    return add_date_labels(res, col) if add_labels else res


def last_n_days(df: _pd.DataFrame, n: int, *, now: datetime | None = None, col: str = DEFAULT_TS_COL, add_labels: bool = False) -> _pd.DataFrame:
    df = ensure_utc_df_strict(df, col)
    if now is None:
        now = datetime.now(timezone.utc)
    else:
        now = _as_utc(now)
    start = now - timedelta(days=n)
    res = df[(df[col] >= start) & (df[col] <= now)]
    return add_date_labels(res, col) if add_labels else res


# ---------------- convenience ---------------- #

def year_to_date(df: _pd.DataFrame, *, now: datetime | None = None, col: str = DEFAULT_TS_COL, add_labels: bool = False) -> _pd.DataFrame:
    df = ensure_utc_df_strict(df, col)
    if now is None:
        now = datetime.now(timezone.utc)
    else:
        now = _as_utc(now)
    start = datetime(now.year, 1, 1, tzinfo=timezone.utc)
    res = df[(df[col] >= start) & (df[col] <= now)]
    return add_date_labels(res, col) if add_labels else res


def month_to_date(df: _pd.DataFrame, *, now: datetime | None = None, col: str = DEFAULT_TS_COL, add_labels: bool = False) -> _pd.DataFrame:
    df = ensure_utc_df_strict(df, col)
    if now is None:
        now = datetime.now(timezone.utc)
    else:
        now = _as_utc(now)
    start = datetime(now.year, now.month, 1, tzinfo=timezone.utc)
    res = df[(df[col] >= start) & (df[col] <= now)]
    return add_date_labels(res, col) if add_labels else res


# ---------------- labeling helper ---------------- #

def add_date_labels(
    df: _pd.DataFrame,
    col: str = DEFAULT_TS_COL,
    *,
    year: bool = True,
    quarter: bool = True,
    month: bool = True,
    dow: bool = True,
    hour: bool = True,
    minute: bool = False,
) -> _pd.DataFrame:
    df = ensure_utc_df_strict(df, col)
    out = df.copy()
    dt = out[col]
    if year:
        out["year"] = dt.dt.year
    if quarter:
        out["quarter"] = ((dt.dt.month - 1) // 3) + 1
    if month:
        out["month"] = dt.dt.month
    if dow:
        out["dow"] = dt.dt.dayofweek
    if hour:
        out["hour"] = dt.dt.hour
    if minute:
        out["minute"] = dt.dt.minute
    return out
