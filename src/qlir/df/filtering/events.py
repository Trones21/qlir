# qlir/df/filtering/events.py

"""
Event-anchored dataframe filters.

Supports:
- windows around a list of event timestamps
- one-sided windows (before-only, after-only)
- event-class filtering if your anchors carry metadata
- include / exclude windows
- optional tagging of rows with event_id / event_label

This is meant for: FOMC, CPI, big listings, exchange incidents, etc.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Sequence, Mapping, Iterable

import pandas as pd

from qlir.time.constants import DEFAULT_TS_COL
from qlir.time.ensure_utc import ensure_utc_df_strict


def _as_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _normalize_events(
    events: Sequence[datetime] | Sequence[Mapping[str, Any]] | pd.DataFrame,
    ts_key: str = "ts",
) -> list[dict[str, Any]]:
    norm: list[dict[str, Any]] = []
    if isinstance(events, pd.DataFrame):
        for _, row in events.iterrows():
            ts = pd.to_datetime(row[ts_key], utc=True).to_pydatetime()
            d = row.to_dict()
            d["ts"] = ts
            norm.append(d)
        return norm

    for e in events:
        if isinstance(e, datetime):
            norm.append({"ts": _as_utc(e)})
        else:
            ts = _as_utc(e[ts_key])
            d = dict(e)
            d["ts"] = ts
            norm.append(d)
    return norm


# --------- selection primitives (where_all / where_any) --------- #

def where_all(events, conditions: dict[str, object], *, ts_key: str = "ts") -> list[dict]:
    norm = _normalize_events(events, ts_key=ts_key)
    return [e for e in norm if all(e.get(k) == v for k, v in conditions.items())]


def where_any(events, conditions: dict[str, Iterable[object] | object], *, ts_key: str = "ts") -> list[dict]:
    norm = _normalize_events(events, ts_key=ts_key)
    out: list[dict] = []
    for e in norm:
        for field, vals in conditions.items():
            if not isinstance(vals, (list, set, tuple)):
                vals = {vals}
            else:
                vals = set(vals)
            if e.get(field) in vals:
                out.append(e)
                break
    return out


def _dedup_events(events: list[dict], *, unique_by: str = "ts") -> list[dict]:
    seen = set()
    out: list[dict] = []
    for e in events:
        key = e.get(unique_by)
        if key in seen:
            continue
        seen.add(key)
        out.append(e)
    return out


def combine_events(*event_lists: list[dict], unique: bool = True, unique_by: str = "ts") -> list[dict]:
    merged: list[dict] = []
    for lst in event_lists:
        merged.extend(lst)
    if unique:
        merged = _dedup_events(merged, unique_by=unique_by)
    return merged


# --------- window builders --------- #

def make_windows_from_events(events: Sequence[dict[str, Any]], before: timedelta, after: timedelta):
    windows: list[tuple[datetime, datetime]] = []
    for e in events:
        anchor = e["ts"]
        windows.append((anchor - before, anchor + after))
    return windows


def make_before_windows_from_events(events: Sequence[dict[str, Any]], before: timedelta):
    windows: list[tuple[datetime, datetime]] = []
    for e in events:
        anchor = e["ts"]
        windows.append((anchor - before, anchor))
    return windows


def make_after_windows_from_events(events: Sequence[dict[str, Any]], after: timedelta):
    windows: list[tuple[datetime, datetime]] = []
    for e in events:
        anchor = e["ts"]
        windows.append((anchor, anchor + after))
    return windows


# --------- dataframe filters --------- #

def around_anchors(
    df: pd.DataFrame,
    anchors,
    *,
    before: timedelta,
    after: timedelta,
    col: str = DEFAULT_TS_COL,
    add_event_id: bool = False,
    event_label_prefix: str = "event_",
    ts_key: str = "ts",
) -> pd.DataFrame:
    df = ensure_utc_df_strict(df, col)
    ts = df[col]

    events = _normalize_events(anchors, ts_key=ts_key)
    windows = make_windows_from_events(events, before, after)

    if len(windows) == 1 and not add_event_id:
        start, end = windows[0]
        return df[(ts >= start) & (ts <= end)]

    mask = pd.Series(False, index=df.index)
    event_ids: list[int | None] = [None] * len(df)

    for idx, (start, end) in enumerate(windows):
        wmask = (ts >= start) & (ts <= end)
        if add_event_id:
            newly = wmask & (~mask)
            if newly.any():
                for i in newly[newly].index:
                    event_ids[i] = idx
        mask = mask | wmask

    filtered = df[mask]

    if add_event_id:
        event_series = pd.Series(event_ids, index=df.index, dtype="Int64").loc[filtered.index]
        filtered = filtered.copy()
        filtered["event_id"] = event_series
        filtered["event_label"] = filtered["event_id"].apply(
            lambda x: f"{event_label_prefix}{x}" if pd.notna(x) else None
        )

    return filtered


def before_anchors(
    df: pd.DataFrame,
    anchors,
    *,
    before: timedelta,
    col: str = DEFAULT_TS_COL,
    add_event_id: bool = False,
    ts_key: str = "ts",
) -> pd.DataFrame:
    df = ensure_utc_df_strict(df, col)
    ts = df[col]

    events = _normalize_events(anchors, ts_key=ts_key)
    windows = make_before_windows_from_events(events, before)

    mask = pd.Series(False, index=df.index)
    event_ids: list[int | None] = [None] * len(df)

    for idx, (start, end) in enumerate(windows):
        wmask = (ts >= start) & (ts <= end)
        if add_event_id:
            newly = wmask & (~mask)
            if newly.any():
                for i in newly[newly].index:
                    event_ids[i] = idx
        mask = mask | wmask

    filtered = df[mask]
    if add_event_id:
        event_series = pd.Series(event_ids, index=df.index, dtype="Int64").loc[filtered.index]
        filtered = filtered.copy()
        filtered["event_id"] = event_series
    return filtered


def after_anchors(
    df: pd.DataFrame,
    anchors,
    *,
    after: timedelta,
    col: str = DEFAULT_TS_COL,
    add_event_id: bool = False,
    ts_key: str = "ts",
) -> pd.DataFrame:
    df = ensure_utc_df_strict(df, col)
    ts = df[col]

    events = _normalize_events(anchors, ts_key=ts_key)
    windows = make_after_windows_from_events(events, after)

    mask = pd.Series(False, index=df.index)
    event_ids: list[int | None] = [None] * len(df)

    for idx, (start, end) in enumerate(windows):
        wmask = (ts >= start) & (ts <= end)
        if add_event_id:
            newly = wmask & (~mask)
            if newly.any():
                for i in newly[newly].index:
                    event_ids[i] = idx
        mask = mask | wmask

    filtered = df[mask]
    if add_event_id:
        event_series = pd.Series(event_ids, index=df.index, dtype="Int64").loc[filtered.index]
        filtered = filtered.copy()
        filtered["event_id"] = event_series
    return filtered
