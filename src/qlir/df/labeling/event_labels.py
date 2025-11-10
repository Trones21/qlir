from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Sequence, Mapping

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


def mark_around_events(
    df: pd.DataFrame,
    events,
    *,
    before: timedelta,
    after: timedelta,
    col: str = DEFAULT_TS_COL,
    out_col: str = "in_event_window",
    add_event_id: bool = True,
) -> pd.DataFrame:
    """
    Add a boolean column marking rows within any event window.
    Optionally add event_id to say *which* event window matched first.
    """
    df = ensure_utc_df_strict(df, col)
    ts = df[col]

    evs = _normalize_events(events)
    out = df.copy()
    out[out_col] = False
    if add_event_id:
        out["event_id"] = pd.Series([None] * len(out), index=out.index, dtype="Int64")

    for idx, e in enumerate(evs):
        start = e["ts"] - before
        end = e["ts"] + after
        mask = (ts >= start) & (ts <= end)

        # set bool
        out.loc[mask, out_col] = True

        # set event_id only if empty
        if add_event_id:
            empty = out["event_id"].isna() & mask
            out.loc[empty, "event_id"] = idx

    return out
