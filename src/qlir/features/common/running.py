from __future__ import annotations
import numpy as _np
import pandas as _pd

__all__ = ["with_counts_running", "with_streaks"]


def with_counts_running(
    df: _pd.DataFrame,
    *,
    session_col: str | None = "session",
    rel_col: str = "relation",
    out_prefix: str = "count_",
) -> _pd.DataFrame:
    out = df.copy()
    if session_col:
        groups = out[session_col]
    else:
        groups = _pd.Series(0, index=out.index)

    for key in ("above", "below", "touch"):
        mask = out[rel_col].eq(key).astype("int8")
        out[f"{out_prefix}{key}"] = mask.groupby(groups).cumsum().astype("int32")
    return out


def with_streaks(
    df: _pd.DataFrame,
    *,
    rel_col: str = "relation",
    session_col: str | None = "session",
    out_id: str = "streak_id",
    out_len: str = "streak_len",
) -> _pd.DataFrame:
    out = df.copy()
    boundary_change = out[rel_col].ne(out[rel_col].shift(1))
    if session_col:
        boundary_change |= out[session_col].ne(out[session_col].shift(1))
    out[out_id] = boundary_change.cumsum()
    out[out_len] = out.groupby(out_id, sort=False).cumcount().add(1).astype("int32")
    return out