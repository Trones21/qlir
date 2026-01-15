import logging
from typing import Iterable

import pandas as _pd

log = logging.getLogger(__name__)



def union_and_sort(dfs: list[_pd.DataFrame], sort_by: list[str] | None = None) -> _pd.DataFrame:
    """
    Union multiple DataFrames and return a sorted, deduplicated result.
    """
    df = _pd.concat(dfs, ignore_index=True)
    
    # Drop duplicates (optional, common for unions)
    df = df.drop_duplicates().reset_index(drop=True)
    
    # Sort if requested
    if sort_by:
        df = df.sort_values(by=sort_by).reset_index(drop=True)
    
    return df


def materialize_index(df, name: str = "tz_start") -> _pd.DataFrame:
    """
    Ensure the DataFrame's index is also available as a column,
    and move it to the first position (in-place).

    Parameters
    ----------
    df : _pd.DataFrame
        Target DataFrame with a DatetimeIndex or similar.
    name : str, default="tz_start"
        Name for the materialized index column.

    Notes
    -----
    - Operates in-place; returns None.
    - If the column already exists, it is simply moved to index 0.
    - Efficient: avoids DataFrame copies.
    """
    if df.index.name != name:
        df.index.name = name

    if name in df.columns:
        move_column(df, name, 0)
    else:
        df.insert(0, name, df.index)
    
    return df



def move_column(df, col: str, to_idx: int = 0) -> _pd.DataFrame:
    """
    Move an existing column to a new ordinal position
    Note: This is basically just an verbosity reducer and its efficent b/c 
    it does the operation in-place; theres no dataframe copy

    Parameters
    ----------
    df : _pd.DataFrame
        Target DataFrame.
    col : str
        Name of the column to move. Must already exist.
    to_idx : int, default=0
        New ordinal index (0-based). Negative values count from the end.

    Notes
    -----
    - Operates in-place; returns None.
    - Uses `pop()` + `insert()` for minimal overhead (no full copy).
    """
    if col not in df.columns:
        raise KeyError(col)

    s = df.pop(col)
    
    if to_idx == -1:
        to_idx = len(df.columns) - 1
    
    df.insert(to_idx, col, s)

    return df

def insert_column(df, col: str, values, to_idx: int = 0) -> _pd.DataFrame:
    """
    Insert a new column at a given ordinal position (in-place).
    
    Note: This is basically just an verbosity reducer and its efficent b/c 
    it does the operation in-place; theres no dataframe copy
    
    Parameters
    ----------
    df : _pd.DataFrame
        Target DataFrame.
    col : str
        Name of the new column.
    values : array-like
        Column values.
    to_idx : int, default=0
        Ordinal index to insert at (0-based).

    Notes
    -----
    - Operates in-place; returns None.
    - Existing column names will raise ValueError (pandas default).
    - Existing columns at or after this position are shifted right.
    """
    df.insert(to_idx, col, values)

    return df

def _ensure_columns(
    df: _pd.DataFrame,
    cols: str | Iterable[str],
    *,
    caller: str | None = None,
) -> None:
    """
    Ensure required columns exist in a DataFrame.

    Raises KeyError with detailed logging if any are missing.

    Internal Note: Pass the caller so end users have a better UX
    """

    if isinstance(cols, str):
        cols = [cols]

    missing = [c for c in cols if c not in df.columns]

    if not missing:
        return

    location = f" ({caller})" if caller else ""

    log.error(
        "Missing required column(s)%s: %s",
        location,
        ", ".join(missing),
    )
    log.info("Available columns (%d): %s", len(df.columns), list(df.columns))

    raise KeyError(f"Missing required columns{location}: {missing}")
