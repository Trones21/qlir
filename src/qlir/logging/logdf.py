import logging
from typing import Iterable, Sequence

import pandas as _pd

from qlir.core.types.named_df import NamedDF
from qlir.core.types.union import LogDFInput
from qlir.logging.ensure import ensure_logging

try:
    from tabulate import tabulate
    _HAS_TABULATE = True
except ImportError:
    _HAS_TABULATE = False


log = logging.getLogger(__name__)



def _filter_df_by_row_idx(
    df: _pd.DataFrame,
    start_incl: int,
    end_excl: int,
) -> _pd.DataFrame:
    """
    Slice a DataFrame by *positional* row indices, safely.

    Rules:
      - Negative indices behave like Python slicing.
      - Out-of-bounds indices are clamped to [0, len(df)].
      - If start >= end, return an empty DataFrame.
      - Original index is preserved (no reset_index).

    Parameters
    ----------
    df : _pd.DataFrame
        The dataframe to slice.
    start_incl : int
        Start index, inclusive.
    end_excl : int
        End index, exclusive.

    Returns
    -------
    _pd.DataFrame
        The sliced DataFrame (possibly empty).
    """
    if df is None or len(df) == 0:
        return df.iloc[0:0]

    n = len(df)

    # Normalize Python-style negative indices
    if start_incl < 0:
        start = max(0, n + start_incl)
    else:
        start = start_incl

    if end_excl < 0:
        end = max(0, n + end_excl)
    else:
        end = end_excl

    # Clamp to valid boundaries
    start = max(0, min(start, n))
    end   = max(0, min(end, n))

    # Empty slice if invalid range
    if start >= end:
        return df.iloc[0:0]

    # Use positional slicing
    return df.iloc[start:end]



def _fmt_df(df: _pd.DataFrame, max_width: int = 120, fmt_bool_cols: bool = False) -> str:
    """
    Formats a DataFrame into a human-readable table that fits within max_width.
    Falls back to df.to_string() if tabulate isn't available.
    """
    df_copy = df.copy()

    for col in df_copy.columns:
        s = df_copy[col]
        # ---- Boolean formatting (opt-in) ----
        if fmt_bool_cols and _pd.api.types.is_bool_dtype(s):
            df_copy[col] = s.map(lambda v: "True" if v is True else "")
            continue

        # ---- Default string conversion + truncation ----
        s_str = s.astype(str)
        max_len = s_str.str.len().max()

        if max_len > max_width // max(1, len(df_copy.columns)):
            cutoff = max_width // max(1, len(df_copy.columns)) - 3
            s_str = s_str.str.slice(0, cutoff) + "…"

        df_copy[col] = s_str

    if _HAS_TABULATE:
        return tabulate(df_copy, headers="keys", tablefmt="github", showindex=True)
    else:
        return df_copy.to_string(index=True)


def logdf(
    data: LogDFInput,
    *,
    from_row_idx: int = 0,
    max_rows: int = 10,
    level: str = "info",
    max_width: int = 200,
    cols_filter_all_dfs: Sequence[str] | None = None,
    cols_filter_by_df_idx: dict[int, Sequence[str] | None] | None = None,
    fmt_bool_cols: bool = False,
) -> None:
    ensure_logging()

    logger = logging.getLogger("qlir.logdf")

    # Map level string → log method with a safe default.
    level_str = (level or "info").lower()
    emit = getattr(logger, level_str, logger.info)

    def _log_one(df: _pd.DataFrame, 
                 name: str | None,
                 effective_cols: Sequence[str] | None,):
        if df is None or df.empty:
            emit(f"{name or 'DataFrame'} is empty.")
            return

        view = df
        
        if effective_cols is not None:
            existing = [c for c in effective_cols if c in df.columns]
            if existing:
                view = df[existing]
        
        
        col_subset_info = ""
        if len(view.columns) != len(df.columns):
            col_subset_info = f"(Showing {len(view.columns)} of {len(df.columns)} columns)"
        header = f"\n📊 {name or 'DataFrame'} (original_shape={df.shape}) {col_subset_info}"
        excl_idx = from_row_idx + max_rows
        filtered = _filter_df_by_row_idx(view, from_row_idx, excl_idx)
        table = _fmt_df(filtered, max_width=max_width, fmt_bool_cols=fmt_bool_cols)

        footer = ""
        if len(filtered) < len(df):
            footer = f"\n… showing rows {from_row_idx}:{min(excl_idx, len(df))} of {len(df)}"

        emit(f"\n{header}\n{table}{footer}\n")

    # ---- Single DataFrame path (index = 0) ----
    if isinstance(data, NamedDF):
        effective_cols = cols_filter_all_dfs
        if cols_filter_by_df_idx and 0 in cols_filter_by_df_idx:
            effective_cols = cols_filter_by_df_idx[0]

        _log_one(data.df, data.name, effective_cols)
        return
    
    if isinstance(data, _pd.DataFrame):
        effective_cols = cols_filter_all_dfs
        if cols_filter_by_df_idx and 0 in cols_filter_by_df_idx:
            effective_cols = cols_filter_by_df_idx[0]

        _log_one(data, None, effective_cols)
        return
    
    if not isinstance(data, Iterable):
        raise TypeError(f"logdf() received unsupported type: {type(data)!r}")

    # ---- Iterable path ----
    for i, item in enumerate(data):
        effective_cols = cols_filter_all_dfs
        if cols_filter_by_df_idx and i in cols_filter_by_df_idx:
            effective_cols = cols_filter_by_df_idx[i]

        if isinstance(item, NamedDF):
            _log_one(item.df, item.name, effective_cols)
        elif isinstance(item, _pd.DataFrame):
            _log_one(item, None, effective_cols)
        else:
            raise TypeError(
                f"logdf() received unsupported item type: {type(item)!r}"
            )
        
