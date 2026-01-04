from dataclasses import dataclass
import logging
from typing import Iterable, Sequence, Union
import pandas as pd

try:
    from tabulate import tabulate
    _HAS_TABULATE = True
except ImportError:
    _HAS_TABULATE = False


log = logging.getLogger(__name__)



def _filter_df_by_row_idx(
    df: pd.DataFrame,
    start_incl: int,
    end_excl: int,
) -> pd.DataFrame:
    """
    Slice a DataFrame by *positional* row indices, safely.

    Rules:
      - Negative indices behave like Python slicing.
      - Out-of-bounds indices are clamped to [0, len(df)].
      - If start >= end, return an empty DataFrame.
      - Original index is preserved (no reset_index).

    Parameters
    ----------
    df : pd.DataFrame
        The dataframe to slice.
    start_incl : int
        Start index, inclusive.
    end_excl : int
        End index, exclusive.

    Returns
    -------
    pd.DataFrame
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



def _fmt_df(df: pd.DataFrame, max_width: int = 120) -> str:
    """
    Formats a DataFrame into a human-readable table that fits within max_width.
    Falls back to df.to_string() if tabulate isn't available.
    """
    df_copy = df.copy()

    # Detect and truncate overly wide columns
    for col in df_copy.columns:
        df_copy[col] = df_copy[col].astype(str)
        max_len = df_copy[col].str.len().max()
        if max_len > max_width // len(df_copy.columns):
            cutoff = max_width // len(df_copy.columns) - 3
            df_copy[col] = df_copy[col].str.slice(0, cutoff) + "…"

    if _HAS_TABULATE:
        return tabulate(df_copy, headers="keys", tablefmt="github", showindex=False)
    else:
        return df_copy.to_string(index=False)


def ensure_logging() -> None:
    """
    Ensure there is *some* logging configuration so logdf
    doesn't silently drop messages in ad-hoc scripts / notebooks.

    Priority:
      1. If the qlir logger has handlers, trust that setup.
      2. Else if root has handlers, trust that.
      3. Else configure a simple root handler at INFO.
    """
    root = logging.getLogger()
    qlir_logger = logging.getLogger("qlir")

    # If either qlir or root already has handlers, don't touch config.
    if qlir_logger.hasHandlers() or root.hasHandlers():
        return

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    print("[init] Logging configured (default INFO)")


@dataclass(frozen=True)
class NamedDF:
    df: pd.DataFrame
    name: str


LogDFInput = Union[
    pd.DataFrame,
    Iterable[pd.DataFrame],
    Iterable[NamedDF],
]

def logdf(
    data: LogDFInput,
    *,
    from_row_idx: int = 0,
    max_rows: int = 10,
    level: str = "info",
    max_width: int = 200,
    cols_filter_all_dfs: Sequence[str] | None = None,
    cols_filter_by_df_idx: dict[int, Sequence[str] | None] | None = None,
) -> None:
    ensure_logging()

    logger = logging.getLogger("qlir.logdf")

    # Map level string → log method with a safe default.
    level_str = (level or "info").lower()
    emit = getattr(logger, level_str, logger.info)

    def _log_one(df: pd.DataFrame, 
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
        table = _fmt_df(filtered, max_width=max_width)

        footer = ""
        if len(filtered) < len(df):
            footer = f"\n… showing rows {from_row_idx}:{min(excl_idx, len(df))} of {len(df)}"

        emit(f"\n{header}\n{table}{footer}\n")

    # ---- Single DataFrame path (index = 0) ----
    if isinstance(data, pd.DataFrame):
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
        elif isinstance(item, pd.DataFrame):
            _log_one(item, None, effective_cols)
        else:
            raise TypeError(
                f"logdf() received unsupported item type: {type(item)!r}"
            )
        







# def logdf(
#     df: pd.DataFrame,
#     from_row_idx: int = 0,
#     max_rows: int = 10,
#     level: str = "info",
#     name: str | None = None,
#     max_width: int = 200,
# ) -> None:
#     """
#     Log a DataFrame via the qlir logger tree.

#     Behavior
#     --------
#       - Messages go to `qlir.logdf` so they use the same handlers/levels
#         configured by setup_logging(LogProfile.*).
#       - If no logging is configured at all, ensure_logging() installs a basic
#         root handler so output is still visible.

#     Parameters
#     ----------
#     df : pd.DataFrame
#         DataFrame to display.
#     from_row_idx : int
#         Zero-based starting row index (inclusive) in positional terms.
#     max_rows : int
#         Maximum number of rows to include (default 10).
#     level : str
#         Logging level ("info", "debug", etc.).
#     name : str, optional
#         Optional name/label shown before the table.
#     max_width : int
#         Max character width before truncating columns.
#     """
#     ensure_logging()

#     # Use a child logger under the qlir namespace so it picks up qlir handlers.
#     logger = logging.getLogger("qlir.logdf")

#     # Map level string → log method with a safe default.
#     level_str = (level or "info").lower()
#     emit = getattr(logger, level_str, logger.info)

#     if df is None or df.empty:
#         emit(f"{name or 'DataFrame'} is empty.")
#         return

#     header = f"\n📊 {name or 'DataFrame'} (shape={df.shape}):"
#     excl_idx = from_row_idx + max_rows
#     filtered_df = _filter_df_by_row_idx(df, start_incl=from_row_idx, end_excl=excl_idx) # converts to using row indices if the dataframe has an index of a different type
#     table = _fmt_df(filtered_df, max_width=max_width)
#     emit(f"{header}\n{table}")
