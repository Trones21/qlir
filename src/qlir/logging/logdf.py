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


def _warn_invalid_columns(
    *,
    df: _pd.DataFrame,
    cols: Sequence[str],
    context: str,
    logger: logging.Logger,
) -> list[str]:
    """
    Warn about invalid column names and return only the valid ones.
    """
    if not cols:
        return []

    existing = set(df.columns)
    valid = []
    invalid = []

    for c in cols:
        if c in existing:
            valid.append(c)
        else:
            invalid.append(c)

    if invalid:
        logger.warning(
            "logdf(%s): ignoring %d invalid column(s): %s",
            context,
            len(invalid),
            invalid,
        )

    return valid


def _format_display_int(orig: _pd.Series, disp: _pd.Series) -> _pd.Series:
    """
    Render floats that are mathematically integral as integers.
    """
    mask = (
        orig.notna()
        & _pd.api.types.is_numeric_dtype(orig)
        & (orig % 1 == 0)
    )
    return disp.where(~mask, orig[mask].astype("Int64").astype(str))



def _fmt_df(df: _pd.DataFrame, max_width: int = 120, fmt_bool_cols: bool = False, na_as_empty: bool | Sequence[str] = False, display_as_int: bool | Sequence[str] = False) -> str:
    """
    Formats a DataFrame into a human-readable table that fits within max_width.
    Falls back to df.to_string() if tabulate isn't available.
    """
    df_copy = df.copy()

    # Normalize NA policy
    if na_as_empty is True:
        na_cols = set(df_copy.columns)
    elif na_as_empty:
        na_cols = set(
        _warn_invalid_columns(
            df=df,
            cols=na_as_empty,
            context="na_as_empty",
            logger=logging.getLogger("qlir.logdf"),
            )
        )
    else:
        na_cols = set()

    # Normalize display_as_int
    if display_as_int is True:
        int_cols = set(df.columns)
    elif display_as_int:
        int_cols = set(display_as_int)
    else:
        int_cols = set()


    for col in df_copy.columns:

        orig = df[col]

        # ---- Bools Fmt / base display ----
        if fmt_bool_cols and _pd.api.types.is_bool_dtype(orig):
            disp = orig.map(lambda v: "True" if bool(v) else "")
        else:
            disp = orig.astype(str)

        # ---- NA display -------
        if col in na_cols:
            disp = disp.mask(orig.isna(), "")

        # ---- DISPLAY AS INT ----
        if col in int_cols:
            disp = _format_display_int(orig, disp)

        # ---- Width / truncation (DISPLAY ONLY) ----
        max_len = disp.str.len().max()
        per_col_width = max(1, max_width // max(1, len(df_copy.columns)))

        if max_len > per_col_width:
            cutoff = per_col_width - 3
            disp = disp.str.slice(0, cutoff) + "…"

        # ---- Commit ----
        df_copy[col] = disp

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
    na_as_empty: bool | Sequence[str] = False,
    display_as_int: bool | Sequence[str] = False,

) -> None:
    """
    Log a DataFrame (or collection of DataFrames) in a compact, human-readable
    tabular form for *intra-analysis inspection*.

    `logdf` is intentionally a **view-layer inspection tool**, not a general-purpose
    DataFrame printer. It is designed to support two primary usage patterns:

    -------------------------------------------------------------------------
    1) Ad-hoc / exploratory usage (during development)
    -------------------------------------------------------------------------

    Used inline while building or debugging a pipeline, typically with minimal
    arguments:

        logdf(df)
        logdf(named_df)
        logdf([df1, df2])

    In this mode, `logdf` acts as a lightweight visual sanity check:
    - No mutation of data
    - No assumptions about semantics
    - Defaults favor compactness and safety

    -------------------------------------------------------------------------
    2) Embedded inspection in column-bundle / analysis operations
    -------------------------------------------------------------------------

    Used as a *permanent, intentional inspection point* inside higher-level
    operations (e.g. column bundles, studies, or semantic transforms). These calls
    are meant to function as a “security blanket” — a quick visual confirmation
    that a specific stage of the pipeline is behaving as expected.

    In this mode, `logdf` is typically called with many arguments, and column
    selection is usually driven by a `ColRegistry` (or `AnnotatedDF.new_cols`)
    rather than hard-coded column names.

    Example:

        view_cols = adf_mae_up.new_cols.get_columns([
            "intra_leg_idx",
            "group_first_open",
            "leg_max_idx",
            "is_excursion_row",
            "excursion_bps",
        ])

        na_cols = adf_mae_up.new_cols.get_columns([
            "intra_leg_idx",
            "leg_max_idx",
        ])

        logdf(
            adf_mae_up.df,
            from_row_idx=30,
            max_rows=30,
            cols_filter_all_dfs=["open", *view_cols],
            fmt_bool_cols=True,
            display_as_int=True,
            na_as_empty=na_cols,
        )

    In this style:
    - Column names are accessed **by semantic key**, not by concrete string
    - Formatting options are chosen to reduce visual noise
    - The call site encodes the intent of the inspection

    -------------------------------------------------------------------------
    Design principles
    -------------------------------------------------------------------------

    - **View-only**: `logdf` never mutates the underlying DataFrame.
    - **Semantic-first**: All formatting decisions are driven by the original
      column values, not by previously formatted output.
    - **Composable formatting**: Options like `fmt_bool_cols`, `na_as_empty`,
      and `display_as_int` operate purely at the display layer and may overlap
      safely on the same columns.
    - **Non-fatal diagnostics**: Invalid column names are logged as warnings
      and ignored, never raised.

    Parameters
    ----------
    data : LogDFInput
        A pandas DataFrame, NamedDF, or an iterable of either.

    from_row_idx : int, default 0
        Starting row index (after filtering) for display.

    max_rows : int, default 10
        Maximum number of rows to display per DataFrame.

    level : str, default "info"
        Logging level to emit at (e.g. "info", "debug", "warning").

    max_width : int, default 200
        Target maximum width for the rendered table (used for truncation).

    cols_filter_all_dfs : Sequence[str] | None
        Columns to display for all DataFrames. Invalid column names are logged
        and ignored.

    cols_filter_by_df_idx : dict[int, Sequence[str] | None] | None
        Per-DataFrame column filters when `data` is an iterable.

    fmt_bool_cols : bool, default False
        If True, boolean columns are rendered with only truthy markers
        (True → "True", False → empty).

    na_as_empty : bool | Sequence[str], default False
        Control how missing values are displayed.
        - False: show default missing markers ("nan", "<NA>")
        - True: render missing values as empty strings for all columns
        - Sequence[str]: apply only to specified columns

    display_as_int : bool | Sequence[str], default False
        Render numeric values that are mathematically integral without a decimal
        point (e.g. 22.0 → "22"), without changing underlying dtype or missingness.
        May be applied globally or per-column.

    Returns
    -------
    None
        This function emits log output only.
    """
    ensure_logging()

    logger = logging.getLogger("qlir.logdf")

    # Map level string → log method with a safe default.
    level_str = (level or "info").lower()
    emit = getattr(logger, level_str, logger.info)

    def _log_one(df: _pd.DataFrame,
                 idx: int, 
                 name: str | None,
                 effective_cols: Sequence[str] | None,):
        if df is None or df.empty:
            emit(f"{name or 'DataFrame'} is empty.")
            return

        view = df
        
        if effective_cols is not None:
            effective_cols = _warn_invalid_columns(
                df=df,
                cols=effective_cols,
                context=f"cols_filter (df_idx={idx})",
                logger=logger,
            )
            existing = [c for c in effective_cols if c in df.columns]
            if existing:
                view = df[existing]
        
        
        col_subset_info = ""
        if len(view.columns) != len(df.columns):
            col_subset_info = f"(Showing {len(view.columns)} of {len(df.columns)} columns)"
        header = f"\n📊 {name or 'DataFrame'} (original_shape={df.shape}) {col_subset_info}"
        excl_idx = from_row_idx + max_rows
        filtered = _filter_df_by_row_idx(view, from_row_idx, excl_idx)
        table = _fmt_df(filtered, max_width=max_width, fmt_bool_cols=fmt_bool_cols, na_as_empty=na_as_empty, display_as_int=display_as_int)

        footer = ""
        if len(filtered) < len(df):
            footer = f"\n… showing rows {from_row_idx}:{min(excl_idx, len(df))} of {len(df)}"

        emit(f"\n{header}\n{table}{footer}\n")

    # ---- Single DataFrame path (index = 0) ----
    if isinstance(data, NamedDF):
        effective_cols = cols_filter_all_dfs
        if cols_filter_by_df_idx and 0 in cols_filter_by_df_idx:
            effective_cols = cols_filter_by_df_idx[0]

        _log_one(df=data.df, idx=0, name=data.name, effective_cols=effective_cols)
        return
    
    if isinstance(data, _pd.DataFrame):
        effective_cols = cols_filter_all_dfs
        if cols_filter_by_df_idx and 0 in cols_filter_by_df_idx:
            effective_cols = cols_filter_by_df_idx[0]

        _log_one(df=data, idx=0, name=None, effective_cols=effective_cols)
        return
    
    if not isinstance(data, Iterable):
        raise TypeError(f"logdf() received unsupported type: {type(data)!r}")

    # ---- Iterable path ----
    for i, item in enumerate(data):
        effective_cols = cols_filter_all_dfs
        if cols_filter_by_df_idx and i in cols_filter_by_df_idx:
            effective_cols = cols_filter_by_df_idx[i]

        if isinstance(item, NamedDF):
            _log_one(df=item.df, idx=i, name=item.name, effective_cols=effective_cols)
        elif isinstance(item, _pd.DataFrame):
            _log_one(df=item, idx=i, name=None, effective_cols=effective_cols)
        else:
            raise TypeError(
                f"logdf() received unsupported item type: {type(item)!r}"
            )
        


