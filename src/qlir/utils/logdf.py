import logging
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



def _fmt_df(df: pd.DataFrame, rows: int = 10, max_width: int = 120) -> str:
    """
    Formats a DataFrame into a human-readable table that fits within max_width.
    Falls back to df.to_string() if tabulate isn't available.
    """
    df_head = df.head(rows).copy()

    # Detect and truncate overly wide columns
    for col in df_head.columns:
        df_head[col] = df_head[col].astype(str)
        max_len = df_head[col].str.len().max()
        if max_len > max_width // len(df_head.columns):
            cutoff = max_width // len(df_head.columns) - 3
            df_head[col] = df_head[col].str.slice(0, cutoff) + "…"

    if _HAS_TABULATE:
        return tabulate(df_head, headers="keys", tablefmt="github", showindex=False)
    else:
        return df_head.to_string(index=False)


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


def logdf(
    df: pd.DataFrame,
    from_row_idx: int = 0,
    max_rows: int = 10,
    level: str = "info",
    name: str | None = None,
    max_width: int = 200,
) -> None:
    """
    Log a DataFrame via the qlir logger tree.

    Behavior
    --------
      - Messages go to `qlir.logdf` so they use the same handlers/levels
        configured by setup_logging(LogProfile.*).
      - If no logging is configured at all, ensure_logging() installs a basic
        root handler so output is still visible.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame to display.
    from_row_idx : int
        Zero-based starting row index (inclusive) in positional terms.
    max_rows : int
        Maximum number of rows to include (default 10).
    level : str
        Logging level ("info", "debug", etc.).
    name : str, optional
        Optional name/label shown before the table.
    max_width : int
        Max character width before truncating columns.
    """
    ensure_logging()

    # Use a child logger under the qlir namespace so it picks up qlir handlers.
    logger = logging.getLogger("qlir.logdf")

    # Map level string → log method with a safe default.
    level_str = (level or "info").lower()
    emit = getattr(logger, level_str, logger.info)

    if df is None or df.empty:
        emit(f"{name or 'DataFrame'} is empty.")
        return

    header = f"\n📊 {name or 'DataFrame'} (shape={df.shape}):"
    excl_idx = from_row_idx + max_rows
    filtered_df = _filter_df_by_row_idx(df, start_incl=from_row_idx, end_excl=excl_idx) # converts to using row indices if the dataframe has an index of a different type
    table = _fmt_df(filtered_df, max_width=max_width)
    emit(f"{header}\n{table}")
