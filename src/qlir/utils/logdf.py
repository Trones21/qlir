import logging
import pandas as pd

try:
    from tabulate import tabulate
    _HAS_TABULATE = True
except ImportError:
    _HAS_TABULATE = False


log = logging.getLogger(__name__)


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

def ensure_logging():
    import logging
    logger = logging.getLogger()
    if not logger.hasHandlers():
        logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
        print("[init] Logging configured (default INFO)")

def logdf(
    df: pd.DataFrame,
    rows: int = 10,
    level: str = "info",
    name: str | None = None,
    max_width: int = 120,
):
    """
    Log or print a DataFrame with aligned columns, truncated to fit max_width.
    Falls back to print() if logging is not configured.
    
    Parameters
    ----------
    df : pd.DataFrame
        DataFrame to display.
    rows : int
        Number of rows to include (default 10).
    level : str
        Logging level ("info", "debug", etc.).
    name : str, optional
        Optional name/label shown before the table.
    max_width : int
        Max character width before truncating columns.
    """
    ensure_logging()
    logger = logging.getLogger()
    emit = getattr(logger, level, logger.info)

    if df is None or df.empty:
        emit(f"{name or 'DataFrame'} is empty.")
        return

    header = f"\n📊 {name or 'DataFrame'} (shape={df.shape}):"
    table = _fmt_df(df, rows=rows, max_width=max_width)
    emit(f"{header}\n{table}")
