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
    rows: int = 10,
    level: str = "info",
    name: str | None = None,
    max_width: int = 200,
) -> None:
    """
    Log a DataFrame via the qlir logger tree.

    Behavior:
      - Messages go to `qlir.logdf` so they use the same handlers/levels
        configured by setup_logging(LogProfile.*).
      - If no logging is configured at all, ensure_logging() installs a basic
        root handler so output is still visible.

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

    # Use a child logger under the qlir namespace so it picks up qlir handlers.
    logger = logging.getLogger("qlir.logdf")

    # Map level string → log method with a safe default.
    level_str = (level or "info").lower()
    emit = getattr(logger, level_str, logger.info)

    if df is None or df.empty:
        emit(f"{name or 'DataFrame'} is empty.")
        return

    header = f"\n📊 {name or 'DataFrame'} (shape={df.shape}):"
    table = _fmt_df(df, rows=rows, max_width=max_width)
    emit(f"{header}\n{table}")
