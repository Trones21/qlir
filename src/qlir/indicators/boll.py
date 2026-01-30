from __future__ import annotations

import numpy as _np
import pandas as _pd

from qlir.core.registries.columns.announce_and_register import announce_column_lifecycle
from qlir.core.registries.columns.registry import ColKeyDecl, ColRegistry
from qlir.core.types.annotated_df import AnnotatedDF
from qlir.df.utils import _ensure_columns

__all__ = ["with_bollinger"]


def with_bollinger(
    df: _pd.DataFrame,
    *,
    close_col: str = "close",
    period: int = 20,
    k: float = 2.0,
    out_mid: str = "boll_mid",
    out_upper: str = "boll_upper",
    out_lower: str = "boll_lower",
    out_valid: str | None = "boll_valid",
    in_place: bool = True,
) -> AnnotatedDF:
    """
    Adds Bollinger Bands to a DataFrame.

    Parameters
    ----------
    df : _pd.DataFrame
        Must contain a `close_col` column.
    close_col : str
        Column name containing closing prices.
    period : int
        Rolling window length for mean and std.
    k : float
        Number of standard deviations for the band width.
    out_mid/out_upper/out_lower : str
        Output column names.
    out_valid : str | None
        Optional validity flag column. If None, no flag is added.
    in_place : bool
        If True, modifies df directly; otherwise returns a copy.
    """
    _ensure_columns(df=df, cols=close_col, caller="boll")

    out = df if in_place else df.copy()
    close = out[close_col].astype(float)

    # --- core math ---
    mid = close.rolling(window=period, min_periods=period//2).mean()
    sd = close.rolling(window=period, min_periods=period//2).std(ddof=0)

    out[out_mid] = mid
    out[out_upper] = mid + k * sd
    out[out_lower] = mid - k * sd

    # --- optional validity flag ---
    if out_valid:
        # Strictly valid only after `period - 1` rows
        out[out_valid] = _np.arange(len(out)) >= (period - 1)

    new_cols = ColRegistry()
    announce_column_lifecycle(caller="with_bollinger", registry=new_cols, 
        decls=[
            ColKeyDecl(key="out_lower", column=out_lower), 
            ColKeyDecl(key="out_mid", column=out_mid), 
            ColKeyDecl(key="out_upper", column=out_upper), 
        ], 
        event="created")

    return AnnotatedDF(df=out, new_cols=new_cols, label="with_bollinger")
