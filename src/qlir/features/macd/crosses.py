from __future__ import annotations

import pandas as _pd

from qlir.core.registries.columns.announce_and_register import announce_column_lifecycle
from qlir.core.registries.columns.registry import ColKeyDecl, ColRegistry
from qlir.core.types.annotated_df import AnnotatedDF
from qlir.df.utils import _ensure_columns
from qlir.perf.df_copy import df_copy_measured
from qlir.perf.logging import log_memory_debug

import logging
log = logging.getLogger(__name__)

__all__ = ["with_macd_cross_flags"]


def with_macd_cross_flags(
    df: _pd.DataFrame,
    *,
    macd_col: str = "macd",
    signal_col: str = "macd_signal",
    prefix: str = "macd",
) -> AnnotatedDF:
    """
    Adds MACD histogram sign and cross flags.

    Columns created:
      {prefix}_hist
      {prefix}_hist_positive
      {prefix}_hist_negative
      {prefix}_cross_up
      {prefix}_cross_down
    """
    _ensure_columns(
        df=df,
        cols=[macd_col, signal_col],
        caller="with_macd_cross_flags",
    )

    new_cols = ColRegistry()

    out, ev = df_copy_measured(df=df, label="with_macd_cross_flags")
    log_memory_debug(ev=ev, log=log)

    hist_col = f"{prefix}_hist"
    cross_up_col = f"{prefix}_cross_up"
    cross_down_col = f"{prefix}_cross_down"
    pos_col = f"{prefix}_hist_positive"
    neg_col = f"{prefix}_hist_negative"

    # histogram (signed)
    out[hist_col] = out[macd_col] - out[signal_col]
    prev = out[hist_col].shift(1)

    # crosses
    out[cross_up_col] = ((out[hist_col] > 0) & (prev <= 0)).astype("int8")
    out[cross_down_col] = ((out[hist_col] < 0) & (prev >= 0)).astype("int8")

    # sign flags
    out[pos_col] = (out[hist_col] > 0).astype("int8")
    out[neg_col] = (out[hist_col] < 0).astype("int8")

    announce_column_lifecycle(
        caller="with_macd_cross_flags",
        registry=new_cols,
        decls=[
            ColKeyDecl(key="hist", column=hist_col),
            ColKeyDecl(key="hist_positive", column=pos_col),
            ColKeyDecl(key="hist_negative", column=neg_col),
            ColKeyDecl(key="cross_up", column=cross_up_col),
            ColKeyDecl(key="cross_down", column=cross_down_col),
        ],
        event="created",
    )

    return AnnotatedDF(
        df=out,
        new_cols=new_cols,
        label="with_macd_cross_flags",
    )
