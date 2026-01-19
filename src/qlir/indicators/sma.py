import logging

import pandas as _pd

from qlir.core.registries.columns.registry import ColRegistry
from qlir.core.types.annotated_df import AnnotatedDF
from qlir.df.utils import _ensure_columns

log = logging.getLogger(__name__)



from qlir.core.semantics.decorators import new_col_func
from qlir.core.semantics.row_derivation import ColumnDerivationSpec


@new_col_func(
    specs=lambda *, col, window, **_: ColumnDerivationSpec(
        op="sma",
        base_cols=(col,),
        read_rows=(-(window - 1), 0),
        scope="output",
        self_inclusive=True,
        log_suffix="sma log suffix test"
    )
)
def sma(
    df: _pd.DataFrame,
    *,
    col: str,
    window: int,
    new_col_name: str | None = None,
    prefix_2_default_col_name: str | None = None,
    min_periods: int | None = None,
    decimals: int | None = None,
    in_place: bool = True,
) -> AnnotatedDF:
    """
    Compute a simple moving average (SMA) for a column.

    Notes
    -----
    - Optional rounding (`decimals`) is applied AFTER rolling mean
      to control floating-point noise for downstream transforms.
    """
    _ensure_columns(df=df, cols=col, caller="sma")

    out = df if in_place else df.copy()

    name = (
        new_col_name
        if new_col_name
        else f"{prefix_2_default_col_name + '_' if prefix_2_default_col_name else ''}{col}_sma_{window}"
    )

    s = (
        out[col]
        .rolling(window=window, min_periods=min_periods or window)
        .mean()
    )

    if decimals is not None:
        s = s.round(decimals)

    out[name] = s
    new_col = ColRegistry()
    new_col.add(key="sma_col", column=name)

    return AnnotatedDF(df=df, new_cols=new_col)
