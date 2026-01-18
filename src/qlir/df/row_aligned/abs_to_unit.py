# qlir/df/row_aligned/abs_to_unit.py

import pandas as _pd

from qlir.core.types.UnitEnum import UnitEnum
from qlir.df.scalars.units import delta_in_bps, delta_in_pct
from qlir.df.utils import _ensure_columns


def abs_to_unit(
    df: _pd.DataFrame,
    *,
    value_col: str,
    ref_col: str,
    unit: UnitEnum,
    out_col: str | None = None,
) -> _pd.DataFrame:
    """
    Row-aligned normalization.
    Each row uses its own reference value.
    """
    _ensure_columns(df=df, cols=[value_col, ref_col], caller="abs_to_unit")
    df = df.copy()
    out_col = out_col or f"{value_col}_{unit.value}"

    if unit == UnitEnum.BPS:
        df[out_col] = delta_in_bps(df[value_col], df[ref_col])

    elif unit == UnitEnum.PCT:
        df[out_col] = delta_in_pct(df[value_col], df[ref_col])

    else:
        raise ValueError(f"Unsupported unit: {unit}")

    return df
