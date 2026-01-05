# qlir/df/row_aligned/abs_to_unit.py

import pandas as _pd
from qlir.core.types.UnitEnum import UnitEnum
from qlir.df.scalars.units import abs_to_bps, abs_to_pct


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

    df = df.copy()
    out_col = out_col or f"{value_col}_{unit.value}"

    if unit == UnitEnum.BPS:
        df[out_col] = abs_to_bps(df[value_col], df[ref_col])

    elif unit == UnitEnum.PCT:
        df[out_col] = abs_to_pct(df[value_col], df[ref_col])

    else:
        raise ValueError(f"Unsupported unit: {unit}")

    return df
