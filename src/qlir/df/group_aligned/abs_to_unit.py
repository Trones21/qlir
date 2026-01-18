# qlir/df/group_aligned/abs_to_unit.py

import pandas as _pd

from qlir.core.types.UnitEnum import UnitEnum
from qlir.df.scalars.units import delta_in_bps, delta_in_pct


def abs_to_unit(
    df: _pd.DataFrame,
    *,
    value_col: str,
    ref_col: str,
    unit: UnitEnum,
    out_col: str | None = None,
) -> _pd.DataFrame:
    """
    Group-aligned normalization.
    All rows are normalized against the first row of the group.
    """

    if df.empty:
        return df

    df = df.copy()
    out_col = out_col or f"{value_col}_{unit.value}"

    ref_price = df.iloc[0][ref_col]

    if unit == UnitEnum.BPS:
        df[out_col] = delta_in_bps(df[value_col], ref_price)

    elif unit == UnitEnum.PCT:
        df[out_col] = delta_in_pct(df[value_col], ref_price)

    else:
        raise ValueError(f"Unsupported unit: {unit}")

    return df
