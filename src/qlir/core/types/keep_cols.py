from __future__ import annotations

from typing import Iterable
import pandas as pd
from enum import Enum, auto

class KeepCols(Enum):
    FINAL = auto()
    ALL = auto()


def apply_keep_policy(
    df: pd.DataFrame,
    *,
    keep: KeepCols | Iterable[str],
    final_col: str,
    candidate_cols: Iterable[str],
    inplace: bool = True,
) -> pd.DataFrame:
    """
    Apply a column keep policy and drop unneeded intermediate columns.

    Parameters
    ----------
    df
        DataFrame to operate on.
    keep
        KeepCols policy or explicit list of columns to keep.
    final_col
        Name of the final output column.
    candidate_cols
        Columns eligible for dropping (typically intermediates).
    inplace
        Whether to mutate df in place.

    Returns
    -------
    pd.DataFrame
        The modified DataFrame.
    """
    if not inplace:
        df = df.copy()

    if final_col in candidate_cols:
        raise ValueError(
        f"final_col '{final_col}' must not appear in candidate_cols "
        f"(got {candidate_cols})"
        )
    
    if isinstance(keep, KeepCols):
        if keep is KeepCols.FINAL:
            cols_to_keep = {final_col}
        elif keep is KeepCols.ALL:
            cols_to_keep = set(df.columns)
        else:
            raise AssertionError(f"Unhandled KeepCols: {keep}")
    else:
        cols_to_keep = set(keep)

    to_drop = [c for c in candidate_cols if c not in cols_to_keep]

    if to_drop:
        df.drop(columns=to_drop, inplace=True)

    return df
