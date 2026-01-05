import pandas as _pd


def assign_condition_group_id(
    df: _pd.DataFrame,
    *,
    condition_col: str,
    group_col: str = "condition_group_id",
) -> _pd.DataFrame:
    """
    Assign monotonically increasing IDs to contiguous True runs.

    False rows get NaN.
    """
    cond = df[condition_col].astype(bool)

    # True where a new True-run starts
    starts = cond & ~cond.shift(fill_value=False)

    # Cumulative sum gives run numbers
    group_ids = starts.cumsum()

    # Mask False rows
    df[group_col] = group_ids.where(cond)

    return df
