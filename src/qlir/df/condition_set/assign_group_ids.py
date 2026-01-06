import pandas as _pd
import logging
log = logging.getLogger(__name__)

def assign_condition_group_id(
    df: _pd.DataFrame,
    *,
    condition_col: str,
    group_col: str = "condition_group_id",
) -> tuple[_pd.DataFrame, str]:
    """
    Assign monotonically increasing IDs to contiguous True runs.
    """
    cond = df[condition_col].astype("boolean")
    
    na_count = df[condition_col].isna().sum()
    if na_count:
        log.info(
            "assign_condition_group_id: filling %d NA values in column '%s' "
            "(expected; NA interferes with contiguous True-run detection).",
            na_count,
            condition_col,
        )

    # IMPORTANT: treat NA as False for boundary detection
    cond_filled = cond.fillna(False)

    # True where a new True-run starts
    starts = cond_filled & ~cond_filled.shift(fill_value=False)

    # Cumulative sum gives run numbers
    group_ids = starts.cumsum()

    # Mask False rows
    df[group_col] = group_ids.where(cond)

    return df, group_col
