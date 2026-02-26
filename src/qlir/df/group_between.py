import pandas as pd


def mark_id_between(
    df: pd.DataFrame,
    *,
    start_col: str,
    start_col_value,
    end_col: str,
    end_col_value,
    inclusive_start: bool,
    inclusive_end: bool,
) -> pd.Series:
    """
    Assign a single-valued segment id to rows between start/end events.

    Output is a *partition* of rows into segments:
      - each row belongs to at most one segment id
      - NaN outside segments

    Safety:
      - Raises if both inclusive_start and inclusive_end are True, because that can
        imply boundary overlap (a row belonging to two segments), which cannot be
        represented in a single id column.
      - Raises if a row is both a start and end event.

    Returns
    -------
    pd.Series (float, with NaN outside) or Int64 if you want to enforce integer NA dtype.
    """

    if inclusive_start and inclusive_end:
        raise ValueError(
            "mark_id_between: inclusive_start=True and inclusive_end=True is not allowed "
            "for partitioned group ids (boundary overlap possible)."
        )

    if start_col not in df.columns:
        raise KeyError(start_col)
    if end_col not in df.columns:
        raise KeyError(end_col)

    start_event = df[start_col] == start_col_value
    end_event = df[end_col] == end_col_value

    both = start_event & end_event
    if bool(both.any()):
        idx = df.index[both][0]
        raise AssertionError(f"Row has both start and end event at index {idx!r}")

    # Segment number ticks up on every start
    seg_num = start_event.cumsum()

    # Build state from last boundary (+1 start, -1 end)
    event = pd.Series(pd.NA, index=df.index, dtype="Int8")
    event.loc[start_event] = 1
    event.loc[end_event] = -1
    state = event.ffill()

    active = (state == 1).fillna(False)

    # Apply inclusivity
    mask = active
    if not inclusive_start:
        mask &= ~start_event
    # inclusive_end is False here (since both-True forbidden); keep it explicit:
    if inclusive_end:
        mask |= end_event
    else:
        mask &= ~end_event

    # Id inside mask, NaN outside
    out = seg_num.where(mask)

    # If you prefer nullable int ids:
    # return out.astype("Int64")
    return outimport pandas as pd


def mark_id_between(
    df: pd.DataFrame,
    *,
    start_col: str,
    start_col_value,
    end_col: str,
    end_col_value,
    inclusive_start: bool,
    inclusive_end: bool,
) -> pd.Series:
    """
    Assign a single-valued segment id to rows between start/end events.

    Output is a *partition* of rows into segments:
      - each row belongs to at most one segment id
      - NaN outside segments

    Safety:
      - Raises if both inclusive_start and inclusive_end are True, because that can
        imply boundary overlap (a row belonging to two segments), which cannot be
        represented in a single id column.
      - Raises if a row is both a start and end event.

    Returns
    -------
    pd.Series (float, with NaN outside) or Int64 if you want to enforce integer NA dtype.
    """

    if inclusive_start and inclusive_end:
        raise ValueError(
            "mark_id_between: inclusive_start=True and inclusive_end=True is not allowed "
            "for partitioned group ids (boundary overlap possible)."
        )

    if start_col not in df.columns:
        raise KeyError(start_col)
    if end_col not in df.columns:
        raise KeyError(end_col)

    start_event = df[start_col] == start_col_value
    end_event = df[end_col] == end_col_value

    both = start_event & end_event
    if bool(both.any()):
        idx = df.index[both][0]
        raise AssertionError(f"Row has both start and end event at index {idx!r}")

    # Segment number ticks up on every start
    seg_num = start_event.cumsum()

    # Build state from last boundary (+1 start, -1 end)
    event = pd.Series(pd.NA, index=df.index, dtype="Int8")
    event.loc[start_event] = 1
    event.loc[end_event] = -1
    state = event.ffill()

    active = (state == 1).fillna(False)

    # Apply inclusivity
    mask = active
    if not inclusive_start:
        mask &= ~start_event
    # inclusive_end is False here (since both-True forbidden); keep it explicit:
    if inclusive_end:
        mask |= end_event
    else:
        mask &= ~end_event

    # Id inside mask, NaN outside
    out = seg_num.where(mask)

    # If you prefer nullable int ids:
    # return out.astype("Int64")
    return out