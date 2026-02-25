import pandas as pd
from pandas.api.types import is_datetime64_any_dtype

def groupby_time(
    df: pd.DataFrame,
    *,
    ts_col: str | pd.DatetimeIndex | None,
    freq: str,
):
    if ts_col is None:
        # Implicit DatetimeIndex
        if not isinstance(df.index, pd.DatetimeIndex):
            raise TypeError("DataFrame index is not a DatetimeIndex")
        return df.groupby(pd.Grouper(freq=freq), sort=False)

    if ts_col in df.columns:
        return df.groupby(pd.Grouper(key=ts_col, freq=freq), sort=False)

    if ts_col in df.index.names:
        return df.groupby(pd.Grouper(level=ts_col, freq=freq), sort=False)

    raise KeyError(
        f"Timestamp '{ts_col}' not found in columns or index levels"
    )


def _validate_ts_ref(df: pd.DataFrame, ts_col: str) -> None:
    # Case 1: timestamp is a column
    if ts_col in df.columns:
        if not is_datetime64_any_dtype(df[ts_col].dtype):
            raise TypeError(
                f"ts_col '{ts_col}' must be datetime-like, "
                f"got dtype {df[ts_col].dtype}"
            )
        return

    # Case 2: timestamp is index level
    if ts_col in df.index.names:
        level = df.index.names.index(ts_col)
        idx = df.index.get_level_values(level)
        if not is_datetime64_any_dtype(idx.dtype):
            raise TypeError(
                f"Index level '{ts_col}' must be datetime-like, "
                f"got dtype {idx.dtype}"
            )
        return

    raise KeyError(
        f"Timestamp '{ts_col}' not found in columns or index levels"
    )
