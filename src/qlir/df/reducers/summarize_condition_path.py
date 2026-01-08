from typing import Literal
import pandas as _pd

from qlir.core.constants import DEFAULT_OHLC_COLS, DEFAULT_OPEN_TIMESTAMP_COL
from qlir.core.types.OHLC_Cols import OHLC_Cols
from qlir.df.condition_set.assign_group_ids import assign_condition_group_id
from qlir.df.utils import _ensure_columns



def summarize_condition_paths(
    df: _pd.DataFrame,
    *,
    condition_col: str,
    ts_col: str = DEFAULT_OPEN_TIMESTAMP_COL,
    ohlc_cols: OHLC_Cols = DEFAULT_OHLC_COLS,
    group_col: str | None = None,
) -> _pd.DataFrame:
    """
    Summarize all contiguous condition-true paths in a DataFrame.
    """

    df = df.copy()

    _ensure_columns(df, [ts_col, *ohlc_cols], caller="summarize_condition_paths")

    # Just adding this step if grouping hasnt been done yet
    if group_col not in df.columns:
        (df, _) = assign_condition_group_id(
            df,
            condition_col=condition_col,
            group_col=group_col,
        )

    paths = (
        df
        .dropna(subset=[group_col])
        .groupby(group_col, sort=True)
        .apply(
            summarize_condition_path,
            ts_col=ts_col,
            ohlc_cols=ohlc_cols
        )
        .reset_index()
    )

    return paths




def summarize_condition_path(
    df: _pd.DataFrame,
    *,
    ts_col: str = DEFAULT_OPEN_TIMESTAMP_COL,
    ohlc_cols: OHLC_Cols = DEFAULT_OHLC_COLS
) -> _pd.Series:
    """
    Summarize a contiguous condition-true price path.

    Execution-agnostic path facts only.
    """

    if df.empty:
        raise ValueError("Cannot summarize empty path")

    first = df.iloc[0]
    last = df.iloc[-1]

    return _pd.Series({
        "start_time": first[ts_col],
        "end_time": last[ts_col],
        "bars": len(df),

        "first_open": df.iloc[0][ohlc_cols.open],
        "first_high": df.iloc[0][ohlc_cols.high],
        "first_low": df.iloc[0][ohlc_cols.low],
        "first_close": df.iloc[0][ohlc_cols.close],

        "last_open": df.iloc[-1][ohlc_cols.open],
        "last_high": df.iloc[-1][ohlc_cols.high],
        "last_low": df.iloc[-1][ohlc_cols.low],
        "last_close": df.iloc[-1][ohlc_cols.close],

        "path_max_high": df[ohlc_cols.high].max(),
        "path_min_low": df[ohlc_cols.low].min(),
    })
