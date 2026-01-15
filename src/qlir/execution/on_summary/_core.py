from typing import Literal

import pandas as _pd


def apply_execution_model(
    paths: _pd.DataFrame,
    *,
    direction: Literal["up", "down"],
    entry_col: str,
    exit_col: str,
) -> _pd.DataFrame:
    """
    Apply an execution model to summarized paths.
    """

    df = paths.copy()

    entry = df[entry_col]
    exit_ = df[exit_col]

    if direction == "up":
        df["pnl"] = exit_ - entry
        df["mae"] = df["path_min_low"] - entry
        df["mfe"] = df["path_max_high"] - entry

    elif direction == "down":
        df["pnl"] = entry - exit_
        df["mae"] = entry - df["path_max_high"]
        df["mfe"] = entry - df["path_min_low"]

    return df
