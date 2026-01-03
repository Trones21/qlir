from typing import Literal
import pandas as pd

from qlir.execution.apply_execution_model import apply_execution_model


def apply_worst_entry_exit_on_close(
    paths: pd.DataFrame,
    *,
    direction: Literal["up", "down"],
) -> pd.DataFrame:
    """
    Worst-case execution:
    - Entry at first candle extreme against the trader
    - Exit at last close
    """

    if direction == "up":
        entry_col = "first_high"
        exit_col = "last_close"
    else:
        entry_col = "first_low"
        exit_col = "last_close"

    return apply_execution_model(
        paths,
        direction=direction,
        entry_col=entry_col,
        exit_col=exit_col,
    )
