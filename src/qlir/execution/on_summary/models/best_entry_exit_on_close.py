from typing import Literal
import pandas as pd

from qlir.execution.on_summary._core import apply_execution_model


def execute(
    paths: pd.DataFrame,
    *,
    direction: Literal["up", "down"],
) -> pd.DataFrame:
    """
    Best-case execution:
    - Entry at first candle extreme favorable to the trader
    - Exit at last close
    """

    if direction == "up":
        entry_col = "first_low"
    else:
        entry_col = "first_high"

    return apply_execution_model(
        paths,
        direction=direction,
        entry_col=entry_col,
        exit_col="last_close",
    )
