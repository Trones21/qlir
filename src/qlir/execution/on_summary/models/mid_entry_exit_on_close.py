from typing import Literal
import pandas as pd

from qlir.execution.on_summary._core import apply_execution_model


def execute(
    paths: pd.DataFrame,
    *,
    direction: Literal["up", "down"],
) -> pd.DataFrame:
    """
    Mid execution:
    - Entry at midpoint of first candle
    - Exit at last close
    """

    paths = paths.copy()

    paths["first_mid"] = (paths["first_high"] + paths["first_low"]) / 2.0

    return apply_execution_model(
        paths,
        direction=direction,
        entry_col="first_mid",
        exit_col="last_close",
    )
