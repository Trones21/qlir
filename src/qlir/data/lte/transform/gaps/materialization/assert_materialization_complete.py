import pandas as pd
from qlir.data.lte.transform.gaps.materialization.markers import ROW_MATERIALIZED_COL


def assert_materialization_complete(df: pd.DataFrame) -> None:
    """
    Assert that the DataFrame has passed the time materialization phase.

    Preconditions:
    - DatetimeIndex representing bar-open timestamps
    - Presence of ROW_MATERIALIZED_COL marker
    """
    if not isinstance(df.index, pd.DatetimeIndex):
        raise TypeError(
            "Expected DataFrame indexed by bar-open timestamps "
            "(DatetimeIndex). Time must be materialized first."
        )

    if ROW_MATERIALIZED_COL not in df.columns:
        raise RuntimeError(
            "Expected materialized rows marker column "
            f"'{ROW_MATERIALIZED_COL}'. Run materialize_missing_rows() first."
        )
