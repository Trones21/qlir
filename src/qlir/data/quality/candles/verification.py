from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from qlir.data.lte.transform.gaps.materialization.markers import SYNTHETIC_COL

# ---------------------------------------------------------------------
# Results
# ---------------------------------------------------------------------

@dataclass(frozen=True)
class GapResolutionVerification:
    """
    Evidence that gap resolution preserved candle count.

    missing_before
        Number of missing candles reported before gap resolution.

    filled_rows
        Number of rows that were backfilled or interpolated.

    delta
        filled_rows - missing_before

    matches
        True if filled_rows == missing_before
    """
    missing_before: int
    filled_rows: int
    delta: int
    matches: bool


# ---------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------

def verify_gap_resolution(
    *,
    missing_count: int,
    df_after_fill: pd.DataFrame,
    filled_flag_col: str = SYNTHETIC_COL,
    strict: bool = True,
) -> GapResolutionVerification:
    """
    Verify that gap resolution conserves candle count.

    This enforces the invariant:

        missing_before == filled_rows

    Parameters
    ----------
    `missing_count`.
        Result of the *pre-fill* Data Candle Quality Report.


    df_after_fill
        DataFrame after backfill / interpolation has occurred.

    filled_flag_col
        Boolean column marking rows that were filled
        (backfilled or interpolated).

    strict
        If True, raise AssertionError when invariant is violated.
        If False, return verification result only.

    Returns
    -------
    GapResolutionVerification
        Structured evidence of the check.
    """

    if filled_flag_col not in df_after_fill.columns:
        raise KeyError(
            f"Expected column `{filled_flag_col}` not found in DataFrame"
        )

    missing_before = missing_count
    filled_rows = int(df_after_fill[filled_flag_col].sum())

    delta = filled_rows - missing_before
    matches = delta == 0

    result = GapResolutionVerification(
        missing_before=missing_before,
        filled_rows=filled_rows,
        delta=delta,
        matches=matches,
    )

    if strict and not matches:
        raise AssertionError(
            "Gap resolution verification failed: "
            f"missing_before={missing_before}, "
            f"filled_rows={filled_rows}, "
            f"delta={delta}"
        )
    
    return result
