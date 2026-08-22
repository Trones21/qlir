"""Mark the extreme bar within each leg.

NOT IMPLEMENTED. This file held a bare function signature with no body (and no
closing colon), so it could not be parsed on any Python version. Nothing
imported it, so the breakage was invisible. The signature is preserved.
"""

from typing import Literal


def mark_leg_extrema(
    df,
    *,
    leg_id_col,
    value_col,
    how: Literal["max", "min"],
    tie_breaker: Literal["first", "last"],
    out_col,
):
    """
    Flag the row holding the max/min `value_col` within each `leg_id_col` group,
    writing a boolean into `out_col`. `tie_breaker` decides which row wins when
    the extreme value repeats inside one leg.
    """
    raise NotImplementedError(
        "mark_leg_extrema is a signature sketch; no implementation was ever written."
    )
