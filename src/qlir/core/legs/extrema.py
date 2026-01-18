from typing import Literal


def mark_leg_extrema(
    df,
    *,
    leg_id_col,
    value_col,
    how: Literal["max", "min"],
    tie_breaker: Literal["first", "last"],
    out_col,
)
