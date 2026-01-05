from __future__ import annotations

import pandas as _pd

from .base import FillPolicy, FillContext


class ConstantFillPolicy(FillPolicy):
    """
    Deterministic constant fill policy.

    All synthetic bars are flat at the previous close.
    """

    name = "constant"

    def generate(self, ctx: FillContext) -> _pd.DataFrame:
        if len(ctx.timestamps) == 0:
            return _pd.DataFrame()

        rows = []
        prev_close = ctx.left["close"]

        for ts in ctx.timestamps:
            rows.append(
                {
                    "timestamp": ts,
                    "open": prev_close,
                    "high": prev_close,
                    "low": prev_close,
                    "close": prev_close,
                }
            )

        return (
            _pd.DataFrame(rows)
            .set_index("timestamp")
        )
