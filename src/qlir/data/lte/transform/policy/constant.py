from __future__ import annotations

import pandas as pd

from .base import FillPolicy, FillContext


class ConstantFillPolicy(FillPolicy):
    """
    Deterministic constant fill policy.

    All synthetic bars are flat at the previous close.
    """

    name = "constant"

    def generate(self, ctx: FillContext) -> pd.DataFrame:
        if len(ctx.timestamps) == 0:
            return pd.DataFrame()

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
            pd.DataFrame(rows)
            .set_index("timestamp")
        )
