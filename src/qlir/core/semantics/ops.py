from __future__ import annotations

from typing import Sequence

import pandas as pd

from .context import get_ctx


def drop_cols(
    df: pd.DataFrame,
    cols: str | Sequence[str],
    *,
    reason: str | None = None,
    errors: str = "ignore",
) -> pd.DataFrame:
    """
    Drop columns from the DataFrame while recording lifecycle events in the active DerivationContext.

    This preserves derivation truth even if intermediate columns are removed from the returned df.
    """
    if isinstance(cols, str):
        cols_list = [cols]
    else:
        cols_list = list(cols)

    ctx = get_ctx()
    if ctx is not None:
        for c in cols_list:
            ctx.add_dropped(col=c, reason=reason)

    return df.drop(columns=cols_list, errors=errors)