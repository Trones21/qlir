from typing import Optional

import numpy as _np
import pandas as _pd

from qlir.core.counters.multivariate import _maybe_copy
from qlir.core.ops._helpers import ColsLike, _add_columns_from_series_map, _normalize_cols


def with_sign(
    df: _pd.DataFrame,
    cols: ColsLike = None,
    *,
    suffix: Optional[str] = None,
    zero_as_zero: bool = True,
    inplace: bool = False,
) -> tuple[_pd.DataFrame, tuple[str, ...]]:
    """
    Add sign of series values: {-1, 0, +1} (or {-1, +1} if zero_as_zero=False).
    """
    out = _maybe_copy(df, inplace)
    use_cols = _normalize_cols(out, cols)

    tmp = out[use_cols]
    sign = _np.sign(tmp)

    if not zero_as_zero:
        sign = sign.replace(0, 1)

    # dtype normalization
    sign = sign.astype("Int8", errors="ignore")

    return _add_columns_from_series_map(
        out,
        use_cols=use_cols,
        series_by_col={c: sign[c] for c in use_cols},
        suffix=suffix or "sign",
    )



def with_abs(
    df: _pd.DataFrame,
    cols: ColsLike = None,
    *,
    suffix: Optional[str] = None,
    inplace: bool = False,
) -> tuple[_pd.DataFrame, tuple[str, ...]]:
    """
    Add absolute value of series.
    """
    out = _maybe_copy(df, inplace)
    use_cols = _normalize_cols(out, cols)

    abs_df = out[use_cols].abs()

    return _add_columns_from_series_map(
        out,
        use_cols=use_cols,
        series_by_col={c: abs_df[c] for c in use_cols},
        suffix=suffix or "abs",
    )


# We do this so that those ppl who crazily do from qlir.core.ops.non_temporal import * 
# will see the public funcs 
__all__ = ["with_sign", "with_abs"]