from typing import Optional, Sequence, Union
import numpy as np
import pandas as pd

from qlir.core.counters.multivariate import _maybe_copy, _safe_name
from qlir.core.ops.helpers import ColsLike, _normalize_cols


def with_sign(
    df: pd.DataFrame,
    cols: ColsLike = None,
    *,
    suffix: Optional[str] = None,
    zero_as_zero: bool = True,
    inplace: bool = False,
) -> pd.DataFrame:
    """
    Add sign of series values: {-1, 0, +1} (or {-1, +1} if zero_as_zero=False).
    """
    out = _maybe_copy(df, inplace)
    use_cols = _normalize_cols(out, cols)

    for c in use_cols:
        name = _safe_name(c, suffix or "sign")
        s = np.sign(out[c])
        if not zero_as_zero:
            # map zeros to +1 (or choose your convention)
            s = s.replace(0, 1)
        out[name] = s.astype("Int8") if pd.api.types.is_integer_dtype(s) else s
    return out


def with_abs(
    df: pd.DataFrame,
    cols: ColsLike = None,
    *,
    suffix: Optional[str] = None,
    inplace: bool = False,
) -> pd.DataFrame:
    """
    Add absolute value of series.
    """
    out = _maybe_copy(df, inplace)
    use_cols = _normalize_cols(out, cols)
    for c in use_cols:
        name = _safe_name(c, suffix or "abs")
        out[name] = out[c].abs()
    return out
