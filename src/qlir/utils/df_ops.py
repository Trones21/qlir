from __future__ import annotations

import pandas as _pd

__all__ = ["ensure_copy", "astype_int8"]


def ensure_copy(df: _pd.DataFrame, in_place: bool) -> _pd.DataFrame:
    return df if in_place else df.copy()


def astype_int8(s: _pd.Series) -> _pd.Series:
    return s.astype("int8")