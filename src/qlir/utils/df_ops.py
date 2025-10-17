from __future__ import annotations
import pandas as pd


__all__ = ["ensure_copy", "astype_int8"]


def ensure_copy(df: pd.DataFrame, in_place: bool) -> pd.DataFrame:
    return df if in_place else df.copy()


def astype_int8(s: pd.Series) -> pd.Series:
    return s.astype("int8")