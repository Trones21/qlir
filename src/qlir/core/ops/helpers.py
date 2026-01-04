# ----------------------------
# Helpers
# ----------------------------

from typing import List, Optional, Sequence, Union
import warnings
import pandas as pd


Number = Union[int, float]
ColsLike =  Optional[Union[str, Sequence[str]]]

def _numeric_cols(df: pd.DataFrame) -> List[str]:
    return [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]

def _normalize_cols(df: pd.DataFrame, cols: ColsLike) -> List[str]:
    if cols is None:
        return _numeric_cols(df)
    if isinstance(cols, str):
        cols = [cols]
    valid = [c for c in cols if c in df.columns]
    invalid = [c for c in cols if c not in df.columns]
    if invalid:
        warnings.warn(f"Ignoring missing columns: {invalid}", RuntimeWarning)
    return valid

def _maybe_copy(df: pd.DataFrame, inplace: bool) -> pd.DataFrame:
    return df if inplace else df.copy()

def _safe_name(base: str, *parts: Union[str, int]) -> str:
    # join non-empty parts with '__'
    extras = [str(p) for p in parts if p is not None and str(p) != ""]
    return f"{base}__{'__'.join(extras)}" if extras else base

