# src/qlir/core/comparators.py
from __future__ import annotations

from typing import Optional, Union, Literal
import numpy as np
import pandas as pd

BoolDtype = "boolean"
Scalar = Union[int, float]

# ----------------------------
# helpers
# ----------------------------

def _maybe_copy(df: pd.DataFrame, inplace: bool) -> pd.DataFrame:
    return df if inplace else df.copy()

def _safe_name(*parts: object, sep: str = "__") -> str:
    toks = [str(p) for p in parts if p is not None and str(p) != ""]
    return sep.join(toks)

def _get_series(df: pd.DataFrame, x: Union[str, Scalar]) -> pd.Series:
    """
    Return a Series for col name or broadcast a scalar to index.
    """
    if isinstance(x, str):
        if x not in df.columns:
            raise KeyError(f"column '{x}' not in DataFrame")
        return df[x]
    # scalar -> Series aligned to df index
    return pd.Series(x, index=df.index)

def _safe_bool(s: pd.Series) -> pd.Series:
    if s.dtype != BoolDtype:
        s = s.astype(BoolDtype)
    # Comparisons with NaN yield False by default for determinism
    return s.fillna(False)

def _compare(
    df: pd.DataFrame,
    a: Union[str, Scalar],
    b: Union[str, Scalar],
    *,
    op: Literal["gt","ge","lt","le","eq","ne"],
    tol: float = 0.0,                # only meaningful for eq (abs diff <= tol) or to bias thresholds
) -> pd.Series:
    A = _get_series(df, a)
    B = _get_series(df, b)

    if op == "gt":
        res = A > (B + tol)
    elif op == "ge":
        res = A >= (B - tol)
    elif op == "lt":
        res = A < (B - tol)
    elif op == "le":
        res = A <= (B + tol)
    elif op == "eq":
        # |A - B| <= tol (default tol=0 exact equality)
        res = (A - B).abs() <= tol
    elif op == "ne":
        # not equal with tolerance -> |A-B| > tol
        res = (A - B).abs() > tol if tol > 0 else (A != B)
    else:
        raise ValueError(f"unknown comparator op: {op}")

    return _safe_bool(res)

# ----------------------------
# public API
# ----------------------------

def with_gt(
    df: pd.DataFrame,
    a: Union[str, Scalar],
    b: Union[str, Scalar],
    *,
    name: Optional[str] = None,
    tol: float = 0.0,
    inplace: bool = False,
) -> pd.DataFrame:
    out = _maybe_copy(df, inplace)
    colname = name or _safe_name(str(a), "gt", str(b))
    out[colname] = _compare(out, a, b, op="gt", tol=tol)
    return out

def with_ge(
    df: pd.DataFrame,
    a: Union[str, Scalar],
    b: Union[str, Scalar],
    *,
    name: Optional[str] = None,
    tol: float = 0.0,
    inplace: bool = False,
) -> pd.DataFrame:
    out = _maybe_copy(df, inplace)
    colname = name or _safe_name(str(a), "ge", str(b))
    out[colname] = _compare(out, a, b, op="ge", tol=tol)
    return out

def with_lt(
    df: pd.DataFrame,
    a: Union[str, Scalar],
    b: Union[str, Scalar],
    *,
    name: Optional[str] = None,
    tol: float = 0.0,
    inplace: bool = False,
) -> pd.DataFrame:
    out = _maybe_copy(df, inplace)
    colname = name or _safe_name(str(a), "lt", str(b))
    out[colname] = _compare(out, a, b, op="lt", tol=tol)
    return out

def with_le(
    df: pd.DataFrame,
    a: Union[str, Scalar],
    b: Union[str, Scalar],
    *,
    name: Optional[str] = None,
    tol: float = 0.0,
    inplace: bool = False,
) -> pd.DataFrame:
    out = _maybe_copy(df, inplace)
    colname = name or _safe_name(str(a), "le", str(b))
    out[colname] = _compare(out, a, b, op="le", tol=tol)
    return out

def with_eq(
    df: pd.DataFrame,
    a: Union[str, Scalar],
    b: Union[str, Scalar],
    *,
    name: Optional[str] = None,
    tol: float = 0.0,              # equality within tolerance
    inplace: bool = False,
) -> pd.DataFrame:
    out = _maybe_copy(df, inplace)
    colname = name or _safe_name(str(a), "eq", str(b))
    out[colname] = _compare(out, a, b, op="eq", tol=tol)
    return out

def with_ne(
    df: pd.DataFrame,
    a: Union[str, Scalar],
    b: Union[str, Scalar],
    *,
    name: Optional[str] = None,
    tol: float = 0.0,              # inequality with tolerance (|A-B| > tol)
    inplace: bool = False,
) -> pd.DataFrame:
    out = _maybe_copy(df, inplace)
    colname = name or _safe_name(str(a), "ne", str(b))
    out[colname] = _compare(out, a, b, op="ne", tol=tol)
    return out
