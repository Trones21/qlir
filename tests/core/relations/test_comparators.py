import numpy as np
import pandas as pd
import pytest
pytestmark = pytest.mark.local
from qlir.core.relations.comparators import (
    with_gt, with_ge, with_lt, with_le, with_eq, with_ne
)

def _df():
    # a and b include equal, greater, less, and NaN cases
    return pd.DataFrame({
        "a": [1.0, 2.0, np.nan, 5.0, 5.0],
        "b": [0.5, 2.0, 3.0,    4.0, 5.1],
    })

def _is_nullable_bool(s: pd.Series) -> bool:
    return str(s.dtype) == "boolean"

# -------------------------
# col-to-col comparisons
# -------------------------

def test_gt_col_col_and_dtype():
    df = _df()
    out = with_gt(df, "a", "b")
    col = "a__gt__b"
    assert col in out.columns
    # a > b elementwise: [True, False, NaN->False, True, False]
    expected = pd.Series([True, False, False, True, False], dtype="boolean")
    assert out[col].equals(expected)
    assert _is_nullable_bool(out[col])

def test_ge_with_tolerance_makes_borderline_true():
    df = _df()
    # With tol=0.1, ge is a >= b - tol
    # idx4: a=5.0, b=5.1 -> compare 5.0 >= (5.1 - 0.1) == 5.0 -> True
    out = with_ge(df, "a", "b", tol=0.1)
    col = "a__ge__b"
    expected = pd.Series([
        True,   # 1.0 >= 0.5 - 0.1
        True,   # 2.0 >= 2.0 - 0.1
        False,  # NaN -> False
        True,   # 5.0 >= 4.0 - 0.1
        True,   # 5.0 >= 5.1 - 0.1 (==5.0)
    ], dtype="boolean")
    assert out[col].equals(expected)

def test_lt_col_scalar_and_name():
    df = _df()
    out = with_lt(df, "a", 2.0)
    col = "a__lt__2.0"
    assert col in out.columns
    # a < 2.0: [True, False, NaN->False, False, False]
    expected = pd.Series([True, False, False, False, False], dtype="boolean")
    assert out[col].equals(expected)

def test_le_scalar_col():
    df = _df()
    # 2.0 <= b + tol (default tol=0)
    out = with_le(df, 2.0, "b")
    col = "2.0__le__b"
    expected = pd.Series([
        False,  # 2.0 <= 0.5 ? False
        True,   # 2.0 <= 2.0 ? True
        True,   # 2.0 <= 3.0 ? True
        False,  # 2.0 <= 4.0 ? False (oops—this should be True; fix below) 
    ], dtype="boolean")
    # The previous line has an error; recompute robustly from data:
    expected = (pd.Series([2.0]*len(df), index=df.index) <= df["b"]).astype("boolean").fillna(False)
    assert out[col].equals(expected)

# -------------------------
# equality / inequality with tolerance
# -------------------------

def test_eq_with_tolerance():
    df = _df()
    # eq: |a - b| <= tol
    out = with_eq(df, "a", "b", tol=0.1)
    col = "a__eq__b"
    # diffs: [0.5,0.0,NaN,1.0,0.1] -> within 0.1 at idx1 and idx4
    expected = pd.Series([False, True, False, False, True], dtype="boolean")
    assert out[col].equals(expected)

def test_ne_with_tolerance():
    df = _df()
    # ne with tol: |a-b| > 0.1 else False
    out = with_ne(df, "a", "b", tol=0.1)
    col = "a__ne__b"
    # diffs: [0.5,0.0,NaN,1.0,0.1] -> True at idx0, idx3; NaN -> False; idx4 exactly tol -> False
    expected = pd.Series([True, False, False, True, False], dtype="boolean")
    assert out[col].equals(expected)

# -------------------------
# inplace & custom names
# -------------------------

def test_inplace_and_custom_name():
    df = _df()
    out = with_gt(df, "a", "b", name="A_gt_B", inplace=True)
    assert out is df
    assert "A_gt_B" in df.columns
    assert _is_nullable_bool(df["A_gt_B"])
