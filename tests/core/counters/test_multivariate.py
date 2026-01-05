import pandas as _pd
import numpy as _np

from qlir.core.counters.multivariate import (
    with_running_true_all,
    with_running_true_at_least,
    with_bars_since_any_true,
)

def test_running_true_all_basic():
    idx = _pd.RangeIndex(5)
    df = _pd.DataFrame({
        "a": [True,  True,  False, True,  True ],
        "b": [True,  True,  True,  True,  False],
        "c": [True,  False, True,  True,  True ],
    }).astype({"a": "boolean", "b": "boolean", "c": "boolean"})

    out = with_running_true_all(df, ["a", "b", "c"])
    col = "all__run_true__a__b__c"
    assert col in out.columns

    # all True only at t0; t1 fails (c False); t2 fails (a False); t3 passes; t4 fails (b False)
    expected = _pd.Series([1, 0, 0, 1, 0], index=idx, dtype="Int64")
    _pd.testing.assert_series_equal(out[col], expected, check_names=False)

def test_running_true_all_inplace():
    df = _pd.DataFrame({
        "a": [True, False],
        "b": [True, True],
    }).astype({"a": "boolean", "b": "boolean"})
    out = with_running_true_all(df, ["a", "b"], inplace=True)
    assert out is df
    assert "all__run_true__a__b" in df.columns

def test_running_true_at_least_k_of_n():
    idx = _pd.RangeIndex(4)
    df = _pd.DataFrame({
        "a": [True,  False, False, True ],
        "b": [True,  True,  False, True ],
        "c": [True,  True,  True,  False],
    }).astype({"a": "boolean", "b": "boolean", "c": "boolean"})

    # k = 2 out of 3
    out = with_running_true_at_least(df, ["a", "b", "c"], k=2)
    col = "atleast__2__run_true__a__b__c"
    assert col in out.columns

    # rows:   t0:3 trues -> True (streak=1)
    #         t1:2 trues -> True (streak=2)
    #         t2:1 true  -> False (streak=0)
    #         t3:2 trues -> True (streak=1)
    expected = _pd.Series([1, 2, 0, 1], index=idx, dtype="Int64")
    _pd.testing.assert_series_equal(out[col], expected, check_names=False)

def test_running_true_at_least_inplace_and_errors():
    df = _pd.DataFrame({
        "a": [True, False],
        "b": [True, True],
        "c": [False, True],
    }).astype({"a": "boolean", "b": "boolean", "c": "boolean"})
    out = with_running_true_at_least(df, ["a", "b", "c"], k=2, inplace=True)
    assert out is df
    assert "atleast__2__run_true__a__b__c" in df.columns

def test_bars_since_any_true_basic():
    idx = _pd.RangeIndex(6)
    df = _pd.DataFrame({
        "a": [False, False, _np.nan, True,  False, False],
        "b": [False, True,  False,  False, False, True ],
        "c": [False, False, False,  False, False, False],
    }).astype({"a": "boolean", "b": "boolean", "c": "boolean"})

    out = with_bars_since_any_true(df, ["a", "b", "c"])
    col = "any__bars_since_true__a__b__c"
    assert col in out.columns

    # any True appears first at t1 (b=True) -> 0
    # t0 before first True -> NaN
    # t2 after last True at t1 -> 1
    # t3 a=True -> 0
    # t4 -> 1
    # t5 b=True -> 0
    expected = _pd.Series([_pd.NA, 0, 1, 0, 1, 0], index=idx, dtype="Int64")
    _pd.testing.assert_series_equal(out[col], expected, check_names=False)

def test_bars_since_any_true_inplace():
    df = _pd.DataFrame({
        "a": [False, True],
        "b": [False, False],
    }).astype({"a": "boolean", "b": "boolean"})
    out = with_bars_since_any_true(df, ["a", "b"], inplace=True)
    assert out is df
    assert "any__bars_since_true__a__b" in df.columns
