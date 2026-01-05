import pandas as _pd
import numpy as _np

from qlir.core.counters.univariate import (
    with_running_true,
    with_bars_since_true,
)

def test_with_running_true_basic_and_dtype():
    idx = _pd.date_range("2024-01-01", periods=6, freq="T")
    df = _pd.DataFrame({
        "c": [True, True, False, True, _np.nan, True]  # NaN -> False
    }, index=idx).astype({"c": "boolean"})

    out = with_running_true(df, "c")
    col = "c__run_true"
    assert col in out.columns
    _pd.testing.assert_series_equal(
        out[col],
        _pd.Series([1, 2, 0, 1, 0, 1], index=idx, dtype="Int64"),
        check_names=False
    )

def test_with_running_true_inplace():
    df = _pd.DataFrame({"c": [True, False, True]}).astype({"c": "boolean"})
    out = with_running_true(df, "c", inplace=True)
    assert out is df
    assert "c__run_true" in df.columns

def test_bars_since_true_basic_and_dtype():
    # Includes NaN before first True and after resets
    idx = _pd.RangeIndex(7)
    df = _pd.DataFrame({
        "c": [False, _np.nan, False, True, False, False, True]
    }).astype({"c": "boolean"})

    out = with_bars_since_true(df, "c")
    col = "c__bars_since_true"
    # Expect NaN before first True (rows 0..2), 0 at True, then increasing until next True resets to 0
    expected = _pd.Series(
        [_pd.NA, _pd.NA, _pd.NA, 0, 1, 2, 0],
        index=idx,
        dtype="Int64"
    )
    _pd.testing.assert_series_equal(out[col], expected, check_names=False)

def test_bars_since_true_inplace():
    df = _pd.DataFrame({"c": [True, False, False]}).astype({"c": "boolean"})
    out = with_bars_since_true(df, "c", inplace=True)
    assert out is df
    assert "c__bars_since_true" in df.columns
