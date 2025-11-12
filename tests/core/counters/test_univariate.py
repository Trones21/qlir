import pandas as pd
import numpy as np

from qlir.core.counters.univariate import (
    with_running_true,
    with_bars_since_true,
)

def test_with_running_true_basic_and_dtype():
    idx = pd.date_range("2024-01-01", periods=6, freq="T")
    df = pd.DataFrame({
        "c": [True, True, False, True, np.nan, True]  # NaN -> False
    }, index=idx).astype({"c": "boolean"})

    out = with_running_true(df, "c")
    col = "c__run_true"
    assert col in out.columns
    pd.testing.assert_series_equal(
        out[col],
        pd.Series([1, 2, 0, 1, 0, 1], index=idx, dtype="Int64"),
        check_names=False
    )

def test_with_running_true_inplace():
    df = pd.DataFrame({"c": [True, False, True]}).astype({"c": "boolean"})
    out = with_running_true(df, "c", inplace=True)
    assert out is df
    assert "c__run_true" in df.columns

def test_bars_since_true_basic_and_dtype():
    # Includes NaN before first True and after resets
    idx = pd.RangeIndex(7)
    df = pd.DataFrame({
        "c": [False, np.nan, False, True, False, False, True]
    }).astype({"c": "boolean"})

    out = with_bars_since_true(df, "c")
    col = "c__bars_since_true"
    # Expect NaN before first True (rows 0..2), 0 at True, then increasing until next True resets to 0
    expected = pd.Series(
        [pd.NA, pd.NA, pd.NA, 0, 1, 2, 0],
        index=idx,
        dtype="Int64"
    )
    pd.testing.assert_series_equal(out[col], expected, check_names=False)

def test_bars_since_true_inplace():
    df = pd.DataFrame({"c": [True, False, False]}).astype({"c": "boolean"})
    out = with_bars_since_true(df, "c", inplace=True)
    assert out is df
    assert "c__bars_since_true" in df.columns
