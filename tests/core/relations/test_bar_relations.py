import numpy as np
import pandas as pd

from qlir.core.relations.bar_relations import (
    add_higher_high,
    add_lower_low,
    add_higher_open,
    add_lower_open,
    add_higher_close,
    add_lower_close,
    add_inside_bar,
    add_outside_bar,
    add_bullish_bar,
    add_bearish_bar,
    add_true_range,
    add_range_expansion_vs_prev,
)

def _ohlc_df():
    # 5-bars synthetic OHLC
    #             o    h    l    c
    data = [
        [10.0, 11.0,  9.0, 10.5],  # t0
        [10.5, 11.5,  9.5, 10.3],  # t1
        [10.0, 10.8,  9.8, 10.6],  # t2  inside (<=,>=) vs t1
        [10.7, 12.0,  9.2, 11.9],  # t3  outside (>=,<=) vs t2
        [10.7, 12.0, 10.0, 10.7],  # t4  same high as t3, higher low
    ]
    df = pd.DataFrame(data, columns=["open", "high", "low", "close"])
    df.index = pd.RangeIndex(len(df))
    return df

# -------------------------
# simple higher/lower relations
# -------------------------

def test_higher_and_lower_relations_basic():
    df = _ohlc_df()

    out = add_higher_high(df.copy())
    assert "high__higher_high" in out
    # t1 high > t0 high -> True; t2 high < t1 -> False; t3 > t2 -> True; t4 == t3 -> False
    expected_hh = pd.Series([False, True, False, True, False], dtype="boolean")
    assert out["high__higher_high"].equals(expected_hh)

    out = add_lower_low(df.copy())
    assert "low__lower_low" in out
    # t1 low > t0 -> False; t2 low > t1 -> False; t3 low < t2 -> True; t4 low > t3 -> False
    expected_ll = pd.Series([False, False, False, True, False], dtype="boolean")
    assert out["low__lower_low"].equals(expected_ll)

    out = add_higher_open(df.copy())
    assert "open__higher_open" in out
    expected_ho = pd.Series([False, True, False, True, False], dtype="boolean")
    assert out["open__higher_open"].equals(expected_ho)

    out = add_lower_close(df.copy())
    assert "close__lower_close" in out
    expected_lc = pd.Series([False, False, True, False, True], dtype="boolean")
    assert out["close__lower_close"].equals(expected_lc)

def test_custom_name_and_inplace():
    df = _ohlc_df()
    out = add_higher_close(df, name="hc_up", inplace=True)
    assert out is df
    assert "hc_up" in df.columns

# -------------------------
# inside / outside bars
# -------------------------

def test_inside_bar_inclusive_and_strict():
    df = _ohlc_df()

    out_inc = add_inside_bar(df.copy(), inclusive="both")
    col = "inside_bar"
    # Check that our crafted t2 is inside t1 (<= and >=)
    expected_inc = pd.Series([False, False, True, False, False], dtype="boolean")
    assert out_inc[col].equals(expected_inc)

    out_strict = add_inside_bar(df.copy(), inclusive="strict")
    # Strict requires < and >, so t2 still inside (10.8 < 11.5 and 9.8 > 9.5)
    expected_strict = expected_inc  # also True at t2 here
    assert out_strict[col].equals(expected_strict)

def test_outside_bar_inclusive_and_strict():
    df = _ohlc_df()

    out_inc = add_outside_bar(df.copy(), inclusive="both")
    col = "outside_bar"
    # t3 engulfs t2 (>=,<=) -> True
    expected_inc = pd.Series([False, False, False, True, False], dtype="boolean")
    assert out_inc[col].equals(expected_inc)

    out_strict = add_outside_bar(df.copy(), inclusive="strict")
    # t3 still strict True (12.0 > 10.8 and 9.2 < 9.8)
    expected_strict = expected_inc
    assert out_strict[col].equals(expected_strict)

# -------------------------
# bullish / bearish bars
# -------------------------

def test_bullish_bearish_allow_equal():
    df = _ohlc_df()

    out_bull = add_bullish_bar(df.copy())
    # bullish when close > open: t0,t2,t3,t4
    expected = pd.Series([True, False, True, True, False], dtype="boolean")
    assert out_bull["bullish_bar"].equals(expected)

    out_bear = add_bearish_bar(df.copy(), allow_equal=True)
    # bearish when close <= open: t1,t4
    expected_bear = pd.Series([False, True, False, False, True], dtype="boolean")
    assert out_bear["bearish_bar"].equals(expected_bear)

# -------------------------
# True Range & Range Expansion
# -------------------------

def test_true_range_values():
    df = _ohlc_df()
    out = add_true_range(df.copy())
    tr = out["true_range"]
    # t0: TR = high-low = 2.0
    assert abs(tr.iloc[0] - 2.0) < 1e-12
    # t1: max( h-l=2.0, |h - prev_c|=|11.5-10.5|=1.0, |l - prev_c|=|9.5-10.5|=1.0 ) = 2.0
    assert abs(tr.iloc[1] - 2.0) < 1e-12
    # t3: max(12.0-9.2=2.8, |12.0-10.6|=1.4, |9.2-10.6|=1.4) = 2.8
    assert abs(tr.iloc[3] - 2.8) < 1e-12

def test_range_expansion_highlow_and_tr():
    df = _ohlc_df()

    # highlow method
    out1 = add_range_expansion_vs_prev(df.copy(), method="highlow")
    col1 = "range_expansion__highlow"
    # compute hl range to validate
    hl = df["high"] - df["low"]
    expected1 = (hl > hl.shift(1)).astype("boolean").fillna(False)
    assert out1[col1].equals(expected1)

    # tr method
    out2 = add_range_expansion_vs_prev(df.copy(), method="tr")
    col2 = "range_expansion__tr"
    # recompute TR and compare with shift
    out_tr = add_true_range(df.copy())
    tr = out_tr["true_range"]
    expected2 = (tr > tr.shift(1)).astype("boolean").fillna(False)
    assert out2[col2].equals(expected2)
