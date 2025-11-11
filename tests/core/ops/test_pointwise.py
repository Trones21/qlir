import numpy as np
import pandas as pd
import pytest
pytestmark = pytest.mark.local

from qlir.core.ops.pointwise import (
    add_diff,
    add_pct_change,
    add_log_return,
    add_shift,
    add_sign,
    add_abs,
    add_bar_direction,
)

def _df_basic():
    idx = pd.date_range("2024-01-01", periods=6, freq="T")
    return pd.DataFrame(
        {
            "open":  [10.0, 10.0, 11.0, 10.0,  0.0,  0.0],
            "close": [10.0, 11.0, 11.0, 12.0, 12.0,  0.0],
            "txt":   ["a", "b", "c", "d", "e", "f"],  # non-numeric
        },
        index=idx,
    )

# ---------------------------
# add_diff
# ---------------------------

def test_add_diff_default_numeric_autopick_and_suffix():
    df = _df_basic()
    out = add_diff(df)  # cols=None -> numeric only
    # both open and close should have diff columns
    assert "open__diff_1" in out and "close__diff_1" in out
    # txt should NOT get one
    assert "txt__diff_1" not in out

    # spot-check values
    assert out["open__diff_1"].iloc[0] is np.nan
    assert out["open__diff_1"].iloc[2] == 1.0  # 11 - 10
    assert out["close__diff_1"].iloc[3] == 1.0  # 12 - 11

def test_add_diff_inplace_and_periods():
    df = _df_basic()
    out = add_diff(df, cols=["close"], periods=2, inplace=True)
    assert out is df
    assert "close__diff_2" in df.columns
    # 2-step diff at t=2: 11 - 10 = 1
    assert df["close__diff_2"].iloc[2] == 1.0

# ---------------------------
# add_pct_change
# ---------------------------

def test_add_pct_change_basic_clip_inf_and_fill():
    df = _df_basic()
    # Without fill, pct when prior is 0 -> inf -> clipped to NaN
    out = add_pct_change(df, cols=["open"], periods=1, clip_inf_to_nan=True)
    col = "open__pct_1"
    assert col in out
    # at t=5 prior open=0 -> (0/0)-1 => NaN after clip/prop
    assert np.isnan(out[col].iloc[5])

    # With fill forward, the 0→0 segment becomes stable => pct 0.0 at last row
    out2 = add_pct_change(df, cols=["open"], periods=1, fill_method="ffill", clip_inf_to_nan=True)
    assert out2[col].iloc[5] == 0.0

def test_add_pct_change_multicol_and_suffix():
    df = _df_basic()
    out = add_pct_change(df, cols=["open", "close"], periods=3, suffix="pc3")
    assert "open__pc3" in out and "close__pc3" in out
    # spot-check: close t=3 vs t=0: 12/10 - 1 = 0.2
    assert abs(out["close__pc3"].iloc[3] - 0.2) < 1e-12

# ---------------------------
# add_log_return
# ---------------------------

def test_add_log_return_clip_vs_epsilon_guard():
    df = _df_basic()

    # Case 1: no epsilon -> log(0) appears -> should be NaN (clipped)
    out = add_log_return(df, cols=["open"], periods=1, clip_inf_to_nan=True)
    col = "open__logret_1"
    assert np.isnan(out[col].iloc[4]) or np.isnan(out[col].iloc[5])

    # Case 2: small epsilon guards zeros -> finite value
    out2 = add_log_return(df, cols=["open"], periods=1, epsilon=1e-9, clip_inf_to_nan=True)
    assert np.isfinite(out2[col].iloc[5])

def test_add_log_return_suffix_and_periods():
    df = _df_basic()
    out = add_log_return(df, cols=["close"], periods=2, suffix="lr2", epsilon=0.0)
    assert "close__lr2" in out

# ---------------------------
# add_shift
# ---------------------------

def test_add_shift_basic_and_inplace():
    df = _df_basic()
    out = add_shift(df, cols=["close"], periods=2)
    assert "close__shift_2" in out
    assert out["close__shift_2"].iloc[2] == df["close"].iloc[0]

    out2 = add_shift(df, cols=["open"], periods=1, inplace=True)
    assert out2 is df
    assert "open__shift_1" in df

# ---------------------------
# add_sign
# ---------------------------

def test_add_sign_zero_convention_true_and_false():
    df = pd.DataFrame({"x": [-2.0, 0.0, 3.0]})
    out = add_sign(df, cols=["x"])
    assert list(out["x__sign"].tolist()) == [-1.0, 0.0, 1.0]  # dtype may be float from numpy.sign

    out2 = add_sign(df, cols=["x"], zero_as_zero=False)
    # zeros mapped to +1
    assert list(out2["x__sign"].tolist()) == [-1.0, 1.0, 1.0]

def test_add_sign_multi_numeric_autopick():
    df = pd.DataFrame({"a": [1, -1, 0], "b": [0.0, 2.0, -3.0], "s": ["x", "y", "z"]})
    out = add_sign(df)  # auto-picks numeric cols a,b
    assert "a__sign" in out and "b__sign" in out and "s__sign" not in out

# ---------------------------
# add_abs
# ---------------------------

def test_add_abs_basic_and_suffix():
    df = pd.DataFrame({"a": [-1.0, 0.0, 2.5]})
    out = add_abs(df, cols=["a"], suffix="ABS")
    assert "a__ABS" in out
    assert out["a__ABS"].tolist() == [1.0, 0.0, 2.5]

# ---------------------------
# add_bar_direction
# ---------------------------

def test_add_bar_direction_pipeline():
    df = pd.DataFrame({"open": [10, 10, 11, 10, 12]})
    out = add_bar_direction(df, col="open")
    # should create open__diff_1 and open__direction
    assert "open__diff_1" in out and "open__direction" in out
    # diffs: [NaN,0,1,-1,2] -> signs: [NaN,0,1,-1,1]
    expected = [np.nan, 0, 1, -1, 1]
    got = out["open__direction"].astype("float64").tolist()
    # compare element-wise allowing NaN
    for g, e in zip(got, expected):
        if np.isnan(e):
            assert np.isnan(g)
        else:
            assert g == e
