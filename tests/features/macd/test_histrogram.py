import pandas as pd
import pytest
from qlir.features.macd.histogram import with_colored_histogram

def test_macd_histogram():
    """
    Verifies:
    - signed dist
    - absolute dist
    - N-1 expansion logic
    - string + int encodings
    """

    df = pd.DataFrame(
        {
            # fast - slow:
            # t0:  1  (bullish)
            # t1:  2  (bullish, expanding)
            # t2:  1  (bullish, contracting)
            # t3: -1  (bearish, expanding)
            "fast": [11, 12, 11, 9],
            "slow": [10, 10, 10, 10],
        }
    )

    # --- string output (default) ---
    out = with_colored_histogram(
        df.copy(),
        fast_col="fast",
        slow_col="slow",
        prefix="macd",
    ).df

    assert out["macd_dist"].tolist() == [1, 2, 1, -1]
    assert out["macd_dist_abs"].tolist() == [1, 2, 1, 1]

    assert out["macd_dist_color"].tolist() == [
        "light_green",  # first row: contracting by definition
        "dark_green",   # expanding bullish
        "light_green",  # contracting bullish
        "dark_red",     # expanding bearish
    ]

    # --- integer output ---
    out_int = with_colored_histogram(
        df.copy(),
        fast_col="fast",
        slow_col="slow",
        prefix="macd",
        as_int=True,
    ).df

    assert out_int["macd_dist_color"].tolist() == [
        1,   # light green
        2,   # dark green
        1,   # light green
        -2,  # dark red
    ]
