from qlir.features.macd.histogram_loose_pyramid import detect_loose_histogram_pyramids
import pandas as pd

def test_detect_loose_histogram_pyramids():
    """
    Validates loose histogram pyramid detection.

    Loose pyramid rules:
    - contiguous segment between sign-crosses
    - length >= 2
    - contains at least one dark and one light
    - allows dark <-> light reversals
    """

    df = pd.DataFrame(
        {
            "hist_color": [
                # --- valid bullish loose pyramid ---
                "dark_red",        # boundary
                "dark_green",
                "light_green",
                "dark_green",      # allowed reversal
                "light_red",       # boundary

                # --- invalid: sandwiched dark (FAIL) ---
                "dark_red",
                "dark_green",
                "dark_red",

                # --- invalid: all dark ---
                "dark_red",
                "dark_green",
                "dark_green",
                "dark_red",

                # --- invalid: single bar ---
                "dark_red",
                "dark_green",
                "dark_red",

                # --- valid bearish loose pyramid ---
                "dark_green",      # boundary
                "dark_red",
                "light_red",
                "dark_red",        # allowed reversal
                "light_green",     # boundary

                # --- invalid: all light ---
                "dark_green",
                "light_red",
                "light_red",
                "dark_green",
            ]
        }
    )

    out = detect_loose_histogram_pyramids(
        df.copy(),
        hist_color_col="hist_color",
        out_col="is_loose_pyramid",
    ).df

    expected = [
        # --- valid bullish ---
        False,  # dark_red boundary
        True,   # dark_green
        True,   # light_green
        True,   # dark_green
        False,  # light_red boundary

        # --- sandwiched dark (FAIL) ---
        False,
        False,
        False,

        # --- all dark (FAIL) ---
        False,
        False,
        False,
        False,

        # --- single bar (FAIL) ---
        False,
        False,
        False,

        # --- valid bearish ---
        False,  # dark_green boundary
        True,   # dark_red
        True,   # light_red
        True,   # dark_red
        False,  # light_green boundary

        # --- all light (FAIL) ---
        False,
        False,
        False,
        False,
    ]

    assert out["is_loose_pyramid"].tolist() == expected
