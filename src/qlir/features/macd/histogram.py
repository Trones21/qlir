import pandas as _pd
from qlir.core.registries.columns.announce_and_register import announce_column_lifecycle
from qlir.core.registries.columns.registry import ColKeyDecl, ColRegistry
from qlir.core.types.annotated_df import AnnotatedDF
from qlir.df.utils import _ensure_columns


def with_colored_histogram(
    df: _pd.DataFrame,
    *,
    hist_col: str = "macd_hist",
    prefix: str = "macd",
    as_int: bool = False,
) -> AnnotatedDF:
    """
    Encodes MACD histogram acceleration state using N-1 expansion logic.

    Inputs:
        hist_col : MACD histogram column (macd_line - signal_line)

    Outputs:
        {prefix}_hist_abs   : absolute histogram magnitude
        {prefix}_hist_color : expansion-aware color
        {prefix}_rg         : regime-only color ("green"|"red")

    String encoding:
        dark_green  = bullish acceleration increasing
        light_green = bullish acceleration decreasing
        light_red   = bearish acceleration decreasing
        dark_red    = bearish acceleration increasing

    Integer encoding:
        +2 = dark green
        +1 = light green
        -1 = light red
        -2 = dark red
    """

    new_cols = ColRegistry()

    hist_abs_col = f"{prefix}_hist_abs"
    color_col = f"{prefix}_hist_color"
    rg_col = f"{prefix}_rg"

    _ensure_columns(df=df, cols=hist_col, caller="with_colored_histogram")

    hist = df[hist_col]

    # --- core math ---
    df[hist_abs_col] = hist.abs()

    expanding = df[hist_abs_col] > df[hist_abs_col].shift(1)
    bullish = hist > 0

    mag = expanding.map({True: 2, False: 1})
    sign = bullish.map({True: 1, False: -1})

    color_int = mag * sign

    # --- regime only ---
    df[rg_col] = color_int.map({
        2: "green",
        1: "green",
        -1: "red",
        -2: "red",
    })

    # --- final encoding ---
    if as_int:
        df[color_col] = color_int
    else:
        df[color_col] = color_int.map({
            2: "dark_green",
            1: "light_green",
            -1: "light_red",
            -2: "dark_red",
        })

    announce_column_lifecycle(
        caller="macd_histogram_color",
        registry=new_cols,
        decls=[
            ColKeyDecl(key="hist_abs", column=hist_abs_col),
            ColKeyDecl(key="hist_color", column=color_col),
            ColKeyDecl(key="rg", column=rg_col),
        ],
        event="created",
    )

    return AnnotatedDF(df=df, new_cols=new_cols, label="with_colored_histogram")


def mark_segment_max_excursion(
    df: _pd.DataFrame,
    *,
    value_col: str,
    sign_col: str,
    out_col: str = "is_segment_max_excursion",
    min_segment_len: int = 3,
) -> AnnotatedDF:
    """
    Marks the bar of maximum excursion within each contiguous sign segment.

    A segment is defined by constant `sign_col` (e.g. histogram sign).
    Only segments with length >= min_segment_len are considered.

    Exactly one bar per qualifying segment is marked True.
    All other rows are False.
    """
    new_cols = ColRegistry()

    values = df[value_col].tolist()
    signs = df[sign_col].tolist()
    n = len(values)

    result = [False] * n

    i = 0
    while i < n:
        start = i
        sgn = signs[i]

        j = i + 1
        while j < n and signs[j] == sgn:
            j += 1

        # segment is [start, j)
        seg_len = j - start
        if seg_len >= min_segment_len:
            seg_vals = values[start:j]

            # index of max excursion (first occurrence)
            k = max(range(seg_len), key=lambda x: abs(seg_vals[x]))

            result[start + k] = True

        i = j

    df[out_col] = result

    announce_column_lifecycle(
        caller="mark_segment_max_excursion",
        registry=new_cols,
        decls=[
            ColKeyDecl(key="segment_max_excursion", column=out_col),
        ],
        event="created",
    )

    return AnnotatedDF(
        df=df,
        new_cols=new_cols,
        label="mark_segment_max_excursion",
    )



# How to use the max excursion
# absolute excursion per histogram segment
# mark_segment_max_excursion(
#     df,
#     value_col="macd_hist_abs",
#     sign_col="macd_hist_positive",
# )

# # signed excursion peak (still works)
# mark_segment_max_excursion(
#     df,
#     value_col="macd_hist",
#     sign_col="macd_hist_positive",
# )