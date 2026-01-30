import pandas as _pd
from qlir.core.registries.columns.announce_and_register import announce_column_lifecycle
from qlir.core.registries.columns.registry import ColKeyDecl, ColRegistry
from qlir.core.types.annotated_df import AnnotatedDF


def with_colored_histogram(
    df: _pd.DataFrame,
    *,
    fast_col: str,
    slow_col: str,
    prefix: str = "macd",
    as_int: bool = False,
) -> AnnotatedDF:
    """
    Encodes MACD fast/slow distance with N-1 expansion logic.

    Outputs:
        {prefix}_dist       : signed distance (fast - slow)
        {prefix}_dist_abs   : absolute distance
        {prefix}_dist_color : expansion-aware color

    String encoding (default):
        "dark_green"  = bullish, expanding
        "light_green" = bullish, contracting
        "light_red"   = bearish, contracting
        "dark_red"    = bearish, expanding

    Integer encoding (as_int=True):
        +2 = dark green
        +1 = light green
        -1 = light red
        -2 = dark red
    """
    new_cols = ColRegistry()

    dist_col = f"{prefix}_dist"
    dist_abs_col = f"{prefix}_dist_abs"
    color_col = f"{prefix}_dist_color"

    # --- core math ---
    df[dist_col] = df[fast_col] - df[slow_col]
    df[dist_abs_col] = df[dist_col].abs()

    expanding = df[dist_abs_col] > df[dist_abs_col].shift(1)
    bullish = df[dist_col] > 0

    mag = expanding.map({True: 2, False: 1})
    sign = bullish.map({True: 1, False: -1})

    color_int = mag * sign

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
        caller="macd_distance_color",
        registry=new_cols,
        decls=[
            ColKeyDecl(key="dist", column=dist_col),
            ColKeyDecl(key="dist_abs", column=dist_abs_col),
            ColKeyDecl(key="dist_color", column=color_col),
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