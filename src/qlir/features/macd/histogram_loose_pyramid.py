
import pandas as _pd
from qlir.core.registries.columns.announce_and_register import announce_column_lifecycle
from qlir.core.registries.columns.registry import ColKeyDecl, ColRegistry
from qlir.core.types.annotated_df import AnnotatedDF


GREEN = {"dark_green", "light_green"}
RED = {"dark_red", "light_red"}


def detect_loose_histogram_pyramids(
    df: _pd.DataFrame,
    *,
    hist_color_col: str,
    out_col: str = "is_loose_histogram_pyramid",
) -> AnnotatedDF:
    """
    Detects loose histogram pyramids.

    A loose pyramid is any contiguous segment between histogram sign changes
    that:
      - has length >= 2
      - contains at least one dark bar
      - contains at least one light bar
      - allows internal expansion/contraction reversals
    """
    new_cols = ColRegistry()

    colors = df[hist_color_col].tolist()
    n = len(colors)

    result = [False] * n

    def sign(c: str) -> str:
        if "green" in c:
            return "green"
        if "red" in c:
            return "red"
        raise ValueError(f"unknown color: {c}")

    def is_dark(c: str) -> bool:
        return c.startswith("dark")

    def is_light(c: str) -> bool:
        return c.startswith("light")

    i = 0
    while i < n:
        start = i
        sgn = sign(colors[i])

        j = i + 1
        while j < n and sign(colors[j]) == sgn:
            j += 1

        # segment is [start, j)
        segment = colors[start:j]

        if len(segment) >= 2:
            has_dark = any(is_dark(c) for c in segment)
            has_light = any(is_light(c) for c in segment)

            if has_dark and has_light:
                for k in range(start, j):
                    result[k] = True

        i = j

    df[out_col] = result

    announce_column_lifecycle(
        caller="detect_loose_histogram_pyramids",
        registry=new_cols,
        decls=[
            ColKeyDecl(key="loose_histogram_pyramid", column=out_col),
        ],
        event="created",
    )

    return AnnotatedDF(
        df=df,
        new_cols=new_cols,
        label="detect_loose_histogram_pyramids",
    )


def detect_loose_green_histogram_pyramids(
    df: _pd.DataFrame,
    *,
    hist_color_col: str,
    out_col: str = "is_loose_green_pyramid",
) -> AnnotatedDF:
    """
    Loose histogram pyramids, bullish direction only.
    """
    adf = detect_loose_histogram_pyramids(
        df,
        hist_color_col=hist_color_col,
        out_col="_tmp_loose_pyramid",
    )

    df = adf.df
    new_cols = ColRegistry()

    df[out_col] = (
        df["_tmp_loose_pyramid"]
        & df[hist_color_col].isin(GREEN)
    )

    df.drop(columns=["_tmp_loose_pyramid"], inplace=True)

    announce_column_lifecycle(
        caller="detect_loose_green_histogram_pyramids",
        registry=new_cols,
        decls=[ColKeyDecl(key="loose_green", column=out_col)],
        event="created",
    )

    return AnnotatedDF(df=df, new_cols=new_cols, label="detect_loose_green_histogram_pyramids")

def detect_loose_red_histogram_pyramids(
    df: _pd.DataFrame,
    *,
    hist_color_col: str,
    out_col: str = "is_loose_red_pyramid",
) -> AnnotatedDF:
    """
    Loose histogram pyramids, bearish direction only.
    """
    adf = detect_loose_histogram_pyramids(
        df,
        hist_color_col=hist_color_col,
        out_col="_tmp_loose_pyramid",
    )

    df = adf.df
    new_cols = ColRegistry()

    df[out_col] = (
        df["_tmp_loose_pyramid"]
        & df[hist_color_col].isin(RED)
    )

    df.drop(columns=["_tmp_loose_pyramid"], inplace=True)

    announce_column_lifecycle(
        caller="detect_loose_red_histogram_pyramids",
        registry=new_cols,
        decls=[ColKeyDecl(key="loose_red", column=out_col)],
        event="created",
    )

    return AnnotatedDF(df=df, new_cols=new_cols, label="detect_loose_red_histogram_pyramids")
