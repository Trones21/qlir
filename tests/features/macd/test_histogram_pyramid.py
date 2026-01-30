import pandas as pd
from qlir.core.registries.columns.announce_and_register import announce_column_lifecycle
from qlir.core.registries.columns.registry import ColKeyDecl, ColRegistry
from qlir.core.types.annotated_df import AnnotatedDF


def detect_histogram_pyramids(
    df: pd.DataFrame,
    *,
    hist_color_col: str,
    out_col: str = "is_perfect_pyramid",
) -> AnnotatedDF:
    """
    Marks bars that are part of a *perfect histogram pyramid*.

    A perfect pyramid is exactly:
      RED → dark_green+ → light_green+ → RED
      GREEN → dark_red+ → light_red+ → GREEN

    Only the interior expanding/contracting bars are marked True.
    All other rows are explicitly marked False.
    """
    new_cols = ColRegistry()

    colors = df[hist_color_col].tolist()
    n = len(colors)

    # default: every row is NOT a pyramid
    result = [False] * n

    GREEN = {"dark_green", "light_green"}
    RED = {"dark_red", "light_red"}

    i = 1  # need a previous bar for boundary detection
    while i < n - 1:
        prev = colors[i - 1]
        cur = colors[i]

        # ---------- bullish pyramid ----------
        if prev in RED and cur == "dark_green":
            start = i
            j = i

            while j < n and colors[j] == "dark_green":
                j += 1

            if j >= n or colors[j] != "light_green":
                i += 1
                continue

            while j < n and colors[j] == "light_green":
                j += 1

            if j < n and colors[j] in RED:
                for k in range(start, j):
                    result[k] = True
                i = j
                continue

        # ---------- bearish pyramid ----------
        if prev in GREEN and cur == "dark_red":
            start = i
            j = i

            while j < n and colors[j] == "dark_red":
                j += 1

            if j >= n or colors[j] != "light_red":
                i += 1
                continue

            while j < n and colors[j] == "light_red":
                j += 1

            if j < n and colors[j] in GREEN:
                for k in range(start, j):
                    result[k] = True
                i = j
                continue

        i += 1

    df[out_col] = result

    announce_column_lifecycle(
        caller="detect_perfect_histogram_pyramids",
        registry=new_cols,
        decls=[
            ColKeyDecl(key="perfect_pyramid", column=out_col),
        ],
        event="created",
    )

    return AnnotatedDF(
        df=df,
        new_cols=new_cols,
        label="detect_perfect_histogram_pyramids",
    )
