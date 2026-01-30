import pandas as _pd

from qlir.core.registries.columns.announce_and_register import announce_column_lifecycle
from qlir.core.registries.columns.registry import ColKeyDecl, ColRegistry
from qlir.core.types.annotated_df import AnnotatedDF


def detect_strict_crossing_sequences(
    df: _pd.DataFrame,
    *,
    hist_color_col: str,
    require_extrema: bool,
    out_col: str,
) -> AnnotatedDF:
    """
    Detects strict histogram crossing sequences.

    If require_extrema=True:
      Enforces dark → light+ → dark+ → light boundaries.

    If require_extrema=False:
      Enforces light+ → dark+ monotonic crossing only.

    Interior bars are marked True.
    """
    new_cols = ColRegistry()

    colors = df[hist_color_col].tolist()
    n = len(colors)

    result = [False] * n

    i = 0
    while i < n - 1:
        c = colors[i]

        # ---------- green -> red ----------
        if c == "light_green":
            j = i
            while j < n and colors[j] == "light_green":
                j += 1

            if j >= n or colors[j] != "dark_red":
                i += 1
                continue

            k = j
            while k < n and colors[k] == "dark_red":
                k += 1

            if require_extrema:
                if i - 1 < 0 or k >= n:
                    i += 1
                    continue
                if colors[i - 1] != "dark_green" or colors[k] != "light_red":
                    i += 1
                    continue

            for idx in range(i, k):
                result[idx] = True

            i = k
            continue

        # ---------- red -> green ----------
        if c == "light_red":
            j = i
            while j < n and colors[j] == "light_red":
                j += 1

            if j >= n or colors[j] != "dark_green":
                i += 1
                continue

            k = j
            while k < n and colors[k] == "dark_green":
                k += 1

            if require_extrema:
                if i - 1 < 0 or k >= n:
                    i += 1
                    continue
                if colors[i - 1] != "dark_red" or colors[k] != "light_green":
                    i += 1
                    continue

            for idx in range(i, k):
                result[idx] = True

            i = k
            continue

        i += 1

    df[out_col] = result

    announce_column_lifecycle(
        caller="detect_strict_crossing_sequences",
        registry=new_cols,
        decls=[ColKeyDecl(key=out_col, column=out_col)],
        event="created",
    )

    return AnnotatedDF(
        df=df,
        new_cols=new_cols,
        label="detect_strict_crossing_sequences",
    )


def detect_strict_extrema_crossings(
    df: _pd.DataFrame,
    *,
    hist_color_col: str,
    out_col: str = "is_strict_extrema_crossing",
) -> AnnotatedDF:
    return detect_strict_crossing_sequences(
        df,
        hist_color_col=hist_color_col,
        require_extrema=True,
        out_col=out_col,
    )

def detect_strict_crossings(
    df: _pd.DataFrame,
    *,
    hist_color_col: str,
    out_col: str = "is_strict_crossing",
) -> AnnotatedDF:
    return detect_strict_crossing_sequences(
        df,
        hist_color_col=hist_color_col,
        require_extrema=False,
        out_col=out_col,
    )
