import pandas as _pd
from qlir.core.registries.columns.announce_and_register import announce_column_lifecycle
from qlir.core.registries.columns.registry import ColKeyDecl, ColRegistry
from qlir.core.types.annotated_df import AnnotatedDF
from qlir.perf.df_copy import df_copy_measured

GREEN = {"dark_green", "light_green"}
RED = {"dark_red", "light_red"}


def detect_histogram_pyramids(
    df: _pd.DataFrame,
    *,
    hist_color_col: str,
    out_col: str = "is_histogram_pyramid",
) -> AnnotatedDF:
    """
    Marks bars that are part of a *histogram pyramid*.

    A pyramid is exactly:
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
        caller="detect_histogram_pyramids",
        registry=new_cols,
        decls=[
            ColKeyDecl(key="pyramid", column=out_col),
        ],
        event="created",
    )

    return AnnotatedDF(
        df=df,
        new_cols=new_cols,
        label="detect_histogram_pyramids",
    )


def detect_strict_green_histogram_pyramids(
    df: _pd.DataFrame,
    *,
    hist_color_col: str,
    out_col: str = "is_strict_green_pyramid",
) -> AnnotatedDF:
    """
    Strict histogram pyramids, bullish direction only.
    """
    adf = detect_histogram_pyramids(
        df,
        hist_color_col=hist_color_col,
        out_col="_tmp_strict_pyramid",
    )

    df = adf.df
    new_cols = ColRegistry()

    df[out_col] = (
        df["_tmp_strict_pyramid"]
        & df[hist_color_col].isin(GREEN)
    )

    df.drop(columns=["_tmp_strict_pyramid"], inplace=True)

    announce_column_lifecycle(
        caller="detect_strict_green_histogram_pyramids",
        registry=new_cols,
        decls=[ColKeyDecl(key="strict_green", column=out_col)],
        event="created",
    )

    return AnnotatedDF(df=df, new_cols=new_cols, label="detect_strict_green_histogram_pyramids")


def detect_strict_red_histogram_pyramids(
    df: _pd.DataFrame,
    *,
    hist_color_col: str,
    out_col: str = "is_strict_red_pyramid",
) -> AnnotatedDF:
    """
    Strict histogram pyramids, bearish direction only.
    """
    adf = detect_histogram_pyramids(
        df,
        hist_color_col=hist_color_col,
        out_col="_tmp_strict_pyramid",
    )

    df = adf.df
    new_cols = ColRegistry()

    df[out_col] = (
        df["_tmp_strict_pyramid"]
        & df[hist_color_col].isin(RED)
    )

    df.drop(columns=["_tmp_strict_pyramid"], inplace=True)

    announce_column_lifecycle(
        caller="detect_strict_red_histogram_pyramids",
        registry=new_cols,
        decls=[ColKeyDecl(key="strict_red", column=out_col)],
        event="created",
    )

    return AnnotatedDF(df=df, new_cols=new_cols, label="detect_strict_red_histogram_pyramids")


import pandas as pd
import numpy as np


def macd_full_pyramidal_annotation(
    df: pd.DataFrame,
    *,
    hist_col: str,
    group_col: str,
    out_prefix: str = "pyr_",
) -> pd.DataFrame:
    """
    Annotating the areas between macd crosses (potential pyramids/mountains/valleys).

    Assumptions:
    - df is already segmented into pyramids via MACD zero-crosses
      (one pyramid == one condition_group_id)
    - hist_col sign is consistent within each group
    - df index is ordered (DatetimeIndex or monotonic integer index)

    This function:
    - identifies the true apex per pyramid
    - splits each pyramid into front/back sides
    - assigns ordinal position per side
    - marks strict monotonicity violations per side

    No filtering, no trade logic, no causal assumptions.
    """

    out = df_copy_measured(df=df, label="macd_full_pyramidal_annotation")

    raise NotImplementedError("Need to write this func")

    new_cols = ColRegistry()

    # ------------------------------------------------------------------
    # 1. Identify apex per pyramid (true max |hist| inside each group)
    # ------------------------------------------------------------------
    #
    # For each condition_group_id:
    #   - locate the row index where |hist| is maximal
    #   - persist apex index and apex value to all rows in the group
    #
    # New columns:
    #   - {out_prefix}_apex_idx
    #   - {out_prefix}_apex_val
    #
    # NOTE:
    # - apex is defined offline (lookahead allowed)
    #

    # TODO: groupby(group_col)
    # TODO: compute idx of max abs(hist_col)
    # TODO: broadcast apex_idx and apex_val back to rows

    # ------------------------------------------------------------------
    # 2. Classify rows as front / back side of pyramid
    # ------------------------------------------------------------------
    #
    # Front side: row index < apex_idx
    # Back side : row index > apex_idx
    # Apex row  : neither (can be left False/False)
    #
    # New columns:
    #   - is_{out_prefix}_front
    #   - is_{out_prefix}_back
    #

    # TODO: boolean masks using index vs apex_idx

    # ------------------------------------------------------------------
    # 3. Ordinal position within entire pyramid (optional but useful)
    # ------------------------------------------------------------------
    #
    # Ordinal index from start of pyramid to end
    #
    # New column (optional):
    #   - {out_prefix}_ord
    #

    # TODO: with_counts or cumcount per group_col

    # ------------------------------------------------------------------
    # 4. Ordinal position within FRONT side
    # ------------------------------------------------------------------
    #
    # Only rows where is_{out_prefix}_front == True
    #
    # Ordinal index resets at start of front side
    # Length is total number of front-side rows per pyramid
    #
    # New columns:
    #   - {out_prefix}_front_ord
    #   - {out_prefix}_front_len
    #

    # TODO: mask front rows
    # TODO: apply with_counts (or equivalent) per group_col on front rows

    # ------------------------------------------------------------------
    # 5. Ordinal position within BACK side
    # ------------------------------------------------------------------
    #
    # Only rows where is_{out_prefix}_back == True
    #
    # Ordinal index resets immediately after apex
    # Length is total number of back-side rows per pyramid
    #
    # New columns:
    #   - {out_prefix}_back_ord
    #   - {out_prefix}_back_len
    #

    # TODO: mask back rows
    # TODO: apply with_counts per group_col on back rows

    # ------------------------------------------------------------------
    # 6. Define strict monotonic expectations (conceptual)
    # ------------------------------------------------------------------
    #
    # Front side expectation:
    #   |hist| should be non-decreasing toward the apex
    #
    # Back side expectation:
    #   |hist| should be non-increasing away from the apex
    #
    # No columns added here — just rule definition for violations.
    #

    # ------------------------------------------------------------------
    # 7. Mark violations on FRONT side
    # ------------------------------------------------------------------
    #
    # For each pyramid front side:
    #   - compare |hist| to previous FRONT-side bar
    #   - if |hist| decreases → violation
    #
    # New column:
    #   - {out_prefix}_viol_front (bool or 0/1)
    #

    # TODO: groupby(group_col)
    # TODO: diff or shift on abs(hist_col) within front side
    # TODO: set viol_front accordingly

    # ------------------------------------------------------------------
    # 8. Mark violations on BACK side
    # ------------------------------------------------------------------
    #
    # For each pyramid back side:
    #   - compare |hist| to previous BACK-side bar
    #   - if |hist| increases → violation
    #
    # New column:
    #   - {out_prefix}_viol_back (bool or 0/1)
    #

    # TODO: groupby(group_col)
    # TODO: diff or shift on abs(hist_col) within back side
    # TODO: set viol_back accordingly

    # ------------------------------------------------------------------
    # 9. (Optional) Derived convenience columns
    # ------------------------------------------------------------------
    #
    # These should be DERIVED, not primary:
    #
    #   - {out_prefix}_viol_any
    #   - {out_prefix}_viol_total
    #
    # Keep front/back separated as the ground truth.
    #

    # TODO: optional derived columns

    return out
