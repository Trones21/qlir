from __future__ import annotations
from dataclasses import dataclass
import pandas as _pd
import numpy as _np
from qlir.core.registries.columns.announce_and_register import announce_column_lifecycle
from qlir.core.registries.columns.registry import ColKeyDecl, ColRegistry
from qlir.core.registries.columns.verify import verify_declared_cols_exist
from qlir.core.types.annotated_df import AnnotatedDF
from qlir.perf.df_copy import df_copy_measured
from qlir.perf.logging import log_memory_info
import logging
log = logging.getLogger(__name__)

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




@dataclass(frozen=True)
class PyrCols:
    apex_idx: str
    apex_val: str
    is_front: str
    is_back: str
    side: str         
    ord: str
    front_ord: str
    front_len: str
    back_ord: str
    back_len: str
    is_viol_front: str
    is_viol_back: str
    viol_any: str
    viol_front_run_dense: str
    viol_back_run_dense: str
    viol_front_run_sparse: str
    viol_back_run_sparse: str
    viol_total: str
    viol_front_total: str
    viol_back_total: str


def _pyr_cols(out_prefix: str) -> PyrCols:
    return PyrCols(
        apex_idx=f"{out_prefix}apex_idx",
        apex_val=f"{out_prefix}apex_val",
        is_front=f"is_{out_prefix}front",
        is_back=f"is_{out_prefix}back",
        side=f"{out_prefix}side",
        ord=f"{out_prefix}ord",
        front_ord=f"{out_prefix}front_ord",
        front_len=f"{out_prefix}front_len",
        back_ord=f"{out_prefix}back_ord",
        back_len=f"{out_prefix}back_len",
        is_viol_front=f"{out_prefix}is_viol_front",
        is_viol_back=f"{out_prefix}is_viol_back",
        viol_any=f"{out_prefix}viol_any",
        viol_front_run_dense=f"{out_prefix}viol_front_run_dense",
        viol_back_run_dense=f"{out_prefix}viol_back_run_dense",
        viol_front_run_sparse=f"{out_prefix}viol_front_run_sparse",
        viol_back_run_sparse=f"{out_prefix}viol_back_run_sparse",
        viol_front_total=f"{out_prefix}viol_front_total",
        viol_back_total=f"{out_prefix}viol_back_total",
        viol_total=f"{out_prefix}viol_total",
    )


def macd_full_pyramidal_annotation(
    df: _pd.DataFrame,
    *,
    hist_col: str,
    group_col: str,
    out_prefix: str = "pyr_",
) -> AnnotatedDF:
    """
    Annotating the areas between macd crosses (potential pyramids/mountains/valleys).

    Assumptions:
    - df is already segmented into pyramids via MACD zero-crosses
      (one pyramid == one condition_group_id)
    - hist_col sign is consistent within each group
    - df index is ordered (DatetimeIndex or monotonic integer index)

    This function:
    - identifies the true apex per pyramid (offline; lookahead allowed)
      NOTE: if the last group is still "open", apex is provisional = max(|hist|) observed so far.
    - splits each pyramid into front/back sides
    - assigns ordinal position per side
    - marks strict monotonicity violations per side

    No filtering, no trade logic, no causal assumptions.
    """
    out, mem_ev = df_copy_measured(df=df, label="macd_full_pyramidal_annotation")
    log_memory_info(ev=mem_ev, log=log)
    
    if hist_col not in out.columns:
        raise KeyError(f"hist_col={hist_col!r} not found in df")
    if group_col not in out.columns:
        raise KeyError(f"group_col={group_col!r} not found in df")

    # Ensure we can do reliable index comparisons
    if not out.index.is_monotonic_increasing:
        out = out.sort_index()

    abs_hist = out[hist_col].abs()

    new_cols = ColRegistry(owner="macd_full_pyramidal_annotation")
    cols = _pyr_cols(out_prefix)
    
    # ------------------------------------------------------------------
    # 0. Ordinal position within entire pyramid (needed for apex ordinal)
    # ------------------------------------------------------------------
    out[cols.ord] = out.groupby(group_col, sort=False).cumcount()

    abs_hist = out[hist_col].abs()

    # ------------------------------------------------------------------
    # 1. Identify apex per pyramid (ordinal position of max |hist|)
    # ------------------------------------------------------------------
    # Per-row max(|hist|) within group
    gmax = abs_hist.groupby(out[group_col], sort=False).transform("max")

    # Rows that hit the max (ties possible)
    is_apex_candidate = abs_hist.eq(gmax)

    # Apex ordinal = first ordinal in the group that hits the max
    apex_ord = (
        out[cols.ord]
        .where(is_apex_candidate)
        .groupby(out[group_col], sort=False)
        .transform("min")
    )

    # Broadcast to rows
    out[cols.apex_idx] = apex_ord  # NOTE: pyr_apex_idx is now ordinal, not datetime
    out[cols.apex_val] = gmax      # apex |hist| (same for all rows in group)

    # ------------------------------------------------------------------
    # 2. Classify rows as front/back relative to apex ordinal
    #    Apex is considered FRONT.
    # ------------------------------------------------------------------
    out[cols.is_front] = out[cols.ord] <= out[cols.apex_idx]  # <= includes apex
    out[cols.is_back]  = out[cols.ord] >  out[cols.apex_idx]

    # ------------------------------------------------------------------
    # 3. Side enum (string)
    # ------------------------------------------------------------------
    out[cols.side] = _np.where(out[cols.is_front].to_numpy(), "frontside", "backside")

    # ------------------------------------------------------------------
    # 4/5. Ordinal positions + lengths within front/back sides
    # ------------------------------------------------------------------
    out[cols.front_ord] = _np.nan
    out[cols.back_ord] = _np.nan
    out[cols.front_len] = 0
    out[cols.back_len] = 0

    front_mask = out[cols.is_front].to_numpy()
    back_mask = out[cols.is_back].to_numpy()

    if front_mask.any():
        out.loc[front_mask, cols.front_ord] = (
            out.loc[front_mask].groupby(group_col, sort=False).cumcount().to_numpy()
        )
        front_sizes = out.loc[front_mask].groupby(group_col, sort=False)[hist_col].transform("size")
        out.loc[front_mask, cols.front_len] = front_sizes.to_numpy()

    if back_mask.any():
        out.loc[back_mask, cols.back_ord] = (
            out.loc[back_mask].groupby(group_col, sort=False).cumcount().to_numpy()
        )
        back_sizes = out.loc[back_mask].groupby(group_col, sort=False)[hist_col].transform("size")
        out.loc[back_mask, cols.back_len] = back_sizes.to_numpy()

    # ------------------------------------------------------------------
    # 7/8. Monotonic violations on each side (strict expectation)
    # ------------------------------------------------------------------
    # Front: |hist| should be non-decreasing toward apex => diff < 0 is violation
    out[cols.is_viol_front] = False
    if front_mask.any():
        d_front = abs_hist.loc[front_mask].groupby(out.loc[front_mask, group_col], sort=False).diff()
        out.loc[front_mask, cols.is_viol_front] = (d_front < 0).fillna(False).to_numpy()

    # Back: |hist| should be non-increasing away from apex => diff > 0 is violation
    out[cols.is_viol_back] = False
    if back_mask.any():
        d_back = abs_hist.loc[back_mask].groupby(out.loc[back_mask, group_col], sort=False).diff()
        out.loc[back_mask, cols.is_viol_back] = (d_back > 0).fillna(False).to_numpy()

    # Optional derived convenience cols (keep front/back as ground truth)

    out[f"{out_prefix}viol_any"] = out[cols.is_viol_front] | out[cols.is_viol_back]

    # ==== Frontside - Running Counts (dense, sparse (to be implemented) and Total =====================

    out[cols.viol_front_run] = 0

    if front_mask.any():
        out.loc[front_mask, cols.viol_front_run] = (
            out.loc[front_mask, cols.is_viol_front]
                .groupby(out.loc[front_mask, group_col], sort=False)
                .cumsum()
                .astype("int64")
                .to_numpy()
        )

    out[cols.viol_front_total] = 0

    if front_mask.any():
        out.loc[front_mask, cols.viol_front_total] = (
            out.loc[front_mask, cols.viol_front_run]
                .groupby(out.loc[front_mask, group_col], sort=False)
                .transform("max")
                .astype("int64")
                .to_numpy()
        )

    out[f"{out_prefix}viol_front_total"] = out[cols.viol_front_total].astype(_np.int8) 
    
    # ==== Backside - Running Counts (dense, sparse (to be implemented) and Total =====================

    out[cols.viol_back_run] = 0

    if back_mask.any():
        out.loc[back_mask, cols.viol_back_run] = (
            out.loc[back_mask, cols.is_viol_back]
                .groupby(out.loc[back_mask, group_col], sort=False)
                .cumsum()
                .astype("int64")
                .to_numpy()
        )

    out[cols.viol_back_total] = 0

    if back_mask.any():
        out.loc[back_mask, cols.viol_back_total] = (
            out.loc[back_mask, cols.viol_back_run]
                .groupby(out.loc[back_mask, group_col], sort=False)
                .transform("max")
                .astype("int64")
                .to_numpy()
        )

    out[f"{out_prefix}viol_back_total"] = out[cols.viol_back_total].astype(_np.int8) 
    
    
    out[f"{out_prefix}viol_total"] = "tbimp"


    announce_column_lifecycle(
        caller="macd_full_pyramidal_annotation",
        registry=new_cols,
        decls=[
            ColKeyDecl("pyr_apex_idx", cols.apex_idx),
            ColKeyDecl("pyr_apex_val", cols.apex_val),
            ColKeyDecl("is_pyr_front", cols.is_front),
            ColKeyDecl("is_pyr_back", cols.is_back),
            ColKeyDecl("pyr_side", cols.side),
            ColKeyDecl("pyr_ord", cols.ord),
            ColKeyDecl("pyr_front_ord", cols.front_ord),
            ColKeyDecl("pyr_front_len", cols.front_len),
            ColKeyDecl("pyr_back_ord", cols.back_ord),
            ColKeyDecl("pyr_back_len", cols.back_len),
            ColKeyDecl("pyr_is_viol_front", cols.is_viol_front),
            ColKeyDecl("pyr_is_viol_back", cols.is_viol_back),
            # some derived outputs:
            ColKeyDecl("pyr_viol_any", f"{out_prefix}viol_any"),
            ColKeyDecl("pyr_viol_front_run_dense", f"{out_prefix}viol_front_run_dense"),
            ColKeyDecl("pyr_viol_back_run_dense", f"{out_prefix}viol_back_run_dense"),
            ColKeyDecl("pyr_viol_front_run_sparse", f"{out_prefix}viol_front_run_sparse"),
            ColKeyDecl("pyr_viol_front_run_sparse", f"{out_prefix}viol_front_run_sparse"),
            ColKeyDecl("pyr_viol_front_total", f"{out_prefix}viol_front_total"),
            ColKeyDecl("pyr_viol_back_total", f"{out_prefix}viol_back_total"),
            ColKeyDecl("pyr_viol_total", f"{out_prefix}viol_total"),
        ],
        event="created",
    )
    verify_declared_cols_exist(df=out, registry=new_cols, caller="macd_full_pyramidal_annotation")
    return AnnotatedDF(df=out, new_cols=new_cols, label="macd_full_pyramidal_annotation")


# def macd_full_pyramidal_annotation(
#     df: pd.DataFrame,
#     *,
#     hist_col: str,
#     group_col: str,
#     out_prefix: str = "pyr_",
# ) -> pd.DataFrame:
#     """
#     Annotating the areas between macd crosses (potential pyramids/mountains/valleys).

#     Assumptions:
#     - df is already segmented into pyramids via MACD zero-crosses
#       (one pyramid == one condition_group_id)
#     - hist_col sign is consistent within each group
#     - df index is ordered (DatetimeIndex or monotonic integer index)

#     This function:
#     - identifies the true apex per pyramid
#     - splits each pyramid into front/back sides
#     - assigns ordinal position per side
#     - marks strict monotonicity violations per side

#     No filtering, no trade logic, no causal assumptions.
#     """

#     out = df_copy_measured(df=df, label="macd_full_pyramidal_annotation")

#     raise NotImplementedError("Need to write this func")

#     new_cols = ColRegistry()

#     # ------------------------------------------------------------------
#     # 1. Identify apex per pyramid (true max |hist| inside each group)
#     # ------------------------------------------------------------------
#     #
#     # For each condition_group_id:
#     #   - locate the row index where |hist| is maximal
#     #   - persist apex index and apex value to all rows in the group
#     #
#     # New columns:
#     #   - {out_prefix}_apex_idx
#     #   - {out_prefix}_apex_val
#     #
#     # NOTE:
#     # - apex is defined offline (lookahead allowed)
#     #

#     # TODO: groupby(group_col)
#     # TODO: compute idx of max abs(hist_col)
#     # TODO: broadcast apex_idx and apex_val back to rows

#     # ------------------------------------------------------------------
#     # 2. Classify rows as front / back side of pyramid
#     # ------------------------------------------------------------------
#     #
#     # Front side: row index < apex_idx
#     # Back side : row index > apex_idx
#     # Apex row  : neither (can be left False/False)
#     #
#     # New columns:
#     #   - is_{out_prefix}_front
#     #   - is_{out_prefix}_back
#     #

#     # TODO: boolean masks using index vs apex_idx

#     # ------------------------------------------------------------------
#     # 3. Ordinal position within entire pyramid (optional but useful)
#     # ------------------------------------------------------------------
#     #
#     # Ordinal index from start of pyramid to end
#     #
#     # New column (optional):
#     #   - {out_prefix}_ord
#     #

#     # TODO: with_counts or cumcount per group_col

#     # ------------------------------------------------------------------
#     # 4. Ordinal position within FRONT side
#     # ------------------------------------------------------------------
#     #
#     # Only rows where is_{out_prefix}_front == True
#     #
#     # Ordinal index resets at start of front side
#     # Length is total number of front-side rows per pyramid
#     #
#     # New columns:
#     #   - {out_prefix}_front_ord
#     #   - {out_prefix}_front_len
#     #

#     # TODO: mask front rows
#     # TODO: apply with_counts (or equivalent) per group_col on front rows

#     # ------------------------------------------------------------------
#     # 5. Ordinal position within BACK side
#     # ------------------------------------------------------------------
#     #
#     # Only rows where is_{out_prefix}_back == True
#     #
#     # Ordinal index resets immediately after apex
#     # Length is total number of back-side rows per pyramid
#     #
#     # New columns:
#     #   - {out_prefix}_back_ord
#     #   - {out_prefix}_back_len
#     #

#     # TODO: mask back rows
#     # TODO: apply with_counts per group_col on back rows

#     # ------------------------------------------------------------------
#     # 6. Define strict monotonic expectations (conceptual)
#     # ------------------------------------------------------------------
#     #
#     # Front side expectation:
#     #   |hist| should be non-decreasing toward the apex
#     #
#     # Back side expectation:
#     #   |hist| should be non-increasing away from the apex
#     #
#     # No columns added here — just rule definition for violations.
#     #

#     # ------------------------------------------------------------------
#     # 7. Mark violations on FRONT side
#     # ------------------------------------------------------------------
#     #
#     # For each pyramid front side:
#     #   - compare |hist| to previous FRONT-side bar
#     #   - if |hist| decreases → violation
#     #
#     # New column:
#     #   - {out_prefix}_viol_front (bool or 0/1)
#     #

#     # TODO: groupby(group_col)
#     # TODO: diff or shift on abs(hist_col) within front side
#     # TODO: set viol_front accordingly

#     # ------------------------------------------------------------------
#     # 8. Mark violations on BACK side
#     # ------------------------------------------------------------------
#     #
#     # For each pyramid back side:
#     #   - compare |hist| to previous BACK-side bar
#     #   - if |hist| increases → violation
#     #
#     # New column:
#     #   - {out_prefix}_viol_back (bool or 0/1)
#     #

#     # TODO: groupby(group_col)
#     # TODO: diff or shift on abs(hist_col) within back side
#     # TODO: set viol_back accordingly

#     # ------------------------------------------------------------------
#     # 9. (Optional) Derived convenience columns
#     # ------------------------------------------------------------------
#     #
#     # These should be DERIVED, not primary:
#     #
#     #   - {out_prefix}_viol_any
#     #   - {out_prefix}_viol_total
#     #
#     # Keep front/back separated as the ground truth.
#     #

#     # TODO: optional derived columns

#     return out
