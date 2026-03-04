from __future__ import annotations
from dataclasses import dataclass
from typing import Iterable
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

    require_cols(out, [hist_col, group_col], caller="macd_full_pyramidal_annotation")
    out = ensure_monotonic_index(out)

    cols = _pyr_cols(out_prefix)
    new_cols = ColRegistry(owner="macd_full_pyramidal_annotation")

    pyr_mark_ord(out, group_col=group_col, cols=cols, caller="pyr_mark_ord")
    abs_hist = pyr_mark_offline_apex(out, hist_col=hist_col, group_col=group_col, cols=cols, caller="pyr_mark_offline_apex")
    pyr_mark_sides(out, cols=cols, caller="pyr_mark_sides")
    pyr_mark_side_ord_and_len(out, hist_col=hist_col, group_col=group_col, cols=cols, caller="pyr_mark_side_ord_and_len")
    pyr_mark_monotonic_violations(out, abs_hist=abs_hist, group_col=group_col, cols=cols, caller="pyr_mark_monotonic_violations")
    mark_side_apex_events(out, 
                          group_cols=group_col, 
                          side_col='pyr_side', 
                          color_col='macd_hist_color', 
                          ord_col= 'pyr_ord', 
                          main_apex_ord_col='pyr_apex_idx',
                          out_front_event_col="is_pyr_frt_local_apex",
                          out_back_event_col='is_pyr_back_local_apex',
                          out_front_event_ord_col='',
                          out_back_event_ord_col='',
                          add_event_ord=True)
    pyr_mark_dense_runs_and_totals(out, group_col=group_col, cols=cols, caller="pyr_mark_dense_runs_and_totals")

    # TODO: sparse runs / totals
    out[f"{out_prefix}viol_back_run_sparse"] = "tbimp"
    out[f"{out_prefix}viol_front_run_sparse"] = "tbimp"
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



def require_cols(df: _pd.DataFrame, cols: Iterable[str], *, caller: str) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise KeyError(f"{caller}: missing required columns: {missing}")

def ensure_monotonic_index(df: _pd.DataFrame) -> _pd.DataFrame:
    return df if df.index.is_monotonic_increasing else df.sort_index()

def pyr_mark_ord(out: _pd.DataFrame, *, group_col: str, cols: PyrCols, caller: str) -> None:
    require_cols(out, [group_col], caller=caller)
    out[cols.ord] = out.groupby(group_col, sort=False).cumcount()

def pyr_mark_offline_apex(
    out: _pd.DataFrame,
    *,
    hist_col: str,
    group_col: str,
    cols: PyrCols,
    caller: str,
) -> _pd.Series:
    require_cols(out, [hist_col, group_col, cols.ord], caller=caller)

    abs_hist = out[hist_col].abs()

    # per-row group max
    gmax = abs_hist.groupby(out[group_col], sort=False).transform("max")

    # first ordinal that hits the max (ties -> earliest)
    apex_ord = (
        out[cols.ord]
        .where(abs_hist.eq(gmax))
        .groupby(out[group_col], sort=False)
        .transform("min")
    )

    out[cols.apex_idx] = apex_ord           # ordinal (int-like, maybe float due to NaN)
    out[cols.apex_val] = gmax               # broadcast |hist| max

    return abs_hist


def pyr_mark_sides(out: _pd.DataFrame, *, cols: PyrCols, caller: str) -> None:
    require_cols(out, [cols.ord, cols.apex_idx], caller=caller)

    out[cols.is_front] = out[cols.ord] <= out[cols.apex_idx]  # apex included
    out[cols.is_back]  = out[cols.ord] >  out[cols.apex_idx]

    out[cols.side] = _np.where(out[cols.is_front].to_numpy(), "frontside", "backside")



def pyr_mark_side_ord_and_len(
    out: _pd.DataFrame,
    *,
    hist_col: str,
    group_col: str,
    cols: PyrCols,
    caller: str,
) -> None:
    require_cols(out, [hist_col, group_col, cols.is_front, cols.is_back], caller=caller)

    n = len(out)

    front_ord = _np.full(n, _np.nan, dtype=float)
    back_ord  = _np.full(n, _np.nan, dtype=float)
    front_len = _np.zeros(n, dtype=_np.int64)
    back_len  = _np.zeros(n, dtype=_np.int64)

    front_mask = out[cols.is_front].to_numpy()
    back_mask  = out[cols.is_back].to_numpy()

    if front_mask.any():
        front_df = out.loc[front_mask]
        front_ord[front_mask] = front_df.groupby(group_col, sort=False).cumcount().to_numpy()
        front_len[front_mask] = front_df.groupby(group_col, sort=False)[hist_col].transform("size").to_numpy()

    if back_mask.any():
        back_df = out.loc[back_mask]
        back_ord[back_mask] = back_df.groupby(group_col, sort=False).cumcount().to_numpy()
        back_len[back_mask] = back_df.groupby(group_col, sort=False)[hist_col].transform("size").to_numpy()

    out[cols.front_ord] = front_ord
    out[cols.back_ord] = back_ord
    out[cols.front_len] = front_len
    out[cols.back_len] = back_len



def pyr_mark_monotonic_violations(
    out: _pd.DataFrame,
    *,
    abs_hist: _pd.Series,
    group_col: str,
    cols: PyrCols,
    caller: str,
) -> None:
    require_cols(out, [group_col, cols.is_front, cols.is_back], caller=caller)

    out[cols.is_viol_front] = False
    out[cols.is_viol_back] = False

    front_mask = out[cols.is_front].to_numpy()
    back_mask  = out[cols.is_back].to_numpy()

    if front_mask.any():
        d_front = abs_hist.loc[front_mask].groupby(out.loc[front_mask, group_col], sort=False).diff()
        out.loc[front_mask, cols.is_viol_front] = (d_front < 0).fillna(False).to_numpy()

    if back_mask.any():
        d_back = abs_hist.loc[back_mask].groupby(out.loc[back_mask, group_col], sort=False).diff()
        out.loc[back_mask, cols.is_viol_back] = (d_back > 0).fillna(False).to_numpy()

    out[cols.viol_any] = out[cols.is_viol_front] | out[cols.is_viol_back]



def pyr_mark_dense_runs_and_totals(
    out: _pd.DataFrame,
    *,
    group_col: str,
    cols: PyrCols,
    caller: str,
) -> None:
    require_cols(out, [group_col, cols.is_front, cols.is_back, cols.is_viol_front, cols.is_viol_back], caller=caller)

    out[cols.viol_front_run_dense] = 0
    out[cols.viol_back_run_dense] = 0
    out[cols.viol_front_total] = 0
    out[cols.viol_back_total] = 0

    front_mask = out[cols.is_front].to_numpy()
    back_mask  = out[cols.is_back].to_numpy()

    if front_mask.any():
        front_df = out.loc[front_mask]
        out.loc[front_mask, cols.viol_front_run_dense] = (
            front_df[cols.is_viol_front]
            .groupby(front_df[group_col], sort=False)
            .cumsum()
            .astype("int64")
            .to_numpy()
        )
        out.loc[front_mask, cols.viol_front_total] = (
            out.loc[front_mask, cols.viol_front_run_dense]
            .groupby(out.loc[front_mask, group_col], sort=False)
            .transform("max")
            .astype("int64")
            .to_numpy()
        )

    if back_mask.any():
        back_df = out.loc[back_mask]
        out.loc[back_mask, cols.viol_back_run_dense] = (
            back_df[cols.is_viol_back]
            .groupby(back_df[group_col], sort=False)
            .cumsum()
            .astype("int64")
            .to_numpy()
        )
        out.loc[back_mask, cols.viol_back_total] = (
            out.loc[back_mask, cols.viol_back_run_dense]
            .groupby(out.loc[back_mask, group_col], sort=False)
            .transform("max")
            .astype("int64")
            .to_numpy()
        )

    # optional compact dtypes
    out[cols.viol_front_total] = out[cols.viol_front_total].astype(_np.int8)
    out[cols.viol_back_total]  = out[cols.viol_back_total].astype(_np.int8)


def mark_side_apex_events(
    df: _pd.DataFrame,
    group_cols: Iterable[str],
    side_col: str,
    color_col: str,
    ord_col: str,
    main_apex_ord_col: str,
    out_front_event_col: str,
    out_back_event_col: str,
    out_front_event_ord_col: str,
    out_back_event_ord_col: str,
    add_event_ord: bool
):
    """
    Mark streaming apex events on both sides + inject main apex.

    Required color labels in color_col:
        'dark_red', 'light_red', 'dark_green', 'light_green'
    
    Required values in side_col:
        'frontside', 'backside'

    Streaming rules:
        frontside apex : dark -> light
        backside apex  : light -> dark

    Main apex:
        ord_col == main_apex_ord_col
        counted as a frontside apex event.

    df is mutated and returned.
    """

    caller = "mark_side_apex_events"

    required = list(group_cols) + [
        side_col,
        color_col,
        ord_col,
        main_apex_ord_col,
    ]

    missing = [c for c in required if c not in df.columns]
    if missing:
        raise KeyError(f"{caller}: missing required columns: {missing}")

    g = df.groupby(group_cols, sort=False)

    prev_color = g[color_col].shift(1)

    is_front = df[side_col] == 'frontside'
    is_back  = df[side_col] == 'backside'

    dark_to_light = (
        ((prev_color == "dark_red") & (df[color_col] == "light_red")) |
        ((prev_color == "dark_green") & (df[color_col] == "light_green"))
    )

    light_to_dark = (
        ((prev_color == "light_red") & (df[color_col] == "dark_red")) |
        ((prev_color == "light_green") & (df[color_col] == "dark_green"))
    )

    front_stream = is_front & dark_to_light
    back_stream  = is_back  & light_to_dark

    main_front = is_front & (df[ord_col] == df[main_apex_ord_col])

    df[out_front_event_col] = front_stream | main_front
    df[out_back_event_col] = back_stream

    if add_event_ord:
        df[out_front_event_ord_col] = g[out_front_event_col].cumsum()
        df[out_back_event_ord_col] = g[out_back_event_col].cumsum()

    return df



# old implementation 
# def macd_full_pyramidal_annotation(
#     df: _pd.DataFrame,
#     *,
#     hist_col: str,
#     group_col: str,
#     out_prefix: str = "pyr_",
# ) -> AnnotatedDF:
#     """
#     Annotating the areas between macd crosses (potential pyramids/mountains/valleys).

#     Assumptions:
#     - df is already segmented into pyramids via MACD zero-crosses
#       (one pyramid == one condition_group_id)
#     - hist_col sign is consistent within each group
#     - df index is ordered (DatetimeIndex or monotonic integer index)

#     This function:
#     - identifies the true apex per pyramid (offline; lookahead allowed)
#       NOTE: if the last group is still "open", apex is provisional = max(|hist|) observed so far.
#     - splits each pyramid into front/back sides
#     - assigns ordinal position per side
#     - marks strict monotonicity violations per side

#     No filtering, no trade logic, no causal assumptions.
#     """
#     out, mem_ev = df_copy_measured(df=df, label="macd_full_pyramidal_annotation")
#     log_memory_info(ev=mem_ev, log=log)
    
#     if hist_col not in out.columns:
#         raise KeyError(f"hist_col={hist_col!r} not found in df")
#     if group_col not in out.columns:
#         raise KeyError(f"group_col={group_col!r} not found in df")

#     # Ensure we can do reliable index comparisons
#     if not out.index.is_monotonic_increasing:
#         out = out.sort_index()

#     abs_hist = out[hist_col].abs()

#     new_cols = ColRegistry(owner="macd_full_pyramidal_annotation")
#     cols = _pyr_cols(out_prefix)
    
#     # ------------------------------------------------------------------
#     # 0. Ordinal position within entire pyramid (needed for apex ordinal)
#     # ------------------------------------------------------------------
#     out[cols.ord] = out.groupby(group_col, sort=False).cumcount()

#     abs_hist = out[hist_col].abs()

#     # ------------------------------------------------------------------
#     # 1. Identify apex per pyramid (ordinal position of max |hist|)
#     # ------------------------------------------------------------------
#     # Per-row max(|hist|) within group
#     gmax = abs_hist.groupby(out[group_col], sort=False).transform("max")

#     # Rows that hit the max (ties possible)
#     is_apex_candidate = abs_hist.eq(gmax)

#     # Apex ordinal = first ordinal in the group that hits the max
#     apex_ord = (
#         out[cols.ord]
#         .where(is_apex_candidate)
#         .groupby(out[group_col], sort=False)
#         .transform("min")
#     )

#     # Broadcast to rows
#     out[cols.apex_idx] = apex_ord  # NOTE: pyr_apex_idx is now ordinal, not datetime
#     out[cols.apex_val] = gmax      # apex |hist| (same for all rows in group)

#     # ------------------------------------------------------------------
#     # 2. Classify rows as front/back relative to apex ordinal
#     #    Apex is considered FRONT.
#     # ------------------------------------------------------------------
#     out[cols.is_front] = out[cols.ord] <= out[cols.apex_idx]  # <= includes apex
#     out[cols.is_back]  = out[cols.ord] >  out[cols.apex_idx]

#     # ------------------------------------------------------------------
#     # 3. Side enum (string)
#     # ------------------------------------------------------------------
#     out[cols.side] = _np.where(out[cols.is_front].to_numpy(), "frontside", "backside")

#     # ------------------------------------------------------------------
#     # 4/5. Ordinal positions + lengths within front/back sides
#     # ------------------------------------------------------------------
#     out[cols.front_ord] = _np.nan
#     out[cols.back_ord] = _np.nan
#     out[cols.front_len] = 0
#     out[cols.back_len] = 0

#     front_mask = out[cols.is_front].to_numpy()
#     back_mask = out[cols.is_back].to_numpy()

#     if front_mask.any():
#         out.loc[front_mask, cols.front_ord] = (
#             out.loc[front_mask].groupby(group_col, sort=False).cumcount().to_numpy()
#         )
#         front_sizes = out.loc[front_mask].groupby(group_col, sort=False)[hist_col].transform("size")
#         out.loc[front_mask, cols.front_len] = front_sizes.to_numpy()

#     if back_mask.any():
#         out.loc[back_mask, cols.back_ord] = (
#             out.loc[back_mask].groupby(group_col, sort=False).cumcount().to_numpy()
#         )
#         back_sizes = out.loc[back_mask].groupby(group_col, sort=False)[hist_col].transform("size")
#         out.loc[back_mask, cols.back_len] = back_sizes.to_numpy()

#     # ------------------------------------------------------------------
#     # 7/8. Monotonic violations on each side (strict expectation)
#     # ------------------------------------------------------------------
#     # Front: |hist| should be non-decreasing toward apex => diff < 0 is violation
#     out[cols.is_viol_front] = False
#     if front_mask.any():
#         d_front = abs_hist.loc[front_mask].groupby(out.loc[front_mask, group_col], sort=False).diff()
#         out.loc[front_mask, cols.is_viol_front] = (d_front < 0).fillna(False).to_numpy()

#     # Back: |hist| should be non-increasing away from apex => diff > 0 is violation
#     out[cols.is_viol_back] = False
#     if back_mask.any():
#         d_back = abs_hist.loc[back_mask].groupby(out.loc[back_mask, group_col], sort=False).diff()
#         out.loc[back_mask, cols.is_viol_back] = (d_back > 0).fillna(False).to_numpy()

#     # Optional derived convenience cols (keep front/back as ground truth)

#     out[f"{out_prefix}viol_any"] = out[cols.is_viol_front] | out[cols.is_viol_back]

#     # ==== Frontside - Running Counts (dense, sparse (to be implemented) and Total =====================

#     out[cols.viol_front_run_dense] = 0

#     if front_mask.any():
#         out.loc[front_mask, cols.viol_front_run_dense] = (
#             out.loc[front_mask, cols.is_viol_front]
#                 .groupby(out.loc[front_mask, group_col], sort=False)
#                 .cumsum()
#                 .astype("int64")
#                 .to_numpy()
#         )

#     out[cols.viol_front_total] = 0

#     if front_mask.any():
#         out.loc[front_mask, cols.viol_front_total] = (
#             out.loc[front_mask, cols.viol_front_run_dense]
#                 .groupby(out.loc[front_mask, group_col], sort=False)
#                 .transform("max")
#                 .astype("int64")
#                 .to_numpy()
#         )

#     out[f"{out_prefix}viol_front_total"] = out[cols.viol_front_total].astype(_np.int8) 
    
#     # ==== Backside - Running Counts (dense, sparse (to be implemented) and Total =====================

#     out[cols.viol_back_run_dense] = 0

#     if back_mask.any():
#         out.loc[back_mask, cols.viol_back_run_dense] = (
#             out.loc[back_mask, cols.is_viol_back]
#                 .groupby(out.loc[back_mask, group_col], sort=False)
#                 .cumsum()
#                 .astype("int64")
#                 .to_numpy()
#         )

#     out[cols.viol_back_total] = 0

#     if back_mask.any():
#         out.loc[back_mask, cols.viol_back_total] = (
#             out.loc[back_mask, cols.viol_back_run_dense]
#                 .groupby(out.loc[back_mask, group_col], sort=False)
#                 .transform("max")
#                 .astype("int64")
#                 .to_numpy()
#         )

#     out[f"{out_prefix}viol_back_total"] = out[cols.viol_back_total].astype(_np.int8) 
    
    
#     out[f"{out_prefix}viol_back_run_sparse"] = "tbimp"
#     out[f"{out_prefix}viol_front_run_sparse"] = "tbimp"
#     out[f"{out_prefix}viol_total"] = "tbimp"


#     announce_column_lifecycle(
#         caller="macd_full_pyramidal_annotation",
#         registry=new_cols,
#         decls=[
#             ColKeyDecl("pyr_apex_idx", cols.apex_idx),
#             ColKeyDecl("pyr_apex_val", cols.apex_val),
#             ColKeyDecl("is_pyr_front", cols.is_front),
#             ColKeyDecl("is_pyr_back", cols.is_back),
#             ColKeyDecl("pyr_side", cols.side),
#             ColKeyDecl("pyr_ord", cols.ord),
#             ColKeyDecl("pyr_front_ord", cols.front_ord),
#             ColKeyDecl("pyr_front_len", cols.front_len),
#             ColKeyDecl("pyr_back_ord", cols.back_ord),
#             ColKeyDecl("pyr_back_len", cols.back_len),
#             ColKeyDecl("pyr_is_viol_front", cols.is_viol_front),
#             ColKeyDecl("pyr_is_viol_back", cols.is_viol_back),
#             # some derived outputs:
#             ColKeyDecl("pyr_viol_any", f"{out_prefix}viol_any"),
#             ColKeyDecl("pyr_viol_front_run_dense", f"{out_prefix}viol_front_run_dense"),
#             ColKeyDecl("pyr_viol_back_run_dense", f"{out_prefix}viol_back_run_dense"),
#             ColKeyDecl("pyr_viol_front_run_sparse", f"{out_prefix}viol_front_run_sparse"),
#             ColKeyDecl("pyr_viol_front_run_sparse", f"{out_prefix}viol_front_run_sparse"),
#             ColKeyDecl("pyr_viol_front_total", f"{out_prefix}viol_front_total"),
#             ColKeyDecl("pyr_viol_back_total", f"{out_prefix}viol_back_total"),
#             ColKeyDecl("pyr_viol_total", f"{out_prefix}viol_total"),
#         ],
#         event="created",
#     )
#     verify_declared_cols_exist(df=out, registry=new_cols, caller="macd_full_pyramidal_annotation")
#     return AnnotatedDF(df=out, new_cols=new_cols, label="macd_full_pyramidal_annotation")


