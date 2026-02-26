import pandas as pd
import numpy as np
from qlir.core.counters import univariate
from qlir.core.types.annotated_df import AnnotatedDF
from qlir.df.condition_set.assign_group_ids import assign_condition_group_id
from qlir.features.macd.crosses import with_macd_cross_flags
from qlir.features.macd.histogram import with_colored_histogram
from qlir.features.macd.histogram_pyramid import detect_histogram_pyramids, macd_full_pyramidal_annotation
from qlir.indicators.macd import with_macd
from qlir.logging.logdf import logdf
from qlir.df.scalars.units import delta_in_bps

import logging
log = logging.getLogger(__name__)

def df_macd_full_pyramidal_annotation(clean_data: pd.DataFrame) -> AnnotatedDF:
    df = with_macd(df=clean_data)
    df["normalized_macd_Δ"] = delta_in_bps(df["macd"], df["close"])
    adf = with_colored_histogram(df=df, hist_col="macd_hist")
    colored_hist_cols = adf.new_cols

    sign = adf.df["macd_rg"].map({
        "green": 1,
        "red": -1,
    })

    segment_id = (
        sign.ne(sign.shift())
    ).cumsum()

    adf.df["group_id"] = segment_id
        
    # log.info(adf.df.columns)
    # logdf(adf.df, max_rows=50, from_row_idx=200, cols_filter_all_dfs=["tz_start","macd_hist","condition_group_id"] )
    
    adf = with_macd_cross_flags(df=adf.df)
    adf.df["is_macd_cross"] = (
        adf.df["macd_cross_up"] | adf.df["macd_cross_down"]
    )

    adf = macd_full_pyramidal_annotation(df=adf.df, hist_col="macd_hist" , group_col="group_id")
    adf.new_cols.extend(colored_hist_cols) # we will want these later
    return adf


def macd_pyramid_perfect_frontside_plus_one_backside_light(adf: AnnotatedDF):
    log.info(f"df columns {adf.df.columns}")
    log.info([k for k in adf.new_cols.keys()])
    logdf(adf.df,  max_rows=100, cols_filter_all_dfs=["group_id", "macd_hist", "macd_hist_color", *adf.new_cols.keys()])
    adf.df["pyr_viol_front"] == 0 
    raise NotImplementedError("We need to add the column perfect_frontside_plus_1_light")
    return df


# These were helprs for something that is no longer needed
# def _mask_short_segments(
#     df: pd.DataFrame,
#     *,
#     group_col: str,
#     min_len: int,
# ) -> None:
#     # group_col has NaN on non-segment rows
#     sizes = df.groupby(group_col, dropna=True, sort=False)[group_col].transform("size")
#     df.loc[sizes < min_len, group_col] = np.nan


# def _union_prefixed_ids_to_monotonic_int(
#     df: pd.DataFrame,
#     *,
#     up_group_col: str,
#     dn_group_col: str,
#     out_group_col: str,
# ) -> None:
#     union = pd.Series(index=df.index, dtype="object")

#     up_mask = df[up_group_col].notna()
#     dn_mask = df[dn_group_col].notna()

#     union.loc[up_mask] = "U" + df.loc[up_mask, up_group_col].astype("int64").astype(str)
#     union.loc[dn_mask] = "D" + df.loc[dn_mask, dn_group_col].astype("int64").astype(str)

#     # convert to monotonic ids (order of first appearance), keep NaN where no segment
#     codes, _uniques = pd.factorize(union, sort=False)
#     df[out_group_col] = np.where(codes >= 0, codes.astype("int64"), np.nan)


# No longer needed because we can simply use hist sign for the group ids
# def build_macd_condition_group_id(
#     df: pd.DataFrame,
#     *,
#     up_segment_col: str,
#     dn_segment_col: str,
#     out_group_col: str = "condition_group_id",
#     min_len: int = 2,
# ) -> tuple[pd.DataFrame, str]:
#     """
#     Build a single monotonic condition_group_id from up/down segment booleans.

#     - assigns contiguous True-run ids separately for up and down
#     - excludes micro-segments (len < min_len) which includes "consecutive cross" cases
#     - unions to a single group id space via prefixing + factorize
#     """
#     out = df  # mutate in place like your nodes

#     # Create up and dn sparse group id cols
#     out, up_gid_col = assign_condition_group_id(df=out, condition_col=up_segment_col, group_col=f"{out_group_col}__up")
#     out, dn_gid_col = assign_condition_group_id(df=out, condition_col=dn_segment_col, group_col=f"{out_group_col}__dn")

#     # Exclude micro segments on each side
#     _mask_short_segments(out, group_col=up_gid_col, min_len=min_len)
#     _mask_short_segments(out, group_col=dn_gid_col, min_len=min_len)

#     # Union to a single monotonic id
#     _union_prefixed_ids_to_monotonic_int(
#         out,
#         up_group_col=up_gid_col,
#         dn_group_col=dn_gid_col,
#         out_group_col=out_group_col,
#     )

#     return out, out_group_col


# This was before we had the full_pyramidal_annotation function
# def macd_old_pyramid_logic(clean_data: pd.DataFrame) -> pd.DataFrame:

#     df = with_macd(df=clean_data)
#     df["normalized_macd_Δ"] = delta_in_bps(df["macd"], df["close"])
#     adf = with_colored_histogram(df=df, fast_col="ema_fast", slow_col="ema_slow")
#     dist_color = adf.new_cols.get_column("dist_color")
#     adf = detect_histogram_pyramids(df=adf.df, hist_color_col=dist_color)
#     pyramid = adf.new_cols.get_column("pyramid")

#     # ID per pyramid, intra_idx, pyramid_len
#     df, grp_col = assign_condition_group_id(df=adf.df, condition_col=pyramid)
#     df, contig_true_rows = univariate.with_running_true(df, col=pyramid)
#     df["pyramid_len"] = df.groupby(grp_col)[contig_true_rows].transform("max")
    
    
#     log.info(df.columns)
#     logdf(df, cols_filter_all_dfs=["close", "ema_fast", "ema_slow","macd", "normalized_macd_Δ", 
#                                        dist_color, 
#                                        pyramid, 
#                                        grp_col, 
#                                        contig_true_rows,
#                                        "pyramid_len"

#                                        ])
    
#     return df
