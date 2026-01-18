


import numpy as np
import pandas as pd
from qlir.df.granularity.distributions.bucketize.lossy.equal_width import bucketize_zoom_equal_width
from qlir.df.scalars.units import delta_in_bps
from qlir.logging.logdf import logdf
from qlir.servers.analysis_server.analyses.sma_14.execution_analyses import _prep
import logging

from qlir.utils.time.fmt import format_ts_human 
log = logging.getLogger(__name__)

def distance_distributions(df):
    dfs, lists_cols = _prep(df)
    
    df_up = dfs[0]
    log.info(lists_cols[0])
    leg_id = "open_sma_14_up_leg_id"
    

    # Mark the intra leg idx 
    df_up['intra_leg_idx'] = df_up.groupby(leg_id).cumcount()
    df_slim = df_up.loc[:,[leg_id, "tz_start","open", "high", "open_sma_14", "intra_leg_idx"]]
    df_slim["hmn_ts"] = format_ts_human(df_slim["tz_start"])
    # Get the leg len (zero based) - And apply to: [all row in group, new col]
    df_slim["leg_len_idx"] = (
        df_slim.groupby(leg_id)["intra_leg_idx"]
        .transform("last")
    )

    # Calc distance to SMA (also in bps)
    df_slim["distance_2_SMA_line"] = df_slim["open"] - df_slim["open_sma_14"]

    df_slim["max_dist_in_bps"] = delta_in_bps(df_slim["distance_2_SMA_line"], df_slim["open_sma_14"])
    
    logdf(df_slim, max_rows=50)
    # Mark the max distance from SMA row for each leg
    max_dist_row_idx = df_slim.groupby(leg_id)["max_dist_in_bps"].idxmax()
    df_slim["is_max_dist_row"] = False
    df_slim.loc[max_dist_row_idx, "is_max_dist_row"] = True

    # Filter to only the max dist rows 
    #df_max_dist = df_slim.loc[df_slim["is_max_dist_row"] == True , :].copy()
    df_max_dist = df_slim

    df_max_dist.rename(columns={"intra_leg_idx": "max_dist_intra_leg_idx"}, inplace=True)

    df_max_dist["stretch_pct_of_leg"] = np.where(
        df_max_dist["leg_len_idx"] == 0,
        1.0,
        df_max_dist["max_dist_intra_leg_idx"] / df_max_dist["leg_len_idx"],
    )
    df_test = df_max_dist.loc[ [i for i in range(7924,7934)], :]
    logdf(df_test)

    is_max_row = df_max_dist["is_max_dist_row"].eq(True)
    is_max_at_end = df_max_dist["max_dist_intra_leg_idx"].eq(df_max_dist["leg_len_idx"])

    hits = df_max_dist.loc[is_max_row & is_max_at_end].copy()
    logdf(hits)
    hits5 = df_max_dist.loc[is_max_row & is_max_at_end & df_max_dist["leg_len_idx"].eq(5)].copy()
    logdf(hits5)




    # test = bucketize_zoom_equal_width(df_question["leg_len_idx"], buckets=100)
    # logdf(test, max_rows=100)
    max_distance_distra = bucketize_zoom_equal_width(df_max_dist["stretch_pct_of_leg"], buckets=1000)
                     
    #logdf(max_distance_distra, max_rows=2000)
# This is quite bimodal, so we are going to have a look at 
# 📊 zoom:depth0 (original_shape=(50, 8)) 
# |    |   bucket_id |   lower |   upper |   count | parent_bucket_id   | pct_fmt   |   depth | cum_pct_fmt   |
# |----|-------------|---------|---------|---------|--------------------|-----------|---------|---------------|
# |  0 |           0 |    0    |    0.02 |   27523 | None               | 18.85%    |       0 | 18.85%        |
# | 49 |          49 |    0.98 |    1    |   55805 | None               | 38.22%    |       0 | 100.00%       |


