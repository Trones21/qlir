



import numpy as np
import pandas as pd
from qlir.core.types.direction import Direction
from qlir.core.types.excursion_type import ExcursionType
from qlir.core.types.named_df import NamedDF
from qlir.df.scalars.units import delta_in_bps
from qlir.logging.logdf import logdf
from qlir.servers.analysis_server.analyses.excursion import excursion
from qlir.servers.analysis_server.analyses.sma_14.execution_analyses import _prep
from qlir.df.granularity.distributions.bucketize.lossy.equal_width import bucketize_zoom_equal_width
import logging
log = logging.getLogger(__name__)



def mfe(df: pd.DataFrame):
    df_mfe_up = excursion(df=df, trendname_or_col_prefix="osma14", direction=Direction.UP, mae_or_mfe=ExcursionType.MFE)
    df_mfe_down = excursion(df=df, trendname_or_col_prefix="osma14", direction=Direction.DOWN, mae_or_mfe=ExcursionType.MFE)



def mfe_original(df: pd.DataFrame):
    
    mfe_df = mfe_rows_up(df)

    assert (mfe_df["leg_of_n_bars"] >= 1).all()
    assert (mfe_df["mfe_from_start"] >= 0).all()
    assert (mfe_df["mfe_from_start"] <= mfe_df["leg_len"]).all() #leg_len uses zero based idx, not 1 based counter 

    survival_curve = mfe_survival_curve(df=mfe_df, leg_len_col="leg_len", mfe_idx_col="mfe_from_start")
    logdf(survival_curve, max_rows=100)

def mfe_rows_up(df):
    dfs, lists_cols = _prep(df)

    df_up = dfs[0]
    up_cols = lists_cols[0]
    leg_id = "open_sma_14_up_leg_id"

    # Mark the intra leg idx 
    df_up['intra_leg_idx'] = df_up.groupby(leg_id).cumcount()
    
    df_slim = df_up.loc[:,[leg_id, "open", "high","intra_leg_idx"]]
    
    # Get the leg len - And apply to: [all row in gorup, new col]
    df_slim["leg_len"] = (
        df_slim.groupby(leg_id)["intra_leg_idx"]
        .transform("last")
    )

    # Get the first open - And apply to: [all rows in group, new col]
    df_slim["group_first_price"] = (
        df_slim.groupby(leg_id)["open"]
        .transform("first")
    )

    # Calc MFE (also in bps)
    df_slim["excursion"] = df_slim["high"] - df_slim["group_first_price"]
    df_slim["exc_bps"] = delta_in_bps(df_slim["excursion"], df_slim["group_first_price"])
    
    # Mark the mfe row for each leg
    mfe_row_idx = df_slim.groupby(leg_id)["exc_bps"].idxmax()
    df_slim["is_mfe_row"] = False
    df_slim.loc[mfe_row_idx, "is_mfe_row"] = True

    # Filter to only the mfe rows 
    df_mfe = df_slim.loc[df_slim["is_mfe_row"] == True , :].copy()
    
    # MFE occurs N candles from start
    df_mfe["mfe_from_start"] = df_mfe["intra_leg_idx"] # no math needed
    
    # MFE occurs N candles from end
    df_mfe["mfe_from_end"] = df_mfe["leg_len"] - df_mfe["intra_leg_idx"]

    #“What fraction of the entire realized leg had elapsed when MFE was first achieved?”
    df_mfe.loc[:,"mfe_pct_from_start"] = np.where(
        df_mfe["leg_len"] == 0,
        1.0,
        df_mfe["mfe_from_start"] / df_mfe["leg_len"]
    )

    df_mfe.loc[:,"mfe_pct_from_end"] = np.where(
        df_mfe["leg_len"] == 0,
        1.0,
        df_mfe["mfe_from_end"] / df_mfe["leg_len"]
    )

    df_mfe["pct_from_sum"] = df_mfe["mfe_pct_from_end"] + df_mfe["mfe_pct_from_start"]
    # If this ever fails → indexing bug upstream.
    # assert np.allclose(
    # df_mfe.loc[df_mfe["leg_len"] > 0, "mfe_pct_from_start"] +
    # df_mfe.loc[df_mfe["leg_len"] > 0, "mfe_pct_from_end"],
    # 1.0
    # )
    # logdf(df_mfe, max_rows=400)
    df_mfe["leg_of_n_bars"] = df_mfe["leg_len"] + 1
    return df_mfe



def mfe_survival_curve(
    df: pd.DataFrame,
    *,
    leg_len_col: str = "leg_len",          # last index (0-based)
    mfe_idx_col: str = "mfe_from_start",   # index (0-based)
    t_max: int | None = None,
) -> pd.DataFrame:
    """
    Compute survival-conditioned MFE-already rates.

    Zero-based index conventions:
    - leg_len = last valid intra-leg index (>= 0)
      (i.e., leg_n_bars = leg_len + 1)
    - mfe_from_start in [0, leg_len]
    - t = intra-leg index (0-based)

    Computes:
        P(i_mfe <= t | leg_len >= t)

    Returns DataFrame with columns:
    - t              : intra-leg index (0-based)
    - survival_rate  : fraction of surviving legs where MFE has already occurred
    - survivors      : number of legs surviving to index t
    """

    if t_max is None:
        t_max = int(df[leg_len_col].max())

    rows = []

    for t in range(t_max + 1):  # include last possible index
        survivors = df[df[leg_len_col] >= t]
        n_survivors = len(survivors)

        if n_survivors == 0:
            break

        mfe_already = (survivors[mfe_idx_col] <= t).sum()
        rate = mfe_already / n_survivors

        rows.append(
            {
                "t": t,
                "mfe_occured_rate_b4_t": rate,
                "mfe_already": mfe_already, 
                "survivors": n_survivors,
            }
        )

    return pd.DataFrame(rows)



def mfe_already_at_t(df: pd.DataFrame, t: int):
    # Surviving legs
    survivors = df[df["leg_len"] >= t + 1]

    # Count legs where MFE has already occurred by bar t
    cnt_mfe_already = (survivors["mfe_from_start"] <= t).sum()

    rate = cnt_mfe_already / len(survivors)

    log.info(
        f"t={t}: {rate:.3f} = {cnt_mfe_already} / {len(survivors)}"
    )

    return rate


def mfe_in_bps():
    NotImplementedError()


def mfe_at_pct_of_leg_dists(df: pd.DataFrame):

    # Get Global Dists of <MFE_of_leg_pct>
    # Note that these wont be symmetrical b/c 0 leg len gets counted in both dists
    #   Note: leg_len 0 is actually leg_len 1, b/c zero indexing. only bar in leg is the zeroth index, therefore len() == 0
    # but if we remove the singles then dists are mirrors 
    # no_singles = df.loc[df["leg_len"] != 0, :]
    # without_single_bar_legs_fe = bucketize_zoom_equal_width(no_singles["mfe_pct_from_end"], max_depth=1)
    # without_single_bar_legs_fs = bucketize_zoom_equal_width(no_singles["mfe_pct_from_start"], max_depth=1)
    # logdf(without_single_bar_legs_fe[0], max_rows=100)
    # logdf(without_single_bar_legs_fs[0], max_rows=100)
    log.info("Uncomment top of mfe_at_pct_of_leg_dists for explanation why global dists are not mirror images")
    
    # “How late did MFE occur relative to initiation?”
    dist_from_start = bucketize_zoom_equal_width(df["mfe_pct_from_start"],
                                                 max_depth=1, 
                                                 buckets=10,
                                                 human_friendly_fmt=True)
    dist_from_start[0].name = "MFE distance from start of leg"
    logdf(dist_from_start[0], max_rows=100)

    # “How close to termination was MFE?”
    dist_from_end = bucketize_zoom_equal_width(df["mfe_pct_from_end"], 
                                               max_depth=1, 
                                               buckets=10,
                                               human_friendly_fmt=True)
    dist_from_end[0].name = "MFE distance from end of leg"
    logdf(dist_from_end[0], max_rows=100)
    
    log.info("Reminder of the counts of legs")
    df_counts = (df
                 .groupby("leg_len")
                 .size()
                 .rename("leg_len_count")
                 .reset_index()
                )
    
    df_counts["total_legs"] = df_counts["leg_len_count"].sum()
    df_counts["pct_of_total_raw"] = (df_counts["leg_len_count"] / df_counts["total_legs"])
    df_counts["pct_of_total"] = (df_counts["leg_len_count"] / df_counts["total_legs"]).map("{:.2%}".format)
    df_counts["cum_pct"] = (df_counts["pct_of_total_raw"].cumsum()).map("{:.2%}".format)
    del df_counts["pct_of_total_raw"]

    logdf(df_counts, max_rows=150)
    # Get Dist of <MFE_of_leg_pct> per leg_len
    # or in math speak: Dist(<MFE_leg_pct | leg_len>)
