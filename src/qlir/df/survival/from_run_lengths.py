import pandas as _pd

from qlir.df.after_grouping.verify_complete_run_length_domain import (
    verify_complete_run_length_domain,
)
from qlir.df.survival.survival_stat import SurvivalStat


def survival_stats_from_run_lengths(
    df_runs: _pd.DataFrame,
    *,
    tail_thresholds: list[float],
    run_len_col: str = "run_len",
    count_col: str = "count",
    survival_label_fmt: str = "{thr}%",
) -> list[SurvivalStat]:

    df = compute_tail_survival(
        df_runs,
        run_len_col=run_len_col,
        count_col=count_col,
    )

    stats: list[SurvivalStat] = []

    for thr in sorted(tail_thresholds, reverse=True):
        hit = df[df["survival_pct"] <= thr].iloc[0]

        stats.append(
            SurvivalStat(
                bar_count=int(hit[run_len_col]),
                survival_rate=survival_label_fmt.format(thr=thr),
            )
        )

    return stats


def compute_tail_survival(
    df: _pd.DataFrame,
    *,
    run_len_col: str = "run_len",
    count_col: str = "count",
) -> _pd.DataFrame:
    """
    Compute right-tail survival probabilities P(X >= N) from a complete,
    non-bucketized run-length distribution.

    Assumes exactly one row per integer run length within the observed
    domain. This invariant is verified explicitly and not repaired.

    Returns a copy of the DataFrame with an added `survival_pct` column.
    """

    verify_complete_run_length_domain(df, run_len_col=run_len_col)

    df = df.sort_values(run_len_col).copy()

    total = df[count_col].sum()

    df["survival_pct"] = (
        (total - df[count_col].cumsum() + df[count_col]) / total * 100
    )

    return df
