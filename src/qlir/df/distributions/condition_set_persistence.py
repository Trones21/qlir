import pandas as pd


def condition_set_persistence_df(
    *,
    df: pd.DataFrame,
    condition_col: str,
    condition_set_name: str,
) -> pd.DataFrame:
    """
    Summarize persistence of a condition set as a run-length distribution.

    Each contiguous run of True values is treated as one event.
    The resulting distribution answers:
    'Once a condition is active, how long does it typically persist?'

    Parameters
    ----------
    df
        DataFrame indexed by time.
    condition_col
        Name of a boolean column indicating when a condition is active.
    condition_set_name
        Human-readable identifier for the condition or condition set.

    Returns
    -------
    pd.DataFrame
        Columns:
        - condition_set
        - duration_bars
        - count
        - pct_of_groups
        - pct_of_groups_running  (descending / survival)
    """
    if condition_col not in df.columns:
        raise KeyError(f"Condition column not found: {condition_col}")

    s = df[condition_col].astype(bool)

    if s.empty:
        return pd.DataFrame(
            columns=[
                "condition_set",
                "duration_bars",
                "count",
                "pct_of_groups",
                "pct_of_groups_running",
            ]
        )

    # Identify contiguous runs
    group_id = (s != s.shift()).cumsum()

    # Keep only True runs and measure their lengths
    run_lengths = (
        s[s]
        .groupby(group_id[s])
        .size()
    )

    if run_lengths.empty:
        return pd.DataFrame(
            {
                "condition_set": [condition_set_name],
                "duration_bars": [],
                "count": [],
                "pct_of_groups": [],
                "pct_of_groups_running": [],
            }
        )

    # Frequency distribution
    dist = (
        run_lengths
        .value_counts()
        .sort_index()
        .rename("count")
        .to_frame()
    )

    total = dist["count"].sum()

    dist["pct_of_groups"] = dist["count"] / total

    # Descending cumulative (survival function)
    dist["pct_of_groups_running"] = (
        dist["count"][::-1].cumsum() / total
    )

    dist["condition_set"] = condition_set_name

    dist = (
        dist
        .reset_index()
        .rename(columns={"index": "duration_bars"})
        .loc[
            :,
            [
                "condition_set",
                "duration_bars",
                "count",
                "pct_of_groups",
                "pct_of_groups_running",
            ],
        ]
    )

    return dist
