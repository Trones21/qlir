import pandas as _pd

# `condition_set_persistence_df` tried to do this in one shot:

# ```
# boolean column
# → infer events
# → infer durations
# → infer distribution
# → infer survival
# ```

# That’s *too much inference* for QLIR’s current philosophy. Even if the function works and has unit tests. 


# ## 3. The *real* philosophical split you implemented

# Your new code implicitly draws a **very important boundary**:

# ### Phase 1: Event materialization (row space → event space)

# * assign condition group IDs
# * compute contiguous-true counters
# * compute per-event max run length
# * **persist these columns**

# This phase is:

# * deterministic
# * inspectable
# * testable
# * composable

# ### Phase 2: Distributional views (event space → summaries)

# * bucketize
# * survival
# * percentiles
# * visualization

# This phase is:

# * intentionally lossy *or* lossless
# * optional
# * reversible (you can always go back to event space)

# That separation is why this feels “safer”.

# ## 6. The deeper pattern you’ve converged on (important)

# What you’ve *actually* implemented is this principle:

# > **QLIR functions should either**
# >
# > 1. *construct state* (with full traceability), or
# > 2. *summarize state* (without inference)


def deprecated_condition_set_persistence_df(
    *,
    df: _pd.DataFrame,
    condition_col: str,
    condition_set_name: str,
) -> _pd.DataFrame:
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
    _pd.DataFrame
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
        return _pd.DataFrame(
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
        return _pd.DataFrame(
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
