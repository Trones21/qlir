import pandas as _pd

from qlir.df.survival.survival_stat import SurvivalStat


def add_columns_for_trend_survival_rates(df: _pd.DataFrame, prefix: str, trendrun_count_column: str, survival_stats: list[SurvivalStat]) -> tuple[_pd.DataFrame, dict]:
    """
    Add survival-rate–based persistence columns for an existing trend run counter
    and return corresponding emit metadata for downstream alerting/logging.

    For each entry in `persistence_marks`, this function derives a new boolean or
    marker column indicating whether the current trend run has reached a specified
    bar-count threshold associated with a given survival-rate percentile. Each
    derived column is accompanied by an emit object describing the condition in
    human-readable form.

    This function is intentionally side-effect–free with respect to I/O: it only
    mutates the provided DataFrame and returns structured metadata, leaving any
    logging, alert emission, or persistence to the caller.

    Parameters
    ----------
    df : pandas.DataFrame
        Input DataFrame containing an existing trend run-length column.
    prefix : str
        Prefix used to namespace generated survival columns.
        Suggested to include:
            - name of column the running count was performed on (e.g. sma-14)
            - direction of the trend
    trendrun_count_column : str
        Column name representing the current trend run length (e.g. consecutive
        bars in a given direction).
    persistence_marks : Iterable[dict]
        Iterable of survival specifications. Each item must contain:
        - "bar_count": int
            The minimum run length to evaluate.
        - "survival_rate": String
            Survival percentile or label associated with that run length.

    Returns
    -------
    tuple[pandas.DataFrame, dict]
        A tuple of:
        - Updated DataFrame with one survival column added per persistence mark.
        - A dict mapping each generated survival column name to an emit object
            with keys:
            - "type": str
            - "description": str

    Notes
    -----
    The returned emit objects are designed for direct inclusion in a trigger or
    signal registry and may be logged, serialized, or written to code by the caller.
    """

    emit_objs: dict[str, dict] = {}

    for stat in survival_stats:
        df, survival_col = condition_persistence_survival_column(
            df=df,
            prefix=prefix,
            persistence_count_column=trendrun_count_column,
            count_to_mark=stat.bar_count,
            survival_at_mark=str(stat.survival_rate),
        )

        emit_objs[survival_col] = {
            "type": "survival rate",
            "description": stat.description(),
        }

    return df, emit_objs



def condition_persistence_survival_column(df, prefix: str, persistence_count_column: str, count_to_mark: int, survival_at_mark: str) -> tuple[_pd.DataFrame, str]:
    new_col = f"{prefix}_{survival_at_mark}_survive"
    df[new_col] = df[persistence_count_column] == count_to_mark
    return df, new_col 
