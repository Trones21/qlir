import pandas as pd


def sma(
    df: pd.DataFrame,
    *,
    col: str,
    window: int,
    prefix: str | None = None,
    min_periods: int | None = None,
    in_place: bool = True,
) -> pd.DataFrame: # type: ignore
    """
    Compute a simple moving average (SMA) for a column.
    """
    out = df if in_place else df.copy()

    name = f"{prefix + '_' if prefix else ''}{col}_sma_{window}"

    out[name] = (
        out[col]
        .rolling(window=window, min_periods=min_periods or window)
        .mean()
    )

    return out
