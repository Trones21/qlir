import pandas as _pd


def sma(
    df: _pd.DataFrame,
    *,
    col: str,
    window: int,
    new_col_name: str | None = None,
    prefix_2_default_col_name: str | None = None,
    min_periods: int | None = None,
    decimals: int | None = None,
    in_place: bool = True,
) -> tuple[_pd.DataFrame, str]: # type: ignore
    """
    Compute a simple moving average (SMA) for a column.

    Notes
    -----
    - Optional rounding (`decimals`) is applied AFTER rolling mean
      to control floating-point noise for downstream transforms.
    """
    out = df if in_place else df.copy()

    name = (
        new_col_name
        if new_col_name
        else f"{prefix_2_default_col_name + '_' if prefix_2_default_col_name else ''}{col}_sma_{window}"
    )

    s = (
        out[col]
        .rolling(window=window, min_periods=min_periods or window)
        .mean()
    )

    if decimals is not None:
        s = s.round(decimals)

    out[name] = s
    return out, name
