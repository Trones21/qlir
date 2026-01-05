import pandas as _pd
from typing import Iterable


def finalize_df(
    df: _pd.DataFrame,
    *,
    columns: Iterable[str] | None = None,
    sort_by: str | None = None,
    ascending: bool = True,
) -> _pd.DataFrame:
    """
    Finalize a DataFrame view:
    - Optionally project + reorder columns
    - Optionally sort rows
    - Reset index
    """
    if columns is not None:
        # preserve specified order, drop missing
        cols = [c for c in columns if c in df.columns]
        df = df.loc[:, cols]

    if sort_by and sort_by in df.columns:
        df = df.sort_values(sort_by, ascending=ascending)

    return df.reset_index(drop=True)
