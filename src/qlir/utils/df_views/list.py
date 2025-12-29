import pandas as pd
from typing import Iterable
from .common import finalize_df


def list_to_df(
    rows: list[dict],
    *,
    columns: Iterable[str] | None = None,
    sort_by: str | None = None,
    ascending: bool = True,
) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=list(columns or []))

    df = pd.DataFrame.from_records(rows)
    return finalize_df(
        df,
        columns=columns,
        sort_by=sort_by,
        ascending=ascending,
    )
