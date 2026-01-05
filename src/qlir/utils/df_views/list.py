import pandas as _pd
from typing import Iterable
from .common import finalize_df


def list_to_df(
    rows: list[dict],
    *,
    columns: Iterable[str] | None = None,
    sort_by: str | None = None,
    ascending: bool = True,
) -> _pd.DataFrame:
    if not rows:
        return _pd.DataFrame(columns=list(columns or []))

    df = _pd.DataFrame.from_records(rows)
    return finalize_df(
        df,
        columns=columns,
        sort_by=sort_by,
        ascending=ascending,
    )
