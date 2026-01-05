import pandas as _pd
from typing import Mapping, Iterable
from .common import finalize_df


def dict_to_df(
    data: Mapping[str, Mapping],
    *,
    include_key: str | None = None,
    columns: Iterable[str] | None = None,
    sort_by: str | None = None,
    ascending: bool = True,
) -> _pd.DataFrame:
    if not data:
        base_cols = list(columns or [])
        if include_key:
            base_cols = [include_key] + base_cols
        return _pd.DataFrame(columns=base_cols)

    records = []
    for key, record in data.items():
        row = dict(record)
        if include_key:
            row[include_key] = key
        records.append(row)

    df = _pd.DataFrame.from_records(records)

    return finalize_df(
        df,
        columns=columns if not include_key else [include_key, *(columns or [])],
        sort_by=sort_by,
        ascending=ascending,
    )
