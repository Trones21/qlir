from dataclasses import asdict, is_dataclass
from typing import Iterable

import pandas as _pd

from .common import finalize_df


def objects_to_df(
    objs: list,
    *,
    columns: Iterable[str] | None = None,
    sort_by: str | None = None,
    ascending: bool = True,
) -> _pd.DataFrame:
    if not objs:
        return _pd.DataFrame(columns=list(columns or []))

    records = []
    for obj in objs:
        if is_dataclass(obj):
            records.append(asdict(obj))
        else:
            records.append(vars(obj))

    df = _pd.DataFrame.from_records(records)

    return finalize_df(
        df,
        columns=columns,
        sort_by=sort_by,
        ascending=ascending,
    )
