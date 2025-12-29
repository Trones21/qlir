import pandas as pd
from dataclasses import is_dataclass, asdict
from typing import Iterable
from .common import finalize_df


def objects_to_df(
    objs: list,
    *,
    columns: Iterable[str] | None = None,
    sort_by: str | None = None,
    ascending: bool = True,
) -> pd.DataFrame:
    if not objs:
        return pd.DataFrame(columns=list(columns or []))

    records = []
    for obj in objs:
        if is_dataclass(obj):
            records.append(asdict(obj))
        else:
            records.append(vars(obj))

    df = pd.DataFrame.from_records(records)

    return finalize_df(
        df,
        columns=columns,
        sort_by=sort_by,
        ascending=ascending,
    )
