from typing import Tuple, TypeAlias
import pandas as pd

OutCols: TypeAlias = (
    str
    | tuple[str, ...]
    | list[str]
    | dict[str, str]
    | dict[str, list[str]]
)

NewColsResult: TypeAlias = tuple[pd.DataFrame, OutCols]
# old NewColsResult = tuple[pd.DataFrame, tuple[str, ...]]
