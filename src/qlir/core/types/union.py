from typing import Iterable, Optional, Sequence, Union
from pandas import DataFrame

from qlir.core.types.named_df import NamedDF

Number = Union[int, float]
ColsLike =  Optional[Union[str, Sequence[str]]]


LogDFInput = Union[
    DataFrame,
    Iterable[DataFrame],
    Iterable[NamedDF],
]
