from dataclasses import dataclass
from pandas import DataFrame

@dataclass(frozen=False)
class NamedDF:
    df: DataFrame
    name: str

