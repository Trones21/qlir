from dataclasses import dataclass
from pandas import DataFrame

@dataclass(frozen=True)
class NamedDF:
    df: DataFrame
    name: str

