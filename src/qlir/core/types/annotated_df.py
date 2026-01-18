from typing import TypeAlias
import pandas as pd

from qlir.core.registries.columns.registry import ColRegistry
from .named_df import NamedDF

DFish: TypeAlias = pd.DataFrame | NamedDF
AnnotatedDataFrame: TypeAlias = tuple[DFish, ColRegistry]
