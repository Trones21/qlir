

from dataclasses import dataclass
from typing import NewType, Optional

from qlir.core.types.direction import Direction


LegId = NewType("LegId", int)

@dataclass(frozen=True)
class LegSpec:
    id_col: str
    ts_col: str
    direction: Optional[Direction]
