from dataclasses import dataclass
from typing import Literal, Optional, Sequence, Tuple


@dataclass(frozen=True)
class ColumnLifecycleEvent:
    """
    Tracks lifecycle state for derived columns (created, dropped, etc.)
    without erasing derivation truth.
    """
    key: str
    col: str
    event: Literal["created", "dropped"]
    reason: Optional[str] = None


def ensure_tuple_str(x: Sequence[str] | str) -> Tuple[str, ...]:
    if isinstance(x, str):
        return (x,)
    return tuple(x)