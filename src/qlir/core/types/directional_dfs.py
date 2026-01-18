from dataclasses import dataclass
from typing import Generic, TypeVar

T = TypeVar("T")

@dataclass(frozen=True)
class DirectionalDFs(Generic[T]):
    up: T
    down: T
