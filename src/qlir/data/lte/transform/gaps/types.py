from dataclasses import dataclass

@dataclass(frozen=True)
class MissingBlock:
    start_idx: int
    end_idx: int
