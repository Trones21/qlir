from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Optional, Sequence, Tuple

RowOffset = int  # relative to i (0=current row)


Scope = Literal["output", "intermediate"]


@dataclass(frozen=True)
class ColumnDerivationSpec:
    """
    Describes how a single derived column at row i is computed in terms of rows read.

    All row offsets are relative to row i and inclusive.

    Examples:
      - rolling window=3 self-inclusive: read_rows=(-2, 0) => [i-2 .. i]
      - lag(1): read_rows=(-1, -1) => [i-1]
      - rolling window=14 row-exclusive (shift=1): read_rows=(-14, -1) => [i-14 .. i-1]
    """

    op: str
    base_cols: Tuple[str, ...]
    read_rows: Tuple[RowOffset, RowOffset]  # inclusive bounds (lo, hi)
    scope: Scope = "output"

    # Explicit flags that remove ambiguity for readers and enable guards later.
    self_inclusive: bool = True

    # Optional grouping boundary information (e.g., "asset" for grouped rolling).
    grouping: Optional[str] = None

    # Hardcoded string that gives the user more info about the function itself. 
    # e.g. for arp we log self_inclusive=True ... but arp doesnt implement shifted windows, so i pass a string to log_suffix+"arp() is ALWAYS self-inclusive" 
    log_suffix: Optional[str] = None

    def format_rows_used(self) -> str:
        lo, hi = self.read_rows

        def fmt(x: int) -> str:
            return "i" if x == 0 else f"i{x:+}"

        if lo == hi:
            return f"[{fmt(lo)}]"
        return f"[{fmt(lo)} .. {fmt(hi)}]"

    def to_human(self, *, write_col: str, write_row: str = "i") -> str:
        base = ",".join(self.base_cols)
        parts = [
            f"{self.op.upper()}",
            f"base_col={base}",
            f"scope={self.scope}",
            f"rows_used={self.format_rows_used()}",
            f"write_col={write_col}",
            f"write_row={write_row}",
            f"self_inclusive={self.self_inclusive}",
            f"{self.log_suffix}",
        ]
        if self.grouping:
            parts.append(f"grouping={self.grouping}")
        return " | ".join(parts)


@dataclass(frozen=True)
class ColumnLifecycleEvent:
    """
    Tracks lifecycle state for derived columns (created, dropped, etc.)
    without erasing derivation truth.
    """

    col: str
    event: Literal["created", "dropped"]
    reason: Optional[str] = None


def ensure_tuple_str(x: Sequence[str] | str) -> Tuple[str, ...]:
    if isinstance(x, str):
        return (x,)
    return tuple(x)