from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional
import shutil


def term_fmt(text: str, indent: int = 0) -> str:
    import shutil
    import textwrap

    width = shutil.get_terminal_size(fallback=(120, 20)).columns
    return textwrap.fill(
        text,
        width=width,
        subsequent_indent=" " * indent,
        replace_whitespace=False,
        drop_whitespace=False,
    )


@dataclass
class PipeAligner:
    """
    Align pipe-separated log segments using running max widths per column.

    This is meant for *live terminal readability* (tmux / tail -f).
    It learns widths as it sees lines, so alignment improves immediately
    and stays stable.

    - Pads each column to the max width *seen so far* for that column index.
    - Caps widths so one pathological value doesn't explode spacing.
    - Truncates final output to terminal width.
    """
    max_cols: int = 8
    min_col_gap: int = 1
    max_col_width: int = 48
    term_width_fallback: int = 120
    widths: List[int] = field(default_factory=list)

    def __call__(self, text: str) -> str:
        term_width = shutil.get_terminal_size(
            fallback=(self.term_width_fallback, 20)
        ).columns

        parts = [p.strip() for p in text.split("|")]
        if not parts:
            return text

        # Ensure widths array is long enough
        need = min(len(parts), self.max_cols)
        if len(self.widths) < need:
            self.widths.extend([0] * (need - len(self.widths)))

        # Update running widths (cap each column)
        for i in range(need):
            self.widths[i] = min(
                self.max_col_width,
                max(self.widths[i], len(parts[i])),
            )

        # Pad per column (except last column actually present)
        out_parts: List[str] = []
        last_idx = len(parts) - 1
        for i, p in enumerate(parts):
            if i < need and i != last_idx:
                out_parts.append(p.ljust(self.widths[i] + self.min_col_gap))
            else:
                out_parts.append(p)

        out = " | ".join(out_parts)

        if len(out) > term_width:
            return out[: term_width - 1] + "…"
        return out
