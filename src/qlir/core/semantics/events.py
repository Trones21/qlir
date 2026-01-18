from __future__ import annotations
import logging

from qlir.core.semantics.row_derivation import ColumnLifecycleEvent
from qlir.utils.str.fmt import PipeAligner

log = logging.getLogger("qlir.columns")

_pipe_align = PipeAligner(max_cols=4, max_col_width=60)

def log_column_event(
    *,
    caller: str,
    ev: ColumnLifecycleEvent,
) -> None:
    """
    Log a column lifecycle event in a human-readable form.
    """
    msg = f"[COLUMN] {ev.col} | {ev.event.upper()} by | {caller}"
    if ev.reason:
        msg += f" | {ev.reason}"


    log.info(_pipe_align(msg))
