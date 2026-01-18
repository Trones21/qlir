from __future__ import annotations
import logging

from qlir.core.semantics.row_derivation import ColumnLifecycleEvent

log = logging.getLogger("qlir.columns")

def log_column_event(
    *,
    caller: str,
    ev: ColumnLifecycleEvent,
) -> None:
    """
    Log a column lifecycle event in a human-readable form.
    """
    msg = f"[COLUMN] {caller} | {ev.event.upper()} | {ev.col}"
    if ev.reason:
        msg += f" | {ev.reason}"

    log.info(msg)
