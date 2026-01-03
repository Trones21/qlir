from enum import StrEnum

from qlir.execution.on_summary.models import (
    best_entry_exit_on_close,
    mid_entry_exit_on_close,
    worst_entry_exit_on_close,
)


class SummaryExecutionModel(StrEnum):
    """
    Built-in summary execution models.

    This is a closed set and MUST NOT be extended at runtime.
    """
    BEST_ENTRY_EXIT_ON_CLOSE = "best_entry_exit_on_close"
    MID_ENTRY_EXIT_ON_CLOSE = "mid_entry_exit_on_close"
    WORST_ENTRY_EXIT_ON_CLOSE = "worst_entry_exit_on_close"


BUILTIN_SUMMARY_EXECUTION_REGISTRY = {
    SummaryExecutionModel.BEST_ENTRY_EXIT_ON_CLOSE: best_entry_exit_on_close.execute,
    SummaryExecutionModel.MID_ENTRY_EXIT_ON_CLOSE: mid_entry_exit_on_close.execute,
    SummaryExecutionModel.WORST_ENTRY_EXIT_ON_CLOSE: worst_entry_exit_on_close.execute,
}
