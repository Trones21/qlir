from qlir.execution.on_summary.diy import diy
from qlir.execution.on_summary.execution_models import SummaryExecutionModel


def list_available_summary_execution_models() -> dict[str, list[str]]:
    """
    List all available summary execution models.

    Returns
    -------
    dict
        {
            "builtin": [...],
            "diy": [...]
        }
    """
    return {
        "builtin": [model.value for model in SummaryExecutionModel],
        "diy": diy.list(),
    }
