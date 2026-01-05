from typing import Any

import pandas as _pd

from qlir.execution.on_summary.execution_models import (
    SummaryExecutionModel,
    BUILTIN_SUMMARY_EXECUTION_REGISTRY,
)
from qlir.execution.on_summary.diy import diy


def execute_summary(
    paths: _pd.DataFrame,
    *,
    model: SummaryExecutionModel | tuple[str, str],
    **kwargs: Any,
) -> _pd.DataFrame:
    """
    Execute a summary-level execution model.

    Parameters
    ----------
    paths
        Path-level summarized DataFrame.
    model
        Either:
        - SummaryExecutionModel (built-in), or
        - ("diy", "<name>") for user-registered models
    """
    # Built-in execution
    if isinstance(model, SummaryExecutionModel):
        fn = BUILTIN_SUMMARY_EXECUTION_REGISTRY[model]
        return fn(paths, **kwargs)

    # Namespaced DIY execution
    if isinstance(model, tuple):
        namespace, name = model

        if namespace != "diy":
            raise ValueError(f"Unknown execution namespace '{namespace}'")

        fn = diy.get(name)
        return fn(paths, **kwargs)

    raise TypeError(
        "model must be SummaryExecutionModel or ('diy', <name>)"
    )
