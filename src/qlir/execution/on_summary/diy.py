from typing import Callable

import pandas as pd


class DIYSummaryExecutionRegistry:
    """
    Registry for user-defined summary execution models.

    This is an open, runtime-extensible namespace.
    """

    def __init__(self):
        self._models: dict[str, Callable[[pd.DataFrame], pd.DataFrame]] = {}

    def register(
        self,
        name: str,
        fn: Callable[[pd.DataFrame], pd.DataFrame],
        *,
        overwrite: bool = False,
    ) -> None:
        if name in self._models and not overwrite:
            raise ValueError(
                f"DIY summary execution model '{name}' already exists"
            )
        self._models[name] = fn

    def get(self, name: str) -> Callable[[pd.DataFrame], pd.DataFrame]:
        try:
            return self._models[name]
        except KeyError:
            raise ValueError(
                f"Unknown DIY summary execution model '{name}'"
            )

    def list(self) -> list[str]:
        return sorted(self._models)


# Public singleton
diy = DIYSummaryExecutionRegistry()
