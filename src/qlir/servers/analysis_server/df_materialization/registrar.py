from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Dict

import pandas as pd

from qlir.servers.analysis_server.df_materialization.registry import DF_REGISTRY

log = logging.getLogger(__name__)

# A DF builder takes the shared base_df (load+clean output)
# and returns a derived df (row-aligned unless explicitly documented otherwise).
DFBuilder = Callable[[pd.DataFrame], pd.DataFrame]


def register_df(df_name: str, builder: DFBuilder) -> None:
    """
    Register a derived DataFrame builder.
    """
    if df_name in DF_REGISTRY:
        raise KeyError(f"DF_REGISTRY already contains df '{df_name}'")
    DF_REGISTRY[df_name] = builder




