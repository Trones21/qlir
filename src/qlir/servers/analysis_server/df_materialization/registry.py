# Example 
# from .builders import (
#     build_df_sma_14_direction,
#     build_df_open_sma_14_survival,
# )

# DF_REGISTRY = {
#     "df_sma_14_direction": build_df_sma_14_direction,
#     "df_open_sma_14_survival": build_df_open_sma_14_survival,
# }

from __future__ import annotations

from collections.abc import Callable
from typing import Dict

import pandas as pd

# A DF builder takes the shared base_df (load+clean output)
# and returns a derived DataFrame.
DFBuilder = Callable[[pd.DataFrame], pd.DataFrame]

# THIS SHOULD REMAIN EMPTY
# server.py calls registration.register_all() (or something else), that then populates this registry (with each call for register_df)
DF_REGISTRY: Dict[str, DFBuilder] = {}

