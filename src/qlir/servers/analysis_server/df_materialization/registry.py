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

from .builders import (
    build_df_path_len_cols,
    build_df_boll
)

# A DF builder takes the shared base_df (load+clean output)
# and returns a derived DataFrame.
DFBuilder = Callable[[pd.DataFrame], pd.DataFrame]

# Authoritative mapping: df_name -> DFBuilder
# This file must remain boring:
# - no imports from analyses
# - no registration logic
# - no side effects
DF_REGISTRY: Dict[str, DFBuilder] = {

    "df_path_len_cols": build_df_path_len_cols,
    "df_boll": build_df_boll
}
