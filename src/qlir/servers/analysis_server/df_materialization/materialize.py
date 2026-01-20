import pandas as pd

from qlir.servers.analysis_server.df_materialization.registrar import DF_REGISTRY


def materialize_required_dfs(
    *,
    base_df: pd.DataFrame,
    required_df_names: set[str],
) -> dict[str, pd.DataFrame]:
    """
    Materialize the required derived DataFrames for this iteration.

    Rules:
    - All derived DFs start from the same base_df (load+clean output).
    - Builders must be deterministic and side-effect free.
    - Missing df_name is a hard error (wiring problem).
    """
    out: dict[str, pd.DataFrame] = {}

    for df_name in sorted(required_df_names):
        builder = DF_REGISTRY.get(df_name)
        if builder is None:
            raise KeyError(
                f"Required derived DF '{df_name}' not registered in DF_REGISTRY"
            )

        df = builder(base_df)

        if not isinstance(df, pd.DataFrame):
            raise TypeError(
                f"DF builder for '{df_name}' returned {type(df)} not pd.DataFrame"
            )

        out[df_name] = df

    return out