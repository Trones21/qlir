import pandas as pd

from qlir.servers.analysis_server.df_materialization.registry import DF_REGISTRY
from qlir.servers.analysis_server.df_materialization.registrar import register_df
from qlir.servers.analysis_server.df_materialization.materialize import materialize_required_dfs

def setup_function():
    DF_REGISTRY.clear()


def test_materialize_multiple_dfs():
    def build_a(df):
        out = df.copy()
        out["a"] = 1
        return out

    def build_b(df):
        out = df.copy()
        out["b"] = 2
        return out

    register_df("df_a", build_a)
    register_df("df_b", build_b)

    base = pd.DataFrame({"x": [10, 20]})

    out = materialize_required_dfs(
        base_df=base,
        required_df_names={"df_a", "df_b"},
    )

    assert set(out.keys()) == {"df_a", "df_b"}
    assert "a" in out["df_a"].columns
    assert "b" in out["df_b"].columns
