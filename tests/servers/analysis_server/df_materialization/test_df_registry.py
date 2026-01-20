# tests/df_materialization/test_registry.py

import pandas as pd
import pytest

from qlir.servers.analysis_server.df_materialization.registry import DF_REGISTRY
from qlir.servers.analysis_server.df_materialization.registrar import register_df
from qlir.servers.analysis_server.df_materialization.materialize import materialize_required_dfs

def setup_function():
    DF_REGISTRY.clear()


def test_register_df():
    def builder(df: pd.DataFrame) -> pd.DataFrame:
        return df

    register_df("df_test", builder)
    assert "df_test" in DF_REGISTRY


def test_register_df_duplicate_raises():
    def builder(df): return df

    register_df("df_test", builder)
    with pytest.raises(KeyError):
        register_df("df_test", builder)


def test_materialize_missing_df_raises():
    base = pd.DataFrame({"a": [1]})

    with pytest.raises(KeyError):
        materialize_required_dfs(
            base_df=base,
            required_df_names={"df_missing"},
        )


def test_materialize_calls_builder():
    called = False

    def builder(df):
        nonlocal called
        called = True
        return df.copy()

    register_df("df_test", builder)

    out = materialize_required_dfs(
        base_df=pd.DataFrame({"x": [1]}),
        required_df_names={"df_test"},
    )

    assert called is True
    assert "df_test" in out
