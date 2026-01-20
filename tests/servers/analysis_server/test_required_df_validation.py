# tests/server/test_required_df_validation.py

import pandas as pd
import pytest

from qlir.servers.analysis_server.df_materialization.registry import DF_REGISTRY
from qlir.servers.analysis_server.server import _collect_required_df_names

def setup_function():
    DF_REGISTRY.clear()


def test_required_df_not_registered_fails_fast():
    outboxes = {
        "qlir-events": {
            "trigger_registry": {
                "t1": {"df": "df_missing", "column": "x"},
            },
            "active_triggers": ["t1"],
        }
    }

    required = _collect_required_df_names(outboxes)
    assert required == {"df_missing"}

    # Now try materializing (this should explode)
    with pytest.raises(KeyError):
        from qlir.servers.analysis_server.df_materialization.materialize import materialize_required_dfs
        materialize_required_dfs(
            base_df=pd.DataFrame({"x": [1]}),
            required_df_names=required,
        )
