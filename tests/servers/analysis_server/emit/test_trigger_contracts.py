# tests/emit/test_trigger_contracts.py

import pytest
from qlir.servers.analysis_server.emit.validate import validate_trigger_registry

def test_df_and_events_is_invalid():
    registry = {
        "bad": {
            "df": "df_x",
            "column": "col",
            "events": ["a"],
        }
    }

    with pytest.raises(ValueError):
        validate_trigger_registry(registry)


def test_neither_df_nor_events_invalid():
    registry = {
        "bad": {
            "description": "oops",
        }
    }

    with pytest.raises(ValueError):
        validate_trigger_registry(registry)
