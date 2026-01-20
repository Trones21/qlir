# tests/server/test_event_conditions.py

import pytest
from qlir.servers.analysis_server.server import _eval_events_condition

def test_all_condition():
    assert _eval_events_condition(
        required_events=["a", "b"],
        condition="ALL",
        triggered_events={"a", "b", "c"},
    )

def test_all_condition_false():
    assert not _eval_events_condition(
        required_events=["a", "b"],
        condition="ALL",
        triggered_events={"a"},
    )

def test_any_condition():
    assert _eval_events_condition(
        required_events=["a", "b"],
        condition="ANY",
        triggered_events={"b"},
    )

def test_any_condition_false():
    assert not _eval_events_condition(
        required_events=["a", "b"],
        condition="ANY",
        triggered_events={"c"},
    )

def test_empty_required_events_false():
    assert not _eval_events_condition(
        required_events=[],
        condition="ALL",
        triggered_events={"a"},
    )

def test_unknown_condition_raises():
    with pytest.raises(ValueError):
        _eval_events_condition(
            required_events=["a"],
            condition="WEIRD",
            triggered_events={"a"},
        )
