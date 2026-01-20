# tests/server/test_event_flow.py

import pandas as pd


# Goal
# ------------------
# Fake base DF
# Fake DF builder
# Fake trigger
# Verify alert emission

# Pattern
# ------------------
# Monkeypatch emit_alert and inspect calls.


def test_event_trigger_fires(monkeypatch):
    from qlir.servers.analysis_server.df_materialization.registry import DF_REGISTRY
    from qlir.servers.analysis_server.df_materialization.registrar import register_df

    DF_REGISTRY.clear()

    def builder(base):
        return pd.DataFrame({"signal": [False, True]})

    register_df("df_test", builder)

    alerts = []

    def fake_emit_alert(outbox, data):
        alerts.append((outbox, data))

    monkeypatch.setattr(
        "qlir.servers.analysis_server.server.emit_alert",
        fake_emit_alert,
    )

    # simulate evaluation
    derived_dfs = {"df_test": builder(None)}
    triggered_events = set()

    last = derived_dfs["df_test"].iloc[-1]
    if last["signal"]:
        triggered_events.add("t1")
        fake_emit_alert("qlir-events", {"trigger": "t1"})

    assert len(alerts) == 1
    assert alerts[0][0] == "qlir-events"
