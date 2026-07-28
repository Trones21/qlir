"""
Smoke test for the real analysis-loop iteration (server.run_loop_iteration).

Drives a few iterations against a temp agg dir and checks the loop mechanics
that unit tests of the provider don't cover:
  - new data is processed and the watermark advances,
  - an unchanged directory hits the freshness gate (skip, watermark held),
  - appended data bumps the fingerprint and is processed again.

Uses empty outboxes / no required DFs so no alert or DF-registry setup is needed;
timestamps are near-now so the staleness path stays quiet.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

from qlir.servers.analysis_server import server
from qlir.servers.analysis_server.etl.pipeline_spec import CANDLES_V1
from qlir.servers.analysis_server.io.clean_data_provider import CleanDataProvider, FULL_EACH_LOOP
from qlir.servers.analysis_server.runtime_state import runtime_state_get

pytestmark = pytest.mark.analysis_server

MIN_MS = 60_000


def test_loop_iteration_gate_and_watermark(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)  # cwd-relative state files land in tmp
    agg = tmp_path / "agg"
    agg.mkdir()

    anchor = datetime.now(timezone.utc).replace(second=0, microsecond=0) - timedelta(minutes=5)
    anchor_ms = int(anchor.timestamp() * 1000)

    def candles(n: int) -> pd.DataFrame:
        t = [anchor_ms + i * MIN_MS for i in range(n)]
        return pd.DataFrame(
            {
                "open_time": t,
                "open": [1.0] * n,
                "high": [2.0] * n,
                "low": [0.5] * n,
                "close": [1.5] * n,
                "volume": [10.0] * n,
            }
        )

    head = agg / "head.parquet"
    candles(5).to_parquet(head)

    provider = CleanDataProvider(agg, CANDLES_V1, last_n_files=3, mode=FULL_EACH_LOOP)
    alert_states: dict = {}
    state_path = str(tmp_path / "runtime_state.json")

    def step(lpt, lfp):
        return server.run_loop_iteration(
            provider=provider,
            parquet_dir=agg,
            outboxes={},
            required_df_names=set(),
            alert_states=alert_states,
            last_processed_ts=lpt,
            last_fingerprint=lfp,
            now=datetime.now(timezone.utc),
            poll_interval_sec=0,
            state_path=state_path,
        )

    # 1) first iteration processes new data -> watermark advances
    lpt, lfp = step(None, None)
    assert lpt is not None
    first_ts = lpt

    # 2) nothing changed on disk -> freshness gate skips, watermark held
    lpt2, lfp2 = step(lpt, lfp)
    assert lpt2 == first_ts
    assert lfp2 == lfp
    assert runtime_state_get("loop.skip_reason") == "no_new_data"

    # 3) a candle is appended (head rewritten) -> fingerprint changes -> processed
    candles(6).to_parquet(head)
    st = head.stat()
    bump = st.st_mtime_ns + 1_000_000_000  # guarantee an mtime change on any FS
    os.utime(head, ns=(bump, bump))

    lpt3, lfp3 = step(lpt2, lfp2)
    assert lpt3 > first_ts
    assert lfp3 != lfp2
