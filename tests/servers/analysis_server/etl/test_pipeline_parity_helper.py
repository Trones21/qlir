"""
Tests for the reusable `assert_incremental_parity` contract-check.

- Positive: the shipped pipelines (dedupe-only and gap-materializing) pass the
  helper against a stream containing gaps and duplicates.
- Negative (teeth): a deliberately *non-local* pipeline — one whose output
  depends on all prior history — MUST be caught by the helper. A parity checker
  that never fails is worthless, so we assert it raises.
"""
from __future__ import annotations

import pandas as pd
import pytest

from qlir.servers.analysis_server.etl.pipeline_spec import ETLPipeline, CANDLES_V1
from qlir.servers.analysis_server.etl.pipelines.first_pipeline import clean_data
from qlir.servers.analysis_server.etl.parity import assert_incremental_parity

pytestmark = pytest.mark.analysis_server

MIN_MS = 60_000
BASE_MS = 1_609_459_200_000  # 2021-01-01T00:00:00Z in ms


def raw_stream_with_gaps_and_dups() -> pd.DataFrame:
    """25 one-minute candles, minutes 12-13 missing (gap), minute 7 duplicated."""
    minutes = [m for m in range(25) if m not in (12, 13)]
    minutes.append(7)  # a duplicate candle
    minutes.sort()
    return pd.DataFrame(
        {
            "open_time": [BASE_MS + m * MIN_MS for m in minutes],
            "open": [1.0] * len(minutes),
            "high": [2.0] * len(minutes),
            "low": [0.5] * len(minutes),
            "close": [1.5] * len(minutes),
            "volume": [10.0] * len(minutes),
        }
    )


def _fill(raw: pd.DataFrame) -> pd.DataFrame:
    clean = clean_data(raw.copy())
    full_idx = pd.date_range(clean.index.min(), clean.index.max(), freq="60s", tz="UTC")
    filled = clean.reindex(full_idx).ffill()
    filled["tz_start"] = filled.index
    return filled


FILL_V1 = ETLPipeline(name="fill_v1", run_full=_fill, overlap_files=1, incremental_safe=True)


@pytest.mark.parametrize("pipeline", [CANDLES_V1, FILL_V1], ids=lambda p: p.name)
def test_shipped_pipelines_are_incremental_safe(tmp_path, pipeline):
    assert_incremental_parity(
        pipeline, raw_stream_with_gaps_and_dups(), tmp_path=tmp_path, chunk_rows=10
    )


def _nonlocal(raw: pd.DataFrame) -> pd.DataFrame:
    """Contract violation: a column that depends on the count of ALL rows seen,
    not just local context. Incremental mode cannot reproduce this."""
    clean = clean_data(raw.copy()).copy()
    clean["global_idx"] = range(len(clean))
    return clean


NONLOCAL = ETLPipeline(name="nonlocal", run_full=_nonlocal, overlap_files=1, incremental_safe=True)


def test_helper_catches_nonlocal_pipeline(tmp_path):
    """The parity check must have teeth: a non-local pipeline is detected."""
    with pytest.raises(AssertionError):
        assert_incremental_parity(
            NONLOCAL, raw_stream_with_gaps_and_dups(), tmp_path=tmp_path, chunk_rows=10
        )
