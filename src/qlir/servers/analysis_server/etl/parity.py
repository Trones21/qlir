# analysis_server/etl/parity.py
"""
Reusable parity check: prove a user-authored ETL pipeline is incremental-safe.

The incremental provider only matches full mode when a pipeline's cross-row
dependencies stay within `overlap_files` chunks. Since the ETL is user-authored,
every pipeline should be validated — a pipeline that reaches further back (a
global normalization, a cumulative over all history, a rolling window wider than
the overlap) will silently diverge in incremental mode.

This helper needs no production data. Given a *representative* raw stream, it
simulates the agg server (rows arrive, full chunks seal, the tail stays as the
volatile head) and asserts the incremental provider produces the exact frame a
full clean would, at every step and for several window sizes.

Framework-agnostic: raises AssertionError (naming the offending step) on
divergence. Call it from your own test:

    def test_my_pipeline_is_incremental_safe(tmp_path):
        raw = my_representative_raw_rows()      # a plain DataFrame
        assert_incremental_parity(MY_PIPELINE, raw, tmp_path=tmp_path)
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from qlir.servers.analysis_server.etl.pipeline_spec import ETLPipeline
from qlir.servers.analysis_server.io.clean_data_provider import (
    CleanDataProvider,
    FULL_EACH_LOOP,
    INCREMENTAL,
)

HEAD_NAME = "head.parquet"


def _materialize(agg_dir: Path, revealed: pd.DataFrame, chunk_rows: int) -> None:
    """
    Lay `revealed` rows out as sealed chunks + a non-empty head, mimicking the
    agg server. Sealed chunks are written **once** and never rewritten (their
    mtime stays stable, so the provider's sealed cache is genuinely reused); only
    the head is rewritten each step, because only the head is volatile.
    """
    n = len(revealed)
    if n == 0:
        return
    n_sealed = (n - 1) // chunk_rows  # keep >= 1 row in the head
    for i in range(n_sealed):
        p = agg_dir / f"part_{i:04d}.parquet"
        if not p.exists():  # sealed chunk == immutable; write once
            revealed.iloc[i * chunk_rows : (i + 1) * chunk_rows].to_parquet(p)
    revealed.iloc[n_sealed * chunk_rows :].to_parquet(agg_dir / HEAD_NAME)


def assert_incremental_parity(
    pipeline: ETLPipeline,
    raw_stream: pd.DataFrame,
    *,
    tmp_path: Path,
    chunk_rows: int = 10,
    reveal_every: int = 3,
    last_n_files_values: tuple[int, ...] = (0, 3),
) -> None:
    """
    Replay `raw_stream` as it 'arrives' and assert incremental == full at every
    reveal point, for each window size in `last_n_files_values`.

    Raises AssertionError (naming the pipeline, window size, and how many rows
    had arrived) at the first point the incremental output diverges from full.
    """
    n = len(raw_stream)
    if n == 0:
        raise ValueError("raw_stream is empty; provide a representative stream")

    reveal_points = sorted(set(range(1, n + 1, reveal_every)) | {n})

    for last_n in last_n_files_values:
        agg = Path(tmp_path) / f"agg_ln{last_n}"
        agg.mkdir(parents=True, exist_ok=True)
        incr = CleanDataProvider(agg, pipeline, last_n_files=last_n, mode=INCREMENTAL)

        for k in reveal_points:
            _materialize(agg, raw_stream.iloc[:k], chunk_rows)
            full = CleanDataProvider(
                agg, pipeline, last_n_files=last_n, mode=FULL_EACH_LOOP
            )
            pd.testing.assert_frame_equal(
                incr.get(),
                full.get(),
                check_freq=False,
                obj=f"[{pipeline.name}] last_n_files={last_n} after {k}/{n} rows",
            )
