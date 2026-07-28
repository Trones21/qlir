"""
Parity harness for the incremental ETL provider.

Replays an evolving agg directory (append to head, rotate head -> sealed chunk,
gaps and duplicate candles at seams, sliding window) and asserts that the
INCREMENTAL provider (persistent cache across the whole replay) produces exactly
the same frame a fresh FULL clean would at every step.

Two pipelines are exercised:
  - candles_v1: sort + dedupe only (no new rows).
  - fill_v1:    sort + dedupe + reindex-to-complete-minutes + ffill. This
                *synthesizes* rows at gaps, so a gap on the sealed<->head seam is
                the exact case incremental could get wrong.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from qlir.servers.analysis_server.etl.pipeline_spec import ETLPipeline, CANDLES_V1
from qlir.servers.analysis_server.etl.pipelines.first_pipeline import clean_data
from qlir.servers.analysis_server.io.clean_data_provider import (
    CleanDataProvider,
    FULL_EACH_LOOP,
    INCREMENTAL,
)

pytestmark = pytest.mark.analysis_server

MIN_MS = 60_000
BASE_MS = 1_609_459_200_000  # 2021-01-01T00:00:00Z, in ms (> 1e12 so read as ms)


def candles(start_min: int, n: int, *, drop: set[int] | None = None) -> pd.DataFrame:
    """n one-minute candles starting `start_min` minutes after BASE. `drop` omits
    those (absolute) minute offsets to create gaps."""
    drop = drop or set()
    mins = [start_min + i for i in range(n) if (start_min + i) not in drop]
    return pd.DataFrame(
        {
            "open_time": [BASE_MS + m * MIN_MS for m in mins],
            "open": [1.0] * len(mins),
            "high": [2.0] * len(mins),
            "low": [0.5] * len(mins),
            "close": [1.5] * len(mins),
            "volume": [10.0] * len(mins),
        }
    )


def _fill(raw: pd.DataFrame) -> pd.DataFrame:
    """Gap-materializing pipeline: clean, then reindex to a complete minute grid."""
    clean = clean_data(raw.copy())
    full_idx = pd.date_range(clean.index.min(), clean.index.max(), freq="60s", tz="UTC")
    filled = clean.reindex(full_idx).ffill()
    filled["tz_start"] = filled.index
    return filled


FILL_V1 = ETLPipeline(name="fill_v1", run_full=_fill, overlap_files=1, incremental_safe=True)

PIPELINES = [CANDLES_V1, FILL_V1]


def _assert_parity(incr: CleanDataProvider, agg_dir: Path, pipeline: ETLPipeline, label: str):
    full = CleanDataProvider(agg_dir, pipeline, last_n_files=incr.last_n_files, mode=FULL_EACH_LOOP)
    got = incr.get()
    exp = full.get()
    pd.testing.assert_frame_equal(
        got, exp, check_freq=False,
        obj=f"[{pipeline.name}] {label}",
    )


def _write(agg_dir: Path, name: str, df: pd.DataFrame) -> None:
    df.to_parquet(agg_dir / name)


@pytest.mark.parametrize("pipeline", PIPELINES, ids=lambda p: p.name)
@pytest.mark.parametrize("last_n_files", [0, 3])
def test_incremental_matches_full_through_evolution(tmp_path, pipeline, last_n_files):
    agg = tmp_path
    incr = CleanDataProvider(agg, pipeline, last_n_files=last_n_files, mode=INCREMENTAL)

    # State 1: cold start, only a head with a few candles
    _write(agg, "head.parquet", candles(0, 5))
    _assert_parity(incr, agg, pipeline, "cold-start head only")

    # State 2: head grows (append more candles)
    _write(agg, "head.parquet", candles(0, 9))
    _assert_parity(incr, agg, pipeline, "head grew")

    # State 3: rotation — old head sealed to part_000, fresh head continues
    _write(agg, "part_000.parquet", candles(0, 10))
    _write(agg, "head.parquet", candles(10, 4))
    _assert_parity(incr, agg, pipeline, "first rotation")

    # State 4: head grows after rotation
    _write(agg, "head.parquet", candles(10, 10))
    _assert_parity(incr, agg, pipeline, "head grew post-rotation")

    # State 5: rotation WITH a gap at the seam (minutes 20,21 missing) and a
    # duplicate boundary candle (minute 19 present in both chunk and head).
    _write(agg, "part_001.parquet", candles(10, 10))          # minutes 10..19
    _write(agg, "head.parquet", candles(19, 6, drop={20, 21}))  # 19(dup),22,23,24 (20,21 gap)
    _assert_parity(incr, agg, pipeline, "rotation with seam gap + dup")

    # State 6: another clean rotation to slide the window (matters for last_n=3)
    _write(agg, "part_002.parquet", candles(19, 6, drop={20, 21}))
    _write(agg, "head.parquet", candles(25, 5))
    _assert_parity(incr, agg, pipeline, "window slide")

    # State 7: head-only growth again
    _write(agg, "head.parquet", candles(25, 12))
    _assert_parity(incr, agg, pipeline, "final head growth")


def test_incremental_safe_false_falls_back_to_full(tmp_path):
    unsafe = ETLPipeline(name="unsafe", run_full=clean_data, incremental_safe=False)
    p = CleanDataProvider(tmp_path, unsafe, last_n_files=3, mode=INCREMENTAL)
    assert p.mode == FULL_EACH_LOOP
