# analysis_server/etl/pipeline_spec.py
"""
An ETL pipeline is user-authored: they write the `run_full` transform (and the
analysis flow that consumes it). This spec is the small contract that lets the
same pipeline run in either `full_each_loop` or `incremental` mode without the
author writing a bespoke incremental version.

Incremental contract (what an author must guarantee to be `incremental_safe`):
  - `run_full(raw_df) -> clean_df` is deterministic and its cross-row
    dependencies are *local* — a row's cleaned value depends only on rows within
    `overlap_files` chunks of it. The provider recomputes the last
    `overlap_files` sealed chunks + the volatile head each loop to supply that
    left-context; a pipeline that reaches further back will not match full mode.
  - The clean output carries `ts_col` (UTC, sortable). The provider splices the
    cached sealed history and the freshly-recomputed head at the last sealed
    timestamp, so `ts_col` must be present and monotonic after cleaning.
  - Set `incremental_safe=False` for a pipeline that genuinely needs unbounded
    history; the provider then transparently falls back to full-each-loop.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

import pandas as pd

from qlir.servers.analysis_server.etl.pipelines.first_pipeline import clean_data


@dataclass(frozen=True)
class ETLPipeline:
    name: str
    run_full: Callable[[pd.DataFrame], pd.DataFrame]
    overlap_files: int = 1
    incremental_safe: bool = True
    ts_col: str = "tz_start"


# --- registry (mirrors the DF_REGISTRY idiom) -----------------------------

CANDLES_V1 = ETLPipeline(
    name="candles_v1",
    run_full=clean_data,
    overlap_files=1,
    incremental_safe=True,
)

_REGISTRY: dict[str, ETLPipeline] = {p.name: p for p in (CANDLES_V1,)}


def get_pipeline(name: str) -> ETLPipeline:
    if name not in _REGISTRY:
        raise KeyError(
            f"Unknown ETL pipeline '{name}'. Registered: {sorted(_REGISTRY)}"
        )
    return _REGISTRY[name]


def register_pipeline(pipeline: ETLPipeline) -> None:
    _REGISTRY[pipeline.name] = pipeline
