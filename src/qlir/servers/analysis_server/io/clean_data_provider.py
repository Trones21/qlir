# analysis_server/io/clean_data_provider.py
"""
Stateful loader that returns the analysis-ready DataFrame each loop.

Two modes:

  full_each_loop
      Read the same file window and run the full ETL every call. Identical to the
      original behavior, and the correctness oracle for incremental.

  incremental
      Cache the cleaned output of the *sealed* chunks and, each loop, recompute
      only `overlap_files` sealed chunks + the volatile head, then splice at the
      last sealed timestamp. See INCREMENTAL_ETL_DESIGN.md.

Why the splice is correct (given the pipeline's locality contract):
  - `sealed_clean` = run_full(all sealed in window) — every internal
    sealed↔sealed boundary is resolved in a single clean, and it only changes
    when the sealed set changes (rotation / window slide), not every loop.
  - `head_clean` = run_full(last `overlap_files` sealed + head). The trailing
    sealed chunks are *left-context* so the first gap/dedup after the last sealed
    candle is computed correctly; we then keep only head_clean rows strictly
    after the last sealed timestamp (`cut`). A sealed candle's own value never
    depends on later data, so the cached sealed rows match full mode, and the new
    tail matches full mode as long as the pipeline's dependencies stay within
    `overlap_files` chunks.
"""
from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from qlir.telemetry.telemetry import telemetry
from qlir.servers.analysis_server.etl.pipeline_spec import ETLPipeline
from qlir.servers.analysis_server.io import parquet_dir as pdir

log = logging.getLogger(__name__)

FULL_EACH_LOOP = "full_each_loop"
INCREMENTAL = "incremental"
VALID_MODES = (FULL_EACH_LOOP, INCREMENTAL)
_ETL_LOG = Path("telemetry/etl_times.log")


class CleanDataProvider:
    def __init__(
        self,
        agg_dir: Path | str,
        pipeline: ETLPipeline,
        *,
        last_n_files: int,
        mode: str = FULL_EACH_LOOP,
    ) -> None:
        if mode not in VALID_MODES:
            raise ValueError(
                f"Unknown ETL mode {mode!r}; expected one of {VALID_MODES}. "
                "Check QLIR_ETL_MODE."
            )

        self.agg_dir = Path(agg_dir)
        self.pipeline = pipeline
        self.last_n_files = last_n_files
        self.mode = mode

        # incremental cache
        self._sealed_clean: pd.DataFrame | None = None
        self._sealed_key: tuple | None = None

        if mode == INCREMENTAL and not pipeline.incremental_safe:
            log.warning(
                "ETL pipeline '%s' is not incremental_safe; running full_each_loop.",
                pipeline.name,
            )
            self.mode = FULL_EACH_LOOP

        if self.mode == INCREMENTAL and last_n_files <= 0:
            log.warning(
                "incremental mode with last_n_files=%d (full dataset) keeps the "
                "entire cleaned history in memory and grows without bound over a "
                "long run. Use a bounded window (last_n_files > 0) for long-lived "
                "servers.",
                last_n_files,
            )

    # -- public -----------------------------------------------------------

    def get(self) -> pd.DataFrame:
        if self.mode == INCREMENTAL:
            return self._get_incremental()
        return self._get_full()

    # -- window selection (mirrors load_latest_parquet_window) ------------

    def _window_sealed(self, sealed: list[Path]) -> list[Path]:
        """Sealed chunks the current window covers."""
        if self.last_n_files > 0:
            n_chunks = self.last_n_files - 1
            return sealed[-n_chunks:] if n_chunks > 0 else []
        return list(sealed)  # last_n_files <= 0 => full dataset

    # -- full mode --------------------------------------------------------

    @telemetry(console=True, log_path=_ETL_LOG)
    def _get_full(self) -> pd.DataFrame:
        sealed, head = pdir.classify(self.agg_dir)
        window = self._window_sealed(sealed) + ([head] if head is not None else [])
        raw = pdir.read_concat(window)
        if raw.empty:
            return raw
        return self.pipeline.run_full(raw)

    # -- incremental mode -------------------------------------------------

    @telemetry(console=True, log_path=_ETL_LOG)
    def _get_incremental(self) -> pd.DataFrame:
        sealed, head = pdir.classify(self.agg_dir)
        window_sealed = self._window_sealed(sealed)
        ts_col = self.pipeline.ts_col

        # Cache clean(window_sealed); recompute only when the sealed set changes.
        key = pdir.file_key(window_sealed)
        if key != self._sealed_key:
            raw = pdir.read_concat(window_sealed)
            self._sealed_clean = (
                self.pipeline.run_full(raw) if not raw.empty else pd.DataFrame()
            )
            self._sealed_key = key
        sealed_clean = self._sealed_clean

        if head is None:
            return sealed_clean.copy()

        # Recompute the volatile region: trailing sealed chunks (context) + head.
        overlap = (
            window_sealed[-self.pipeline.overlap_files :]
            if self.pipeline.overlap_files > 0
            else []
        )
        recompute_raw = pdir.read_concat(overlap + [head])
        if recompute_raw.empty:
            return sealed_clean.copy()
        head_clean = self.pipeline.run_full(recompute_raw)

        if sealed_clean is None or sealed_clean.empty:
            return head_clean

        # Splice at the last sealed timestamp: keep cached sealed rows, take only
        # the recomputed rows strictly newer than the last sealed candle.
        cut = sealed_clean[ts_col].max()
        new_tail = head_clean[head_clean[ts_col] > cut]
        return pd.concat([sealed_clean, new_tail])
