from __future__ import annotations

import os
import time
import logging
logger = logging.getLogger("qlir.manifest_aggregator")

from pathlib import Path
from typing import Dict, Any, Iterable
from datetime import datetime, timezone
from qlir.data.core.paths import get_symbol_interval_limit_raw_dir
from qlir.data.sources.binance.manifest_delta_log import (
    iter_manifest_deltas,
    apply_manifest_delta,
)


# KLINES MANIFEST (current location)
from qlir.data.sources.binance.endpoints.klines.manifest.manifest import (
    load_or_create_manifest,
)
from qlir.data.sources.binance.endpoints.klines.manifest.persist import (
    write_manifest_snapshot,
)

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Snapshot policy
# ---------------------------------------------------------------------------

SNAPSHOT_INTERVAL_SEC = 5.0
MAX_EVENTS_PER_SNAPSHOT = 100
MAX_DELTA_LOG_BYTES = 10 * 1024 * 1024  # 10 MB


# ---------------------------------------------------------------------------
# Process entrypoint
# ---------------------------------------------------------------------------

def run_manifest_aggregator(job_config, data_root: Path) -> None:
    """
    Long-running Binance manifest aggregation service.

    Notes:
    - Response artifacts are the source of truth
    - Manifest deltas describe new metadata derived from artifacts
    - manifest.json is a cached index over response files
    - safe to crash/restart

    """

    # Note can refactor this to be more generic if the aggregator ever needs to do it based on something else 
    # but this combo represents a complete identyity set for the type of data we are getting  
    sym_interval_limit_raw_dir = get_symbol_interval_limit_raw_dir(
        data_root=data_root,
        datasource=job_config.datasource, 
        endpoint=job_config.endpoint,
        symbol=job_config.symbol,
        interval=job_config.interval,
        limit=job_config.limit
    )

    # To easily toggle
    if os.getenv("QLIR_MANIFEST_LOG"):
        _setup_manifest_logging(sym_interval_limit_raw_dir / "logs")


    manifest_path = sym_interval_limit_raw_dir / "manifest.json"
    delta_log_path = sym_interval_limit_raw_dir / "manifest.delta"


    log.info(
        "Starting Binance manifest aggregator | dir=%s",
        sym_interval_limit_raw_dir,
    )

    # ---------------------------------------------------------------------
    # Load existing snapshot (if any)
    # ---------------------------------------------------------------------

    manifest: Dict[str, Any] = load_or_create_manifest(
        manifest_path=manifest_path,
        symbol=job_config.symbol,
        interval=job_config.interval,
        limit=job_config.limit,
    )

    last_snapshot_ts = time.monotonic()
    events_since_snapshot = 0
    delta_log_bytes_at_snapshot = delta_log_path.stat().st_size if delta_log_path.exists() else 0

    # ---------------------------------------------------------------------
    # Main loop
    # ---------------------------------------------------------------------

    try:
        while True:
            for event in _consume_delta_log(delta_log_path):
                apply_manifest_delta(manifest, event)
                slice_key = event.get("slice_comp_key")
                log.debug("Applied manifest delta | slice=%s", slice_key)
                events_since_snapshot += 1

                if _should_snapshot(
                    last_snapshot_ts=last_snapshot_ts,
                    events_since_snapshot=events_since_snapshot,
                    delta_log_path=delta_log_path,
                    delta_log_bytes_at_snapshot=delta_log_bytes_at_snapshot,
                ):
                    _write_snapshot(manifest, manifest_path)
                    last_snapshot_ts = time.monotonic()
                    events_since_snapshot = 0
                    delta_log_bytes_at_snapshot = delta_log_path.stat().st_size

            time.sleep(0.25)

    except KeyboardInterrupt:
        log.info("Manifest aggregator shutting down; writing final snapshot")
        _write_snapshot(manifest, manifest_path)
        return

    except Exception:
        log.exception("Manifest aggregator crashed")
        raise


# ---------------------------------------------------------------------------
# Delta Log consumption
# ---------------------------------------------------------------------------

def _consume_delta_log(delta_log_path: Path) -> Iterable[Dict[str, Any]]:
    if not delta_log_path.exists():
        return iter(())
    return iter_manifest_deltas(delta_log_path)



# ---------------------------------------------------------------------------
# Snapshot policy
# ---------------------------------------------------------------------------

def _should_snapshot(
    *,
    last_snapshot_ts: float,
    events_since_snapshot: int,
    delta_log_path: Path,
    delta_log_bytes_at_snapshot: int,
) -> bool:
    now = time.monotonic()

    # 1️⃣ Event-count based trigger
    if events_since_snapshot >= MAX_EVENTS_PER_SNAPSHOT:
        log.debug(
            "Snapshot triggered | reason=event_count events=%d threshold=%d",
            events_since_snapshot,
            MAX_EVENTS_PER_SNAPSHOT,
        )
        return True

    # 2️⃣ Time-based trigger
    elapsed = now - last_snapshot_ts
    if elapsed >= SNAPSHOT_INTERVAL_SEC:
        log.debug(
            "Snapshot triggered | reason=time elapsed=%.2fs threshold=%.2fs",
            elapsed,
            SNAPSHOT_INTERVAL_SEC,
        )
        return True

    # 3️⃣ Delta-log size-based trigger
    if delta_log_path.exists():
        current_size = delta_log_path.stat().st_size
        delta_bytes = current_size - delta_log_bytes_at_snapshot

        if delta_bytes >= MAX_DELTA_LOG_BYTES:
            log.debug(
                "Snapshot triggered | reason=delta_size bytes=%d threshold=%d",
                delta_bytes,
                MAX_DELTA_LOG_BYTES,
            )
            return True

    return False



# ---------------------------------------------------------------------------
# Snapshot write
# ---------------------------------------------------------------------------

def _write_snapshot(manifest: Dict[str, Any], manifest_path: Path) -> None:
    manifest.setdefault("summary", {})["last_evaluated_at"] = (
        datetime.now(timezone.utc).isoformat()
    )

    write_manifest_snapshot(
        manifest_path=manifest_path,
        manifest=manifest,
    )

    log.info(
        "Manifest snapshot written | slices=%d",
        len(manifest.get("slices", {})),
    )

# ---------------------------------------------------------------------------
#  Logging
# ---------------------------------------------------------------------------
# tail the file in a separate terminal

def _setup_manifest_logging(log_dir: Path) -> None:
    logger = logging.getLogger("qlir.manifest_aggregator")

    log_path = log_dir / "manifest_aggregator.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)

    handler = logging.FileHandler(log_path, encoding="utf-8")
    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )
    handler.setFormatter(formatter)

    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    logger.propagate = False  # 🔑 prevent stdout duplication
