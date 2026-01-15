from __future__ import annotations

import json
import logging
import os
import time

from qlir.data.sources.binance.server import BinanceServerConfig

log = logging.getLogger("qlir.manifest_aggregator")

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

from qlir.data.core.paths import get_symbol_interval_limit_raw_dir

# KLINES MANIFEST (current location)
from qlir.data.sources.binance.endpoints.klines.manifest.manifest import (
    load_existing_manifest_snapshot,
    snapshot_created_at,
    write_manifest_snapshot,
)
from qlir.data.sources.binance.manifest_delta_log import (
    apply_manifest_delta,
)

# ---------------------------------------------------------------------------
# Snapshot policy
# ---------------------------------------------------------------------------

SNAPSHOT_INTERVAL_SEC = 120
MAX_EVENTS_PER_SNAPSHOT = 5
MAX_DELTA_LOG_BYTES = 100 * 1024 * 1024   # 100MB 


# ---------------------------------------------------------------------------
# Process entrypoint
# ---------------------------------------------------------------------------

def run_manifest_delta_service(server_config: BinanceServerConfig, data_root: Path) -> None:
    """
    Long-running Binance manifest aggregation service.

    Notes:
    - Response artifacts are the source of truth
    - Manifest deltas describe new metadata derived from artifacts
    - manifest.json is a cached index over response files
    - safe to crash/restart

    """

    datasource=server_config.datasource
    endpoint=server_config.endpoint
    symbol=server_config.job_config.symbol
    interval=server_config.job_config.interval
    limit=server_config.job_config.limit

    # Note can refactor this to be more generic if the aggregator ever needs to do it based on something else 
    # but this combo represents a complete identyity set for the type of data we are getting  
    sym_interval_limit_raw_dir = get_symbol_interval_limit_raw_dir(
        data_root=data_root,
        datasource=datasource, 
        endpoint=endpoint,
        symbol=symbol,
        interval=interval,
        limit=limit
    )

    # To easily toggle
    if os.getenv("QLIR_MANIFEST_LOG"):
        log.info("Enabling delta log service logging because QLIR_MANIFEST_LOG is set")
        _setup_manifest_logging(sym_interval_limit_raw_dir / "logs")
    else:
        log.info("Set QLIR_MANIFEST_LOG to enable delta log service logging")

    manifest_path = sym_interval_limit_raw_dir / "manifest.json"
    delta_log_path = sym_interval_limit_raw_dir / "manifest.delta"


    log.info(
        "Starting Binance manifest aggregator | dir=%s",
        sym_interval_limit_raw_dir,
    )

    # ---------------------------------------------------------------------
    # Set Path Where this service can pickup full manifests dropped off by the worker
    # ---------------------------------------------------------------------
    snapshot_dir = sym_interval_limit_raw_dir.joinpath("manifest_snapshot")
    snapshot_path = snapshot_dir / "manifest.snapshot.json"


    # ---------------------------------------------------------------------
    # Wait for a Manifest Snapshot 
    # ---------------------------------------------------------------------
    log.info("Waiting for manifest.snapshot.json to exist | path=%s", snapshot_path)
    
    while True:
        if snapshot_path.exists() and snapshot_path.stat().st_size > 0:
                break
        log.warning("STILL waiting for manifest.snapshot.json to exist | path=%s", snapshot_path)
        time.sleep(0.5)

    log.info("Loading Manifest into Aggregator")

    manifest: Dict[str, Any] = load_existing_manifest_snapshot(
        snapshot_path=snapshot_path
    )

    last_snapshot_ts = time.monotonic()
    events_since_snapshot = 0
    delta_log_bytes_at_snapshot = delta_log_path.stat().st_size if delta_log_path.exists() else 0

    # ---------------------------------------------------------------------
    # Bootstrap: apply all existing deltas once
    # ---------------------------------------------------------------------

    log.info("Applying existing manifest deltas (bootstrap)")

    delta_offset = 0

    if delta_log_path.exists():
        with delta_log_path.open("r", encoding="utf-8") as f:
            for line in f:
                delta = json.loads(line)
                apply_manifest_delta(manifest, delta)
            delta_offset = f.tell()

    log.info("Bootstrap complete | delta_offset=%d", delta_offset)


    # ---------------------------------------------------------------------
    # Main loop
    # ---------------------------------------------------------------------

    try:
        while True:
            if delta_log_path.exists():
                with delta_log_path.open("r", encoding="utf-8") as f:
                    f.seek(delta_offset)
                    for line in f:
                        delta = json.loads(line)
                        apply_manifest_delta(manifest, delta)
                        events_since_snapshot += 1
                    delta_offset = f.tell()

            if _should_snapshot(
                last_snapshot_ts=last_snapshot_ts,
                events_since_snapshot=events_since_snapshot,
                delta_log_path=delta_log_path,
                delta_log_bytes_at_snapshot=delta_log_bytes_at_snapshot,
            ):
                _write_snapshot(manifest, manifest_path)
                last_snapshot_ts = time.monotonic()
                events_since_snapshot = 0
                if delta_log_path.exists():
                    delta_log_bytes_at_snapshot = delta_log_path.stat().st_size
                else:
                    delta_log_bytes_at_snapshot = 0

            # Apply a Full Snapshot if it exists
            if snapshot_path.exists():
                dt = snapshot_created_at(snapshot_path)
                log.info(f"Full manifest snapshot detected. Snapshot created at: {dt}")

                with snapshot_path.open("r", encoding="utf-8") as f:
                    manifest = json.load(f)

                write_manifest_snapshot(
                    manifest_path=manifest_path,
                    manifest=manifest,
                )

                snapshot_path.unlink()  # consume

            time.sleep(0.25)

    except KeyboardInterrupt:
        log.info("Manifest aggregator shutting down; writing final snapshot")
        _write_snapshot(manifest, manifest_path)
        return

    except Exception:
        log.exception("Manifest aggregator crashed")
        raise


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
    log = logging.getLogger("qlir.manifest_aggregator")

    log_path = log_dir / "manifest_aggregator.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)

    handler = logging.FileHandler(log_path, encoding="utf-8")
    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )
    handler.setFormatter(formatter)

    log.addHandler(handler)
    log.setLevel(logging.DEBUG)
    log.propagate = False  # 🔑 prevent stdout duplication
    log.info("zkp-Setup manifest aggregator logger - qlir.manifest_aggregator")




