from __future__ import annotations

import json
import logging
import os
import time

from qlir.data.sources.interactive_brokers.server_config_models import IBKRServerConfig

log = logging.getLogger("qlir.ibkr.manifest_builder")

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

from qlir.data.core.paths import get_symbol_interval_limit_raw_dir
from qlir.data.sources.interactive_brokers.endpoints.historical_bars.manifest.manifest import (
    load_existing_manifest_snapshot,
    snapshot_created_at,
    write_manifest_snapshot,
)
from qlir.data.sources.interactive_brokers.manifest_delta_log import apply_manifest_delta

# ---------------------------------------------------------------------------
# Snapshot policy
# ---------------------------------------------------------------------------

SNAPSHOT_INTERVAL_SEC = 120
MAX_EVENTS_PER_SNAPSHOT = 5
MAX_DELTA_LOG_BYTES = 100 * 1024 * 1024  # 100MB


def run_manifest_delta_service(server_config: IBKRServerConfig, data_root: Path) -> None:
    """
    Long-running IBKR manifest builder (the "Manifest Builder" process).

    Sole writer of manifest.json. Consumes the Fetcher's snapshot + delta log.
    Response artifacts are the source of truth; manifest.json is a cached index.
    """
    datasource = server_config.datasource
    endpoint = server_config.endpoint
    symbol = server_config.job_config.symbol
    interval = server_config.job_config.interval
    limit = server_config.job_config.limit

    sym_interval_limit_raw_dir = get_symbol_interval_limit_raw_dir(
        data_root=data_root,
        datasource=datasource,
        endpoint=endpoint,
        symbol=symbol,
        interval=interval,
        limit=limit,
    )

    if os.getenv("QLIR_MANIFEST_LOG"):
        log.info("Enabling delta log service logging because QLIR_MANIFEST_LOG is set")
        _setup_manifest_logging(sym_interval_limit_raw_dir / "logs")
    else:
        log.info("Set QLIR_MANIFEST_LOG to enable delta log service logging")

    manifest_path = sym_interval_limit_raw_dir / "manifest.json"
    delta_log_path = sym_interval_limit_raw_dir / "manifest.delta"

    log.info("Starting IBKR manifest builder | dir=%s", sym_interval_limit_raw_dir)

    snapshot_dir = sym_interval_limit_raw_dir.joinpath("manifest_snapshot")
    snapshot_path = snapshot_dir / "manifest.snapshot.json"

    # ---- wait for the Fetcher's startup snapshot ----
    log.info("Waiting for manifest.snapshot.json to exist | path=%s", snapshot_path)
    while True:
        if snapshot_path.exists() and snapshot_path.stat().st_size > 0:
            break
        log.warning("STILL waiting for manifest.snapshot.json | path=%s", snapshot_path)
        time.sleep(0.5)

    log.info("Loading manifest snapshot into builder")
    manifest: Dict[str, Any] = load_existing_manifest_snapshot(snapshot_path=snapshot_path)

    last_snapshot_ts = time.monotonic()
    events_since_snapshot = 0
    delta_log_bytes_at_snapshot = (
        delta_log_path.stat().st_size if delta_log_path.exists() else 0
    )

    # ---- bootstrap: apply all existing deltas once ----
    log.info("Applying existing manifest deltas (bootstrap)")
    delta_offset = 0
    if delta_log_path.exists():
        with delta_log_path.open("r", encoding="utf-8") as f:
            for line in f:
                delta = json.loads(line)
                apply_manifest_delta(manifest, delta)
            delta_offset = f.tell()
    log.info("Bootstrap complete | delta_offset=%d", delta_offset)

    # ---- main loop ----
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
                delta_log_bytes_at_snapshot = (
                    delta_log_path.stat().st_size if delta_log_path.exists() else 0
                )

            # Consume any fresh full snapshot the Fetcher dropped
            if snapshot_path.exists():
                dt = snapshot_created_at(snapshot_path)
                log.info("Full manifest snapshot detected | created_at=%s", dt)

                with snapshot_path.open("r", encoding="utf-8") as f:
                    manifest = json.load(f)

                write_manifest_snapshot(manifest_path=manifest_path, manifest=manifest)
                snapshot_path.unlink()  # consume

            time.sleep(0.25)

    except KeyboardInterrupt:
        log.info("Manifest builder shutting down; writing final snapshot")
        _write_snapshot(manifest, manifest_path)
        return

    except Exception:
        log.exception("Manifest builder crashed")
        raise


def _should_snapshot(
    *,
    last_snapshot_ts: float,
    events_since_snapshot: int,
    delta_log_path: Path,
    delta_log_bytes_at_snapshot: int,
) -> bool:
    now = time.monotonic()

    if events_since_snapshot >= MAX_EVENTS_PER_SNAPSHOT:
        return True

    if (now - last_snapshot_ts) >= SNAPSHOT_INTERVAL_SEC:
        return True

    if delta_log_path.exists():
        delta_bytes = delta_log_path.stat().st_size - delta_log_bytes_at_snapshot
        if delta_bytes >= MAX_DELTA_LOG_BYTES:
            return True

    return False


def _write_snapshot(manifest: Dict[str, Any], manifest_path: Path) -> None:
    manifest.setdefault("summary", {})["last_evaluated_at"] = (
        datetime.now(timezone.utc).isoformat()
    )
    write_manifest_snapshot(manifest_path=manifest_path, manifest=manifest)
    log.info("Manifest snapshot written | slices=%d", len(manifest.get("slices", {})))


def _setup_manifest_logging(log_dir: Path) -> None:
    logger = logging.getLogger("qlir.ibkr.manifest_builder")
    log_path = log_dir / "manifest_builder.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)

    handler = logging.FileHandler(log_path, encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
