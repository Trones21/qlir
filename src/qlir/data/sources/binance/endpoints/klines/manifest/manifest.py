from datetime import datetime, timezone
import json
import os
from pathlib import Path
import time
from typing import Dict
import logging

from qlir.data.sources.common.slices.canonical_hash import make_canonical_slice_hash
from qlir.data.sources.common.slices.manifest_serializer import deserialize_manifest
from qlir.data.sources.common.slices.slice_classification import SliceClassification
from qlir.data.sources.common.slices.slice_key import SliceKey
from qlir.data.sources.common.slices.slice_status import SliceStatus
from qlir.time.iso import now_iso 
log = logging.getLogger(__name__)

from qlir.utils.str.color import Ansi, colorize



MANIFEST_FILENAME = "manifest.json"

def load_or_create_manifest(
    manifest_path: Path,
    symbol: str,
    interval: str,
    limit: int,
) -> Dict:
    """
    Load manifest.json if present; otherwise create a fresh skeleton.
    """
    if manifest_path.exists():
        with manifest_path.open("r", encoding="utf-8") as f:
            manifest = json.load(f)
        
        deserialize_manifest(manifest)
        return manifest
    
    # Fresh skeleton
    log.info("Creating fresh manifest (in-memory object) because there was no manifest found at: %s", manifest_path)
    
    return {
        "endpoint": "klines",
        "symbol": symbol,
        "interval": interval,
        "limit": limit,
        "summary": {
            "total_slices": 0,
            "complete_slices": 0,
            "partial_slices": 0,
            "failed_slices": 0,
            "missing_slices": 0,
            "needs_refresh_slices": 0,
            "last_evaluated_at": None,
        },  
        "slices": {},
    }


def wait_for_load_manifest(manifest_path: Path) -> Dict:
    """
    Load an existing manifest from disk.

    Contract:
    - waits for the manifest to exist, then loads it 
    """
    log.info("Waiting for manifest.json to exist | path=%s", manifest_path)
    
    while True:
        if manifest_path.exists() and manifest_path.stat().st_size > 0:
                break
        log.warning("STILL waiting for manifest.json to exist | path=%s", manifest_path)
        time.sleep(2)

    with manifest_path.open("r", encoding="utf-8") as f:
        manifest = json.load(f)

    deserialize_manifest(manifest)
    return manifest


def load_existing_manifest_snapshot(snapshot_path: Path) -> Dict:
    """
    Load an existing manifest from disk.

    Contract:
    - manifest.json MUST already exist
    - Caller is responsible for waiting until it does
    """
    if not snapshot_path.exists():
        raise FileNotFoundError(f"Manifest snapshot not found: {snapshot_path}")

    with snapshot_path.open("r", encoding="utf-8") as f:
        manifest = json.load(f)

    deserialize_manifest(manifest)
    return manifest


def _write_manifest_atomic(manifest_path: Path, manifest: Dict) -> None:
    tmp_path = manifest_path.with_suffix(".tmp")
    with tmp_path.open("w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, sort_keys=True)
    tmp_path.replace(manifest_path)


def write_full_manifest_snapshot(
    snapshot_dir: Path,
    manifest: dict,
    reason: str
) -> None:
    """
    Write a snapshot of the current manifest state.

    Notes:
    - Called by the WORKER so that the manifest aggregator can pick up this snapshot
    - Manifest is a materialized view
    - May lag behind real-time slice updates
    """
    tmp = snapshot_dir / "manifest.snapshot.tmp"
    final = snapshot_dir / "manifest.snapshot.json"

    snapshot_dir.mkdir(parents=True, exist_ok=True)

    with tmp.open("w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, sort_keys=True)
        f.flush()
        os.fsync(f.fileno())

    tmp.replace(final)  # atomic publish

    if reason:
        log.info("Full Manifest Snapshot Taken, deltalog service will pickup from path=%s, reason=%s", final, reason)


def snapshot_created_at(path: Path) -> datetime:
    st = path.stat()
    return datetime.fromtimestamp(st.st_mtime, tz=timezone.utc)


def write_manifest_snapshot(
    manifest_path: Path,
    manifest: Dict,
) -> None:
    """
    Write a snapshot of the current manifest state.

    Notes:
    - Called by the manifest aggregator
    - Manifest is a materialized view
    - May lag behind real-time slice updates
    """
    _write_manifest_atomic(manifest_path, manifest)



def seed_manifest_with_expected_slices(manifest, expected_slices: list[SliceKey]):
    """
    Ensure every expected slice exists in manifest with at least a 'missing' status.
    """
    changed = False

    for s in expected_slices:
        composite_key = s.canonical_slice_composite_key()
        if composite_key not in manifest['slices']:
            slice_hash = make_canonical_slice_hash(s)
            manifest['slices'][composite_key] = {
                "slice_status": SliceStatus.MISSING,
                "slice_id": slice_hash,
                "first_ts": s.start_ms,
                "last_ts": s.end_ms,
                "requested_at": None,
                "completed_at": None,
                "relative_path": None,
                "n_items": None,
                "error": None,
                "http_status": None,
            }
            log.info(f"Expected slice not found in manifest. Adding {composite_key} - {slice_hash}")
            changed = True
    
    if changed == False:
        log.info("All expected slices found in manifest.")

    return changed


def update_manifest_with_classification(manifest, classified: SliceClassification):
    """
    Mutates manifest in-place using classification results.
    """
    # mark slice-level status    
    for slice_key in classified.missing:
        slice_comp_key = slice_key.canonical_slice_composite_key()
        manifest["slices"][slice_comp_key]["slice_status"] = SliceStatus.MISSING

    for slice_key in classified.partial:
        slice_comp_key = slice_key.canonical_slice_composite_key()
        manifest["slices"][slice_comp_key]["slice_status"] = SliceStatus.PARTIAL

    for slice_key in classified.needs_refresh:
        slice_comp_key = slice_key.canonical_slice_composite_key()
        manifest["slices"][slice_comp_key]["slice_status"] = SliceStatus.NEEDS_REFRESH

    for slice_key in classified.complete:
        slice_comp_key = slice_key.canonical_slice_composite_key()
        manifest["slices"][slice_comp_key]["slice_status"] = SliceStatus.COMPLETE

    for slice_key in classified.failed:
        slice_comp_key = slice_key.canonical_slice_composite_key()
        manifest["slices"][slice_comp_key]["slice_status"] = SliceStatus.FAILED

    # update summary block (very useful for debugging / dashboards)
    manifest["summary"] = {
        "missing": len(classified.missing),
        "partial": len(classified.partial),
        "needs_refresh": len(classified.needs_refresh),
        "complete": len(classified.complete),
        "last_updated": now_iso(),
    }

    return manifest

