from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Dict, Any, Iterable
from datetime import datetime, timezone

log = logging.getLogger("qlir.manifest_delta_log")

# ---------------------------------------------------------------------------
# Delta log format
# ---------------------------------------------------------------------------
#
# Each line is a JSON object describing a manifest update.
# This is NOT a historical journal.
# It is a durable queue of unapplied manifest deltas.
#
# Ground truth = responses/*.json
# manifest.json = cached index
# delta log = pending index updates
#
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Writing deltas (called by workers)
# ---------------------------------------------------------------------------

def append_manifest_delta(
    delta_log_path: Path,
    delta: Dict[str, Any],
) -> None:
    """
    Append a manifest delta to the delta log.

    Contract:
    - Response artifact MUST already exist on disk
    - Delta describes metadata derived from that artifact
    - Once this returns, the delta is guaranteed durable
    """
    delta = dict(delta)
    delta.setdefault("ts", _now_iso())

    delta_log_path.parent.mkdir(parents=True, exist_ok=True)

    with delta_log_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(delta, sort_keys=True))
        f.write("\n")
        f.flush()


# ---------------------------------------------------------------------------
# Reading deltas (called by aggregator)
# ---------------------------------------------------------------------------

def iter_manifest_deltas(delta_log_path: Path) -> Iterable[Dict[str, Any]]:
    """
    Iterate all manifest deltas currently present in the delta log.

    Notes:
    - This intentionally does NOT track offsets yet
    - Deltas are expected to be idempotent / overwrite-style
    """
    if not delta_log_path.exists():
        return []

    def _iter():
        with delta_log_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    yield json.loads(line)
                except json.JSONDecodeError:
                    log.exception("Failed to decode manifest delta line")

    return _iter()


# ---------------------------------------------------------------------------
# Applying deltas
# ---------------------------------------------------------------------------

def apply_manifest_delta(
    manifest: Dict[str, Any],
    delta: Dict[str, Any],
) -> None:
    """
    Apply a single manifest delta to the in-memory manifest.

    This mutates `manifest` in-place.

    Expected delta shape (example):
    {
        "slice_comp_key": "...",
        "slice_id": "...",
        "slice_status": "complete",
        "relative_path": "responses/abc.json",
        "http_status": 200,
        "n_items": 1000,
        "requested_at": "...",
        "completed_at": "...",
    }
    """
    slice_key = delta.get("slice_comp_key")
    if slice_key is None:
        log.warning("Manifest delta missing slice_comp_key: %s", delta)
        return

    slices = manifest.setdefault("slices", {})
    entry = slices.get(slice_key, {})

    # Overwrite-style merge
    for k, v in delta.items():
        if k == "slice_comp_key":
            continue
        entry[k] = v

    slices[slice_key] = entry


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
