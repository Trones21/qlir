from __future__ import annotations

from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, Iterable

log = logging.getLogger("qlir.ibkr.manifest_delta_log")

# ---------------------------------------------------------------------------
# Delta log format (identical contract to the Binance source)
#
# Each line is a JSON object describing a manifest update. It is NOT a historical
# journal — it is a durable queue of unapplied manifest deltas.
#
# Ground truth = responses/*.json ; manifest.json = cached index ; delta log = pending updates
# ---------------------------------------------------------------------------


def append_manifest_delta(delta_log_path: Path, delta: Dict[str, Any]) -> None:
    """Append a manifest delta to the delta log (durable on return)."""
    delta = dict(delta)
    delta.setdefault("ts", _now_iso())

    delta_log_path.parent.mkdir(parents=True, exist_ok=True)

    with delta_log_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(delta, sort_keys=True))
        f.write("\n")
        f.flush()
        os.fsync(f.fileno())


def rewrite_manifest_json(manifest: dict, path: Path) -> None:
    tmp = path.with_suffix(".full.tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, sort_keys=True)
        f.flush()
        os.fsync(f.fileno())
    tmp.replace(path)  # atomic on POSIX


def iter_manifest_deltas(delta_log_path: Path) -> Iterable[Dict[str, Any]]:
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


def apply_manifest_delta(manifest: Dict[str, Any], delta: Dict[str, Any]) -> None:
    """Apply a single manifest delta in-place (overwrite-merge, keyed by slice_comp_key)."""
    slice_key = delta.get("slice_comp_key")
    if slice_key is None:
        log.warning("Manifest delta missing slice_comp_key: %s", delta)
        return

    slices = manifest.setdefault("slices", {})
    entry = slices.get(slice_key, {})

    for k, v in delta.items():
        if k == "slice_comp_key":
            continue
        entry[k] = v

    slices[slice_key] = entry


def append_delta_log_to_in_memory_manifest(delta_log_path: Path, manifest):
    if delta_log_path.exists():
        with delta_log_path.open("r", encoding="utf-8") as f:
            for line in f:
                delta = json.loads(line)
                apply_manifest_delta(manifest, delta)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
