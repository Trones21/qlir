
from pathlib import Path

from qlir.data.core.paths import expected_slice_response_uri
from qlir.data.sources.binance.endpoints.klines.manifest.read_metadata import read_response_metadata
from qlir.data.sources.binance.endpoints.klines.manifest.summary import update_summary
from qlir.data.sources.common.slices.canonical_hash import make_canonical_slice_hash
from qlir.data.sources.common.slices.slice_key import SliceKey
from qlir.data.sources.common.slices.slice_status import SliceStatus
from qlir.telemetry.telemetry import telemetry
from qlir.time.iso import now_utc
import logging
log = logging.getLogger(__name__)



@telemetry(log_path=Path("telemetry/manifest_rebuild.log"), console=True)
def rebuild_manifest_from_responses(
    *,
    responses_dir: Path,
    expected_slices: list[SliceKey],
    symbol: str,
    interval: str,
    limit: int,
) -> dict:
    log.warning(
        "Rebuilding manifest from filesystem responses (this may take time)",
        extra={"tag": ("MANIFEST", "REBUILD")},
    )

    manifest = {
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
            "last_evaluated_at": now_utc().isoformat(),
        },
        "slices": {},
    }

    for slice_key in expected_slices:
        comp_key = slice_key.canonical_slice_composite_key()
        slice_id = make_canonical_slice_hash(slice_key)

        entry = {
            "slice_id": slice_id,
            "slice_status": SliceStatus.MISSING,
            "slice_status_reason": None,
            "first_ts": slice_key.start_ms,
            "last_ts": slice_key.end_ms,
            "requested_at": None,
            "completed_at": None,
            "relative_path": None,
            "n_items": None,
            "http_status": None,
            "error": None,
        }

        path = expected_slice_response_uri(responses_dir, slice_key)

        if not path.exists():
            manifest["slices"][comp_key] = entry
            continue

        entry["relative_path"] = str(path.relative_to(responses_dir.parent))

        # Zero-byte file → failed
        if path.stat().st_size == 0:
            entry["slice_status"] = SliceStatus.FAILED
            entry["slice_status_reason"] = "empty_response_file"
            manifest["slices"][comp_key] = entry
            continue

        meta = read_response_metadata(path)
        
        if meta is None:
            entry.update({"slice_status": SliceStatus.FAILED})
            manifest["slices"][comp_key] = entry
            continue

        status = SliceStatus.try_parse(meta.get("slice_status"))
        if status is None:
            status = SliceStatus.NEEDS_REFRESH
            log.info(f"Slice marked as needs refresh. Status is {status}")
        
        entry.update({
            "slice_status": status,
            
            "slice_status_reason": meta.get("slice_status_reason"),
            "n_items": meta.get("n_items"),
            "http_status": meta.get("http_status"),
            "requested_url": meta.get("url"),
            "requested_at": meta.get("requested_at"),
            "completed_at": meta.get("completed_at"),
        })

        manifest["slices"][comp_key] = entry

    update_summary(manifest)

    log.info(
        "Manifest rebuild complete",
        extra={
            "tag": ("MANIFEST", "REBUILD"),
            "total": manifest["summary"]["total_slices"],
            "complete": manifest["summary"]["complete_slices"],
            "missing": manifest["summary"]["missing_slices"],
        },
    )

    return manifest
