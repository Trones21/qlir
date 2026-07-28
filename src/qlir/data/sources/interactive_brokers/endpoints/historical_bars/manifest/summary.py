import logging
from typing import Dict

from qlir.data.sources.common.slices.slice_status import SliceStatus
from qlir.data.sources.interactive_brokers.manifest_delta_log import _now_iso

log = logging.getLogger(__name__)


def update_summary(manifest: Dict) -> None:
    """Recompute summary stats based on current slice entries."""
    slices = manifest.get("slices", {})
    total = len(slices)
    complete = sum(1 for e in slices.values() if e.get("slice_status") == SliceStatus.COMPLETE)
    partial = sum(1 for e in slices.values() if e.get("slice_status") == SliceStatus.PARTIAL)
    failed = sum(1 for e in slices.values() if e.get("slice_status") == SliceStatus.FAILED)
    missing = sum(1 for e in slices.values() if e.get("slice_status") == SliceStatus.MISSING)
    needs_refresh = sum(1 for e in slices.values() if e.get("slice_status") == SliceStatus.NEEDS_REFRESH)
    in_progress = sum(1 for e in slices.values() if e.get("slice_status") == SliceStatus.IN_PROGRESS)

    accounted_for = complete + partial + failed + missing + needs_refresh + in_progress
    if accounted_for != total:
        log.warning(
            "Manifest summary mismatch: total=%d accounted=%d", total, accounted_for
        )

    manifest["summary"] = {
        "total_slices": total,
        "missing_slices": missing,
        "complete_slices": complete,
        "partial_slices": partial,
        "needs_refresh": needs_refresh,
        "in_progress": in_progress,
        "failed_slices": failed,
        "last_evaluated_at": _now_iso(),
    }
    log.info("Manifest Summary: %s", manifest["summary"])
