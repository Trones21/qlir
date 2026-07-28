import logging
from pathlib import Path

from qlir.data.sources.interactive_brokers.endpoints.historical_bars.manifest.validation.report import (
    ManifestValidationReport,
)

log = logging.getLogger(__name__)


def validate_manifest_and_fs_integrity(manifest: dict, response_dir: Path) -> ManifestValidationReport:
    """
    Lightweight manifest validation for IBKR.

    All checks are warnings unless the manifest is fundamentally unusable. Deliberately
    lighter than the Binance validator: IBKR has no request URL to parse, and equities have
    legitimate open-time gaps (nights/weekends/holidays), so URL-spacing and contiguity
    invariants don't apply. We check structure + manifest/filesystem integrity.
    """
    report = ManifestValidationReport()

    if "slices" not in manifest:
        report.fatal.append("Manifest missing required 'slices' key")
        return report

    slices = manifest["slices"]
    if not slices:
        report.warnings.append({"violation": "empty_manifest", "details": "zero slices"})
        return report

    # Manifest <-> filesystem integrity: any entry that claims a response file must have one.
    missing_files: list[str] = []
    for comp_key, entry in slices.items():
        rel = entry.get("relative_path")
        if not rel:
            continue
        path = response_dir.parent / rel
        if not path.exists():
            missing_files.append(comp_key)

    if missing_files:
        report.warnings.append(
            {
                "violation": "manifest_entries_missing_response_files",
                "count": len(missing_files),
                "examples": missing_files[:5],
            }
        )
        log.warning(
            "Manifest/filesystem integrity: %d entries reference a missing response file "
            "(examples: %s)",
            len(missing_files),
            missing_files[:5],
        )

    return report
