from pathlib import Path
import logging
log = logging.getLogger(__name__)

def validate_manifest_vs_responses(
    manifest: dict,
    responses_dir: Path,
) -> dict:
    """
    Validate referential integrity between manifest entries and response files.

    Returns a dict of issues (empty means OK).
    """
    slices = manifest["slices"]

    manifest_paths = set()
    for entry in slices.values():
        manifest_paths.add(entry["relative_path"])

    
    filesystem_paths = {
        str("responses"/p.relative_to(responses_dir))
        for p in responses_dir.glob("*.json")
    }

    log.debug(f"Found {len(manifest_paths)} relative path entries in manifest")
    log.debug(f"Found {len(filesystem_paths)} json files in {responses_dir}")

    issues = {}

    if len(manifest_paths) != len(filesystem_paths):
        issues["count_mismatch"] = {
            "manifest": len(manifest_paths),
            "filesystem": len(filesystem_paths),
        }

    missing_files = manifest_paths - filesystem_paths
    if missing_files:
        issues["missing_response_files"] = sorted(missing_files)

    orphan_files = filesystem_paths - manifest_paths
    if orphan_files:
        issues["orphan_response_files"] = sorted(orphan_files)

    log.debug(issues)
    return issues
