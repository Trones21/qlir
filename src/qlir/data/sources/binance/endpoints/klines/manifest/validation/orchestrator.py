import logging
from pathlib import Path

from qlir.core.types.named_df import NamedDF
from qlir.data.sources.binance.endpoints.klines.manifest.validation.manifest_fs_integrity import (
    validate_manifest_vs_responses,
)
from qlir.data.sources.binance.endpoints.klines.manifest.validation.manifest_structure import (
    do_all_slices_have_same_top_level_metadata,
)
from qlir.data.sources.binance.endpoints.klines.manifest.validation.open_time_spacing import (
    validate_slice_open_spacing_wrapper,
)
from qlir.data.sources.binance.endpoints.klines.manifest.validation.report import (
    ManifestValidationReport,
)
from qlir.data.sources.binance.endpoints.klines.manifest.validation.validate_slice_invariants import (
    validate_slice_invariants,
)
from qlir.data.sources.binance.endpoints.klines.manifest.validation.violations import violations_df
from qlir.data.sources.common.slices.slice_status import SliceStatus
from qlir.logging.logdf import logdf
from qlir.utils.str.color import Ansi, colorize

log = logging.getLogger(__name__)


def validate_manifest_and_fs_integrity(manifest: dict, response_dir: Path) -> ManifestValidationReport:
    """
    High-level manifest validation entry point.

    All checks are warnings unless the manifest is fundamentally unusable.
    Intended to be called once per worker before processing begins.

    Manifest dict should include both the info read from manifest.json as well as manifest.delta 
    """
    report = ManifestValidationReport()
    log.info(colorize(
        "Running manifest validation",
        Ansi.BOLD,
        Ansi.CYAN,
    ), extra={"tag": ("MANIFEST","VALIDATION")})

    # ─────────────────────────────────────────────
    # Fatal preconditions
    # ─────────────────────────────────────────────

    if "slices" not in manifest:
        report.fatal.append(
            "Manifest missing required 'slices' key"
        )
        return report

    slices = manifest["slices"]
    if not slices:
        log.warning(colorize(
            "Manifest contains zero slices",
            Ansi.YELLOW,
            Ansi.BOLD,
        ))
        report.warnings.append({
        "violation": "empty_manifest",
        "details": "Manifest contains zero slices",
        })
        return report

    # ─────────────────────────────────────────────
    # Relative Gaps (warnings - maybe i'll write a func to automatically fix the issues in the manifest at a later date)
    # ─────────────────────────────────────────────

    slice_parse_violations, open_spacing_violations = validate_slice_open_spacing_wrapper(manifest)
    report.add_slice_parse_violations(slice_parse_violations)
    report.add_open_spacing_violations(open_spacing_violations)

    # ─────────────────────────────────────────────
    # Manifest Structure (warnings - maybe i'll write a func to automatically fix the issues in the manifest at a later date)
    # ─────────────────────────────────────────────

    same_shape, structures = do_all_slices_have_same_top_level_metadata(slices)
    report.record_and_log_structure_validation(same_shape=same_shape, structures=structures)

    # ─────────────────────────────────────────────
    # Manifest ↔ Filesystem Integrity (warnings - maybe i'll write a func to automatically fix the issues in the manifest/fs at a later date)
    # ─────────────────────────────────────────────

    fs_issues = validate_manifest_vs_responses(
        manifest=manifest,
        responses_dir=response_dir,
    )
    report.record_and_log_manifest_vs_responses(fs_issues=fs_issues)


    # ─────────────────────────────────────────────
    # Slice Facts (warnings - maybe i'll write a func to automatically fix the issues in the manifest/fs at a later date)
    # ─────────────────────────────────────────────

    slice_invariant_violations = []
    for slice_key, slice_obj in manifest["slices"].items():
        
        if SliceStatus.try_parse(slice_obj['slice_status']) == SliceStatus.MISSING.value:
            # Slice exists structurally but has not yet been requested.
            # Invariants depending on requested_url are not meaningful yet.
            continue

        inv_violations = validate_slice_invariants(
            slice_key=slice_key,
            slice_obj=slice_obj,
            manifest=manifest,
        )
        slice_invariant_violations.extend(inv_violations)
        report.add_slice_invariant_violations(inv_violations)



    if not slice_invariant_violations:
         log.info(
                colorize(
                    "Slice invariant validation passed (all slices satisfy all invariants)",
                    Ansi.DIM,
                ),
                extra={"tag": ("MANIFEST", "VALIDATION", "STRUCTURE")},
            )
    else:

        viol_df = violations_df(slice_invariant_violations)
        counts_df = (
        viol_df.groupby(["rule", "exception"], dropna=False)
            .size()
            .rename("violation_count")
            .reset_index()
            .sort_values("violation_count", ascending=False)
        )
        logdf(NamedDF(counts_df, "Count by rule"),  max_rows=999)

    # ─────────────────────────────────────────────
    # Summary
    # ─────────────────────────────────────────────

    log.info(colorize(
        "Manifest validation completed (warnings may exist)",
        Ansi.CYAN,
        Ansi.BOLD,
    )) 

    return report



        