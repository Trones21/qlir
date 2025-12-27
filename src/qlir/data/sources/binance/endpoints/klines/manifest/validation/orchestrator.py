from pathlib import Path
from qlir.data.core.paths import get_raw_responses_dir_path
from qlir.data.sources.binance.endpoints.klines.manifest.validation.manifest_fs_integrity import validate_manifest_vs_responses
from qlir.data.sources.binance.endpoints.klines.manifest.validation.manifest_structure import do_all_slices_have_same_top_level_metadata
from qlir.data.sources.binance.endpoints.klines.manifest.validation.open_time_spacing import validate_slice_open_spacing_wrapper
from qlir.data.sources.binance.endpoints.klines.manifest.validation.report import ManifestValidationReport
from qlir.utils.str.color import Ansi, colorize
import logging 
log = logging.getLogger(__name__)


def validate_manifest_and_fs_integrity(manifest: dict, response_dir: Path) -> ManifestValidationReport:
    """
    High-level manifest validation entry point.

    All checks are warnings unless the manifest is fundamentally unusable.
    Intended to be called once per worker before processing begins.
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

    validate_slice_open_spacing_wrapper(manifest)
    
    # ─────────────────────────────────────────────
    # Manifest Structure (warnings - maybe i'll write a func to automatically fix the issues in the manifest at a later date)
    # ─────────────────────────────────────────────

    same_shape, structures = do_all_slices_have_same_top_level_metadata(slices)

    if not same_shape:
        log.warning(colorize(
            "Manifest contains slices with different top-level structures",
            Ansi.YELLOW,
            Ansi.BOLD,  
        ),extra={"tag": ("MANIFEST","VALIDATION","STRUCTURE") })

        for s in structures:
            slice_keys = s['slice_keys']
            preview = slice_keys[:2]
            suffix = " ..." if len(slice_keys) > 2 else ""

            log.warning({
                "slice_keys_count": colorize(str(len(slice_keys)),Ansi.BOLD) ,
                "slice_keys": f"{preview}{suffix}",
                
            },extra={"tag":("MANIFEST","VALIDATION","STRUCTURE","DETAILS")})
            log.warning({"structure": s['keys']})
            report.warnings.append(
                { "violation": "Manifest contains slices with different top-level structures",
                "details":{
                "slice_keys_count": colorize(str(len(slice_keys)),Ansi.BOLD) ,
                "slice_keys": f"{preview}{suffix}",
                "structure": s['keys']                
                }
                }
            )

    # ─────────────────────────────────────────────
    # Manifest ↔ Filesystem Integrity (warnings - maybe i'll write a func to automatically fix the issues in the manifest/fs at a later date)
    # ─────────────────────────────────────────────

    fs_issues = validate_manifest_vs_responses(
        manifest=manifest,
        responses_dir=response_dir,
    )

    if fs_issues:
        log.warning(colorize(
            "Manifest / filesystem integrity issues detected",
            Ansi.YELLOW,
            Ansi.BOLD,
        ))
        log.warning(fs_issues, extra={"tag":("MANIFEST","VALIDATION","MANIFEST_FS_TEGRIDY","DETAILS")})
        report.warnings.append({"violation": "Manifest / filesystem integrity issues detected", 
                                "details": fs_issues})
    # ─────────────────────────────────────────────
    # Summary
    # ─────────────────────────────────────────────

    log.info(colorize(
        "Manifest validation completed (warnings may exist)",
        Ansi.CYAN,
        Ansi.BOLD,
    )) 

    return report
