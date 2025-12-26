from pathlib import Path
from qlir.data.core.paths import get_raw_responses_dir_path
from qlir.data.sources.binance.endpoints.klines.manifest.validation.manifest_fs_integrity import validate_manifest_vs_responses
from qlir.data.sources.binance.endpoints.klines.manifest.validation.manifest_structure import do_all_slices_have_same_top_level_metadata
from qlir.utils.str.color import Ansi, colorize
import logging 
log = logging.getLogger(__name__)


def validate_manifest_and_fs_integrity(manifest: dict, response_dir: Path) -> None:
    """
    High-level manifest validation entry point.

    All checks are warnings unless the manifest is fundamentally unusable.
    Intended to be called once per worker before processing begins.
    """

    log.info(colorize(
        "Running manifest validation",
        Ansi.BOLD,
        Ansi.CYAN,
    ))

    # ─────────────────────────────────────────────
    # Fatal preconditions
    # ─────────────────────────────────────────────

    if "slices" not in manifest:
        raise RuntimeError(
            "Manifest is missing required 'slices' key — cannot evaluate state"
        )

    slices = manifest["slices"]
    if not slices:
        log.warning(colorize(
            "Manifest contains zero slices",
            Ansi.YELLOW,
            Ansi.BOLD,
        ))
        return  # Nothing to do anyway

    # ─────────────────────────────────────────────
    # Manifest Structure (warnings)
    # ─────────────────────────────────────────────

    same_shape, structures = do_all_slices_have_same_top_level_metadata(slices)

    if not same_shape:
        log.warning(colorize(
            "Manifest contains slices with different top-level structures",
            Ansi.YELLOW,
            Ansi.BOLD,
        ),extra={"tag": "ABABABBA"})

        for s in structures:
            slice_keys = s['slice_keys']
            preview = slice_keys[:2]
            suffix = " ..." if len(slice_keys) > 2 else ""

            log.warning({
                "slice_keys_count": colorize(str(len(slice_keys)),Ansi.BOLD) ,
                "slice_keys": f"{preview}{suffix}",
                
            },extra={"tag": "PARTIAL"})
            log.warning({"structure": s['keys']})

    # ─────────────────────────────────────────────
    # Manifest ↔ Filesystem Integrity (warnings)
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
        log.warning(fs_issues)

    # ─────────────────────────────────────────────
    # Summary
    # ─────────────────────────────────────────────

    log.info(colorize(
        "Manifest validation completed (warnings may exist)",
        Ansi.MAGENTA,
        Ansi.BOLD,
    ))
