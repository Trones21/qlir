
import logging

from qlir.core.types.named_df import NamedDF
from qlir.data.sources.binance.endpoints.klines.manifest.validation.open_time_spacing import OpenSpacingViolations, SliceParseViolations
from qlir.data.sources.binance.endpoints.klines.manifest.validation.violations import ManifestViolation
from qlir.utils.df_views.list import list_to_df
from qlir.logging.logdf import logdf
from qlir.utils.str.color import Ansi, colorize
log = logging.getLogger(__name__)

class ManifestValidationReport:
    def __init__(self):
        self.fatal: list[str] = []
        self.warnings: list[dict] = []
        self.violations: list[dict] = []

    def has_fatal(self) -> bool:
        return bool(self.fatal)
    

    def record_and_log_structure_validation(self, *, same_shape: bool, structures: list):
        if same_shape:
            log.info(
                colorize(
                    "Manifest structure validation passed (all slices consistent)",
                    Ansi.DIM,
                ),
                extra={"tag": ("MANIFEST", "VALIDATION", "STRUCTURE")},
            )
            
        if not same_shape:
            log.warning(colorize(
                "Manifest contains slices with different top-level structures",
                Ansi.YELLOW,
                Ansi.BOLD,  
            ),extra={"tag": ("MANIFEST","VALIDATION","STRUCTURE") })
            counts_by_shape = []
            
            for s in structures:
                slice_keys = s['slice_keys']
                preview = slice_keys[:2]
                suffix = " ..." if len(slice_keys) > 2 else ""

                log.warning({
                    "slice_keys_count": colorize(str(len(slice_keys)),Ansi.BOLD) ,
                    "slice_keys": f"{preview}{suffix}",
                    
                },extra={"tag":("MANIFEST","VALIDATION","STRUCTURE","DETAILS")})
                log.warning({"structure": s['keys']})
                
                violation = { "violation": "Manifest contains slices with different top-level structures",
                    "details":{
                    "slice_keys_count": len(slice_keys),
                    "slice_keys": f"{preview}{suffix}",
                    "structure": s['keys']                
                    }
                    }
                self.warnings.append(violation)
                counts_by_shape.append(violation['details'])

            df_to_log = list_to_df(rows=counts_by_shape, columns=["slice_keys_count", "structure"])
            logdf(NamedDF(df_to_log, name="Manifest Objs Counts by Structure"))

    def record_and_log_manifest_vs_responses(self, *, fs_issues):
        if not fs_issues:
            {}
        if fs_issues:
            log.warning(colorize(
                "Manifest / filesystem integrity issues detected",
                Ansi.YELLOW,
                Ansi.BOLD,
            ))
            log.warning(fs_issues, extra={"tag":("MANIFEST","VALIDATION","MANIFEST_FS_TEGRIDY","DETAILS")})
            self.warnings.append({"violation": "Manifest / filesystem integrity issues detected", 
                                    "details": fs_issues})

    def add_slice_invariant_violations(
        self,
        violations: list[ManifestViolation],
    ) -> None:
        for v in violations:
            # record
            self.violations.append({
                "type": "slice_invariant",
                "rule": v.rule,
                "slice_key": v.slice_key,
                "message": v.message,
                "extra": v.extra,
            })

            # log (one per violation, explicit & searchable)
            log.warning(
                colorize(
                    f"[{v.rule}] {v.message}",
                    Ansi.YELLOW,
                ),
                extra={
                    "tag": ("MANIFEST", "VALIDATION", "SLICE_INVARIANTS"),
                    "slice_key": v.slice_key,
                    **(v.extra or {}),
                },
            )  
    
    def add_slice_parse_violations(self, slice_parse_violations: SliceParseViolations):
        log.debug('add_slice_structure_violations: log as df NotImplemented,but violations appended to report')
        log.debug(slice_parse_violations)
        self.violations.append(slice_parse_violations)
    
    def add_open_spacing_violations(self, open_spacing_violations: OpenSpacingViolations):       
        log.debug('open_spacing_violations: log as df NotImplemented, but violations appended to report')
        log.debug(open_spacing_violations)
        self.violations.append(open_spacing_violations)