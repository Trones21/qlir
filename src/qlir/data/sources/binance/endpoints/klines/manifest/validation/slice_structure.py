from dataclasses import dataclass
import logging
from urllib.parse import parse_qs, urlparse

log = logging.getLogger(__name__)
from qlir.data.sources.binance.endpoints.klines.manifest.validation.violations import (
    ManifestViolation,
)


def isolate_open_time_from_composite_key(
    composite_key: str,
) -> int | ManifestViolation:
    parts = composite_key.split(":")
    if len(parts) < 4:
        log.warning(
            f"Invalid composite key format: {composite_key}",
            extra={"tag": ("MANIFEST","VALIDATION","SLICE_KEY")},
        )
        return ManifestViolation(
                slice_key=composite_key,
                rule="composite_key_format_error",
                message=f"Invalid composite key format: {composite_key}",
            )

    try:
        return int(parts[2])
    except ValueError:
        log.warning(
            f"Invalid startTime in composite key: {composite_key}",
            extra={"tag": ("MANIFEST","VALIDATION","SLICE_KEY")},
        )
        return ManifestViolation(
                slice_key=composite_key,
                rule="composite_key_format_error",
                message=f"Invalid startTime in composite key: {composite_key}",
            )
        return None


# Note: I didnt add logging here, but the one above already had it... i still may add logging but this will proably go to 
# a specific logger like a slice validations logger or something (definitely write to a separate file)
def isolate_open_time_from_request_url(
    slice_key: str,
    slice_entry: dict,
) -> int | ManifestViolation:
    
    url = slice_entry.get("requested_url") or slice_entry.get("url")
    if not url:
        return ManifestViolation(
                slice_key=slice_key,
                rule="missing_requested_url_or_url",
                message="Slice entry missing 'requested_url'",
            )

    qs = parse_qs(urlparse(url).query)
    start = qs.get("startTime")
    if not start:
        return ManifestViolation(
                slice_key=slice_key,
                rule="missing_startTime",
                message="requested_url missing startTime param",
                extra={"url": url},
            )

    try:
        return int(start[0])
    except Exception:
        return ManifestViolation(
                slice_key=slice_key,
                rule="invalid_startTime",
                message="startTime not parseable as int (ms)",
                extra={"value": start[0]},
            )


@dataclass(frozen=True)
class SliceFacts:
    symbol: str
    interval: str
    limit: int
    start_time: int


    

