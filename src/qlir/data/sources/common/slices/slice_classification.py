

from dataclasses import dataclass

from qlir.data.sources.common.slices.slice_key import SliceKey
from qlir.data.sources.common.slices.slice_status import SliceStatus
import logging
log = logging.getLogger(__name__)

@dataclass
class SliceClassification:
    missing: list[SliceKey]
    partial: list[SliceKey]
    needs_refresh: list[SliceKey]
    complete: list[SliceKey]
    failed: list[SliceKey]


def classify_slices(
    expected: list[SliceKey],
    manifest: dict,
) -> SliceClassification:

    slices = manifest.get("slices", {})
    result = SliceClassification([], [], [], [], [])

    for slice_key in expected:
        key = slice_key.canonical_slice_composite_key()
        entry = slices.get(key)

        if not entry:
            result.missing.append(slice_key)
            continue
        
        status = SliceStatus(entry.get("slice_status"))
        if status == SliceStatus.MISSING:
            result.missing.append(slice_key)
            continue

        # worker.add_or_update_entry_meta_contract adds these fields
        has_contract = "__meta_contract" in entry
        if not has_contract:
            log.debug(f"Metadata contract object in manifest for slice {key} is out of date, couldnt find __meta_contract field")
        
        has_status = has_contract and "status" in entry["__meta_contract"]
        if not has_status:
            log.debug(f"Metadata contract object in manifest for slice {key} is out of date, couldnt find __meta_contract.status field")
            
        is_out_of_sync = (
            not has_contract or
            not has_status or
            entry["__meta_contract"]["status"] == "out_of_sync"
        )
        if is_out_of_sync:
            log.debug(f"Metadata contract in manifest for slice {key} is out of date, setting SliceStatus to NEEDS_REFRESH")
            result.needs_refresh.append(slice_key)
            continue

        if status == SliceStatus.PARTIAL:
            result.partial.append(slice_key)
        elif status == SliceStatus.COMPLETE:
            result.complete.append(slice_key)
        elif status == SliceStatus.FAILED:
            result.failed.append(slice_key)

    log.info(
        {"Slice Classification Summary": 
            {
            k: len(v)
            for k, v in vars(result).items()
            }
        }
    )
    
    return result