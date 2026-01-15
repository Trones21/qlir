

from dataclasses import dataclass
import logging
import os
from typing import Iterable

from qlir.data.sources.common.slices.slice_key import (
    SliceKey,
    get_current_slice_key,
    slice_key_from_canonical,
)
from qlir.data.sources.common.slices.slice_status import SliceStatus

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

    slices: dict = manifest.get("slices", {})
    result = SliceClassification([], [], [], [], [])



    # 'SOLUSDT:1m:1768245600000:1000', 'SOLUSDT:1m:1768305600000:1000'


    current_slice, expected_iter = try_resolve_current_slice(
        expected=expected,
        slices=slices,
    )

    if current_slice is not None:
        result.partial.append(current_slice)


    for slice_key in expected_iter:
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
        if os.getenv("QLIR_REFRESH_ON_METADATA_SCHEMA_MISMATCH"):
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




def try_resolve_current_slice(
    *,
    expected: list[SliceKey],
    slices: dict,
) -> tuple[SliceKey | None, Iterable[SliceKey]]:
    """
    Returns:
      (current_slice: SliceKey | None,
       expected_without_current: Iterable[SliceKey])

    First we derive the expected current (open) slice deterministically from wall-clock time.
    This slice should already exist in the manifest. Absence indicates upstream code failure (since slices are deterministic by wall clock time)

    Then we remove it from the iterable if we found it (because the caller handles the current slice separately... yes I know this is coupled... but split for readability, testabiltiy etc.
    """
    try:
        if not slices:
            raise ValueError("No slices available")

        prior_key = next(reversed(slices))
        current_slice_key_str = get_current_slice_key(prior_key)

        if current_slice_key_str not in slices:
            raise ValueError(
                f"Current slice key {current_slice_key_str} not found"
            )

        current_slice = slice_key_from_canonical(
            current_slice_key_str
        )

        # Exclude it structurally from generic classification
        expected_without_current = (
            sk for sk in expected
            if sk.canonical_slice_composite_key()
            != current_slice_key_str
        )

        return current_slice, expected_without_current

    except Exception as exc:
        log.error(
            "Failed to resolve current slice; falling back",
            exc_info=exc,
        )
        return None, expected

