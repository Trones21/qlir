
from urllib.parse import parse_qs, urlparse
from qlir.data.core.paths import get_raw_responses_dir_path
from qlir.data.sources.binance.endpoints.klines.manifest.validation.manifest_fs_integrity import validate_manifest_vs_responses
from qlir.utils.str.color import Ansi, colorize
import logging 
log = logging.getLogger(__name__)
from collections import OrderedDict, defaultdict
from typing import Any, Dict, Iterable, List


def do_all_slices_have_same_top_level_metadata(slices_dict) -> tuple[bool, list]:
    structures = get_distinct_top_level_metadata_structures_and_group_by_key([slices_dict])
    if len(structures) > 1:
        return False, structures
    else:
        return True, structures


def get_distinct_top_level_metadata_structures_and_group_by_key(
    slices_dicts: Iterable[Dict[str, Dict[str, Any]]],
) -> List[Dict[str, Any]]:
    """
    Identify distinct top-level metadata structures across slice entries.

    Each slice entry is grouped by its top-level key set (structure),
    ignoring values.

    Returns:
        [
            {
                "shape": {key: type, ...},
                "keys": [key, ...],
                "slice_keys": [slice_composite_key, ...],
            },
            ...
        ]
    """
    groups: dict[frozenset[str], list[tuple[str, Dict[str, Any]]]] = defaultdict(list)

    for slice_dict in slices_dicts:
        for slice_key, entry in slice_dict.items():
            if not isinstance(entry, dict):
                raise TypeError(
                    f"Slice entry for {slice_key} must be dict, got {type(entry)}"
                )

            key_signature = frozenset(entry.keys())
            groups[key_signature].append((slice_key, entry))

    results: list[dict[str, Any]] = []

    for key_sig, items in groups.items():
        _, sample_entry = items[0]

        shape = {k: type(v) for k, v in sample_entry.items()}

        results.append(
            {
                "shape": shape,
                "keys": sorted(key_sig),
                "slice_keys": [slice_key for slice_key, _ in items],
            }
        )

    return results
