from typing import Dict
from qlir.data.sources.common.slices.entry_serializer import serialize_entry
from qlir.data.sources.common.slices.slice_status import SliceStatus
from qlir.data.sources.common.slices.slice_status_reason import SliceStatusReason
from qlir.utils.enum import deserialize_enum


def serialize_manifest(manifest: dict) -> dict:
    out = dict(manifest)
    out["summary"] = dict(manifest["summary"])
    out["slices"] = {}

    for k, entry in manifest["slices"].items():
        out["slices"][k] = serialize_entry(entry)

    return out


def deserialize_manifest(manifest: Dict) -> None:
    for entry in manifest.get("slices", {}).values():
        entry["slice_status"] = deserialize_enum(
            SliceStatus,
            entry["slice_status"],
        )

        if entry.get("slice_status_reason") is not None:
            entry["slice_status_reason"] = deserialize_enum(
                SliceStatusReason,
                entry["slice_status_reason"],
            )
