from qlir.data.sources.common.slices.slice_status import SliceStatus
from qlir.data.sources.common.slices.slice_status_reason import SliceStatusReason
from qlir.utils.enum import deserialize_enum, serialize_enum


SLICE_ENTRY_ENUMS = {
    "slice_status": SliceStatus,
    "slice_status_reason": SliceStatusReason,
}


def serialize_entry(entry: dict) -> dict:
    out = dict(entry)
    for key, enum_cls in SLICE_ENTRY_ENUMS.items():
        if key in out:
            out[key] = serialize_enum(out[key])
    return out

def deserialize_entry(entry: dict) -> dict:
    out = dict(entry)
    for key, enum_cls in SLICE_ENTRY_ENUMS.items():
        if key in out and out[key] is not None:
            out[key] = deserialize_enum(enum_cls, out[key])
    return out
