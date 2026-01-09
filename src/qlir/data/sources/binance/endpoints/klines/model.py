from __future__ import annotations

# This file used to contain SliceKey, SliceStatus, SliceClassification before they were refactored out to common.slices module 

# The meta fields we expect fetch_and_persist_slice to 
# we dont want a class because we dont want to break the system when changes are made 
# We just want to add an annotation using apply_contract() - basically this tells us that the object is usable, but it was produced under an older contract.
# The system can then decided how to handle this 
REQUIRED_FIELDS = [
    "slice_id",
    "relative_path",
    "slice_status",
    "http_status",
    "n_items",
    "first_ts",
    "last_ts",
    "url",
    "requested_at",
    "completed_at",
]
