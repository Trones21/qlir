import json
from pathlib import Path


def read_response_metadata(path: Path) -> dict | None:
    """Read and return the `meta` section from a persisted response file (None if bad)."""
    try:
        with path.open("r", encoding="utf-8") as f:
            obj = json.load(f)
    except Exception:
        return None

    meta = obj.get("meta")
    if not isinstance(meta, dict):
        return None
    return meta
