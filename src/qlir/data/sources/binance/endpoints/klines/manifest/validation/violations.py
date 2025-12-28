from dataclasses import dataclass
from typing import Any

@dataclass
class ManifestViolation:
    slice_key: str | None
    rule: str
    message: str
    severity: str = "error"   # or "warn"
    extra: dict[str, Any] | None = None


def attach_violations(manifest: dict, violations: list[ManifestViolation]) -> None:
    for v in violations:
        entry = manifest["slices"].get(v.slice_key)
        if not entry:
            continue

        contract = entry.setdefault("__contract", {})
        missing = contract.setdefault("missing", [])
        missing.append(v.rule)

        if v.message:
            contract.setdefault("notes", []).append(v.message)

        contract["status"] = "out_of_sync"
