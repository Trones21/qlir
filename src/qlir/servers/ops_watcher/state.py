from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class CheckState:
    # "ok" | "fail"
    status: str = "ok"
    last_emit_ts: float = 0.0

    # For log growth checks: track a window baseline
    baseline_ts: float = 0.0
    baseline_size: int = 0


@dataclass
class WatcherState:
    version: int = 1
    checks: dict[str, CheckState] = field(default_factory=dict)


def _check_key(kind: str, name: str) -> str:
    return f"{kind}:{name}"


def get_check_state(state: WatcherState, kind: str, name: str) -> CheckState:
    key = _check_key(kind, name)
    if key not in state.checks:
        state.checks[key] = CheckState()
    return state.checks[key]


def load_state(path: Path) -> WatcherState:
    if not path.exists():
        return WatcherState()

    raw = json.loads(path.read_text(encoding="utf-8"))
    st = WatcherState(version=int(raw.get("version", 1)))
    checks_raw: dict[str, Any] = raw.get("checks", {})
    for k, v in checks_raw.items():
        st.checks[k] = CheckState(
            status=str(v.get("status", "ok")),
            last_emit_ts=float(v.get("last_emit_ts", 0.0)),
            baseline_ts=float(v.get("baseline_ts", 0.0)),
            baseline_size=int(v.get("baseline_size", 0)),
        )
    return st


def save_state(path: Path, state: WatcherState) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = {
        "version": state.version,
        "saved_ts": time.time(),
        "checks": {
            k: {
                "status": v.status,
                "last_emit_ts": v.last_emit_ts,
                "baseline_ts": v.baseline_ts,
                "baseline_size": v.baseline_size,
            }
            for k, v in state.checks.items()
        },
    }
    path.write_text(json.dumps(raw, indent=2, sort_keys=True), encoding="utf-8")
