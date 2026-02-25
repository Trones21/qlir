import json
import os
import time
from pathlib import Path
from datetime import datetime, date
from typing import Any


RUNTIME_STATE: dict[str, Any] = {}


def update_runtime_state(path: str, value: Any) -> None:
    cur: dict[str, Any] = RUNTIME_STATE
    parts = path.split(".")
    for p in parts[:-1]:
        nxt = cur.get(p)
        if not isinstance(nxt, dict):
            nxt = {}
            cur[p] = nxt
        cur = nxt
    cur[parts[-1]] = value


def runtime_state_get(obj_path: str, default=None):
    cur = RUNTIME_STATE
    for part in obj_path.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return default
        cur = cur[part]
    return cur


def _json_default(o: Any) -> Any:
    if isinstance(o, (datetime, date)):
        return o.isoformat()
    if isinstance(o, Path):
        return str(o)
    raise TypeError(f"Not JSON serializable: {type(o).__name__}")


def write_json_atomic(obj: Any, path: str | Path, *, indent: int = 2) -> Path:
    path = Path(path).expanduser()
    path.parent.mkdir(parents=True, exist_ok=True)

    tmp = path.with_name(path.name + ".tmp")
    payload = json.dumps(obj, indent=indent, sort_keys=True, default=_json_default)

    with open(tmp, "w", encoding="utf-8") as f:
        f.write(payload)
        f.flush()
        os.fsync(f.fileno())

    os.replace(tmp, path)

    # best-effort directory fsync (durability)
    try:
        dir_fd = os.open(str(path.parent), os.O_DIRECTORY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
    except Exception:
        pass

    return path


def end_loop_and_sleep(*, state_path: str | Path, sleep_sec: int) -> None:
    # Never let state writing kill the server.
    try:
        write_json_atomic(RUNTIME_STATE, state_path)
    except Exception as e:
        # Store the failure in-state; keep going.
        update_runtime_state("state_write.error", repr(e))
        update_runtime_state("state_write.error_at", datetime.utcnow().isoformat())
    time.sleep(sleep_sec)