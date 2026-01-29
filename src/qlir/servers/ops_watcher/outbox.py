from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any


def _iso_utc(ts: float) -> str:
    # simple ISO-ish UTC; good enough for ops events
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts))


def emit_event(outbox_dir: Path, *, event: dict[str, Any]) -> Path:
    outbox_dir.mkdir(parents=True, exist_ok=True)

    ts = time.time()
    event = dict(event)
    event.setdefault("ts", ts)
    event.setdefault("ts_utc", _iso_utc(ts))

    # Unique filename, atomic write
    fname = f"{int(ts)}-{os.getpid()}-{event.get('type','event')}.json"
    tmp = outbox_dir / (fname + ".tmp")
    final = outbox_dir / fname

    tmp.write_text(json.dumps(event, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(final)
    return final
