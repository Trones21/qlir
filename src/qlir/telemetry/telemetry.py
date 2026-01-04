# This is a first pass to just experiment with 
# see the chat with chatgpt with a full implementation (core is similr but that are some classes and "config" utils to make things easier)

# qlir/telemetry.py
from __future__ import annotations

import time
from datetime import datetime, timezone
from functools import wraps
from pathlib import Path
from typing import Callable, TypeVar

F = TypeVar("F", bound=Callable)

def telemetry(
    *,
    log_path: Path | None = None,
    console: bool = True,
):
    def decorator(fn: F) -> F:
        @wraps(fn)
        def wrapper(*args, **kwargs):
            start = time.perf_counter()
            try:
                return fn(*args, **kwargs)
            finally:
                elapsed = time.perf_counter() - start
                ts = datetime.now(timezone.utc).isoformat()

                line = f"{ts} | {fn.__name__} | {elapsed:.6f}s"

                if console:
                    print(f"⏱ {line}")

                if log_path:
                    log_path.parent.mkdir(parents=True, exist_ok=True)
                    with log_path.open("a") as f:
                        f.write(line + "\n")

        return wrapper  # type: ignore
    return decorator