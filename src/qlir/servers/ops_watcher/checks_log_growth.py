from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any

from .config import LogGrowthCheckConfig


def _file_size(path: Path) -> int | None:
    try:
        return path.stat().st_size
    except FileNotFoundError:
        return None


def eval_log_growth_check(
    cfg: LogGrowthCheckConfig,
    *,
    baseline_ts: float,
    baseline_size: int,
) -> tuple[bool, dict[str, Any], float, int]:
    """
    Returns:
      ok, payload, new_baseline_ts, new_baseline_size
    """
    now = time.time()
    size = _file_size(cfg.path)

    if size is None:
        payload = {
            "type": "log_missing",
            "check_kind": "log_growth",
            "name": cfg.name,
            "severity": cfg.severity,
            "path": str(cfg.path),
        }
        if cfg.note:
            payload["note"] = cfg.note
        # keep baseline; missing is a failure
        return False, payload, baseline_ts, baseline_size

    # Initialize baseline if needed
    if baseline_ts <= 0:
        baseline_ts = now
        baseline_size = size
        payload = {
            "type": "log_baseline_set",
            "check_kind": "log_growth",
            "name": cfg.name,
            "severity": "info",
            "path": str(cfg.path),
            "baseline_size": baseline_size,
        }
        return True, payload, baseline_ts, baseline_size

    window_s = cfg.window_hours * 3600
    age = now - baseline_ts

    # If window not yet complete, don't fail; just keep waiting.
    if age < window_s:
        payload = {
            "type": "log_window_incomplete",
            "check_kind": "log_growth",
            "name": cfg.name,
            "severity": "info",
            "path": str(cfg.path),
            "baseline_age_s": age,
            "window_s": window_s,
        }
        return True, payload, baseline_ts, baseline_size

    growth = size - baseline_size
    ok = growth >= cfg.min_growth_bytes

    payload = {
        "type": "log_stall" if not ok else "log_growth_ok",
        "check_kind": "log_growth",
        "name": cfg.name,
        "severity": cfg.severity if not ok else "info",
        "path": str(cfg.path),
        "window_hours": cfg.window_hours,
        "min_growth_bytes": cfg.min_growth_bytes,
        "baseline_size": baseline_size,
        "current_size": size,
        "growth_bytes": growth,
    }
    if cfg.note:
        payload["note"] = cfg.note

    # Roll the baseline forward every completed window
    return ok, payload, now, size
