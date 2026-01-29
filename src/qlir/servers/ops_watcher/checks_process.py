from __future__ import annotations

from dataclasses import asdict
from typing import Any

import psutil

from .config import ProcessCheckConfig, SelfCheckConfig


def _iter_proc_cmdlines() -> list[str]:
    out: list[str] = []
    for p in psutil.process_iter(attrs=["cmdline"]):
        cmd = p.info.get("cmdline") or []
        if not cmd:
            continue
        out.append(" ".join(cmd))
    return out


def count_matching_proc_cmdline(selector: str) -> int:
    # substring match, conservative
    cnt = 0
    for cmdline in _iter_proc_cmdlines():
        if selector in cmdline:
            cnt += 1
    return cnt


def eval_process_check(cfg: ProcessCheckConfig) -> tuple[bool, dict[str, Any]]:
    cnt = count_matching_proc_cmdline(cfg.proc_cmdline_contains)
    ok = cnt >= 1
    payload: dict[str, Any] = {
        "type": "process_missing" if not ok else "process_ok",
        "check_kind": "process",
        "name": cfg.name,
        "severity": cfg.severity if not ok else "info",
        "proc_cmdline_contains": cfg.proc_cmdline_contains,
        "detected_count": cnt,
    }
    if cfg.note:
        payload["note"] = cfg.note
    return ok, payload


def eval_self_check(cfg: SelfCheckConfig) -> tuple[bool, dict[str, Any]]:
    cnt = count_matching_proc_cmdline(cfg.proc_cmdline_contains)

    # For "self", missing should never happen while we're running,
    # but duplicates can happen (e.g., launched twice).
    ok = cnt >= 1 and cnt <= 1

    if cnt == 0:
        etype = "self_missing"
        severity = "critical"
    elif cnt > 1:
        etype = "self_duplicate"
        severity = cfg.severity
    else:
        etype = "self_ok"
        severity = "info"

    payload: dict[str, Any] = {
        "type": etype,
        "check_kind": "self",
        "severity": severity,
        "proc_cmdline_contains": cfg.proc_cmdline_contains,
        "detected_count": cnt,
    }
    return ok, payload
