"""
Connectivity preflight for Interactive Brokers.

IBKR has no headless API-key auth: a running, logged-in IB Gateway/TWS must be reachable
over a socket. Rather than let the worker die on a raw ConnectionRefusedError, we run an
explicit preflight and log a bordered, actionable report of exactly what is present/missing.
"""

from __future__ import annotations

import importlib.util
import logging
import socket

log = logging.getLogger(__name__)


def is_ib_async_installed() -> bool:
    return importlib.util.find_spec("ib_async") is not None


def is_gateway_reachable(host: str, port: int, timeout: float = 3.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def run_preflight(*, host: str, port: int) -> bool:
    """
    Check every prerequisite and log a clear report. Returns True iff all pass.

    Never raises — the caller decides what to do with a False result (the worker aborts
    with a pointer to this report).
    """
    checks: list[tuple[str, bool, str]] = []

    ib_ok = is_ib_async_installed()
    checks.append((
        "ib_async installed",
        ib_ok,
        "run `poetry install` (ib-async is declared in pyproject.toml)",
    ))

    gw_ok = is_gateway_reachable(host, port)
    checks.append((
        f"IB Gateway/TWS reachable at {host}:{port}",
        gw_ok,
        "start IB Gateway or TWS and LOG IN, then enable the API "
        "(Settings -> API -> Enable ActiveX and Socket Clients). "
        "See src/qlir/servers/ibkr_data_server/README.md, or use the launch script.",
    ))

    all_ok = all(ok for _, ok, _ in checks)

    border = "=" * 64
    lines = ["", border, " IBKR PREFLIGHT", border]
    for name, ok, fix in checks:
        lines.append(f" [{'OK ' if ok else 'MISSING'}] {name}")
        if not ok:
            lines.append(f"          fix: {fix}")
    lines.append(border)
    if all_ok:
        lines.append(" All prerequisites satisfied — connecting.")
    else:
        lines.append(" Prerequisites MISSING — the data server cannot start (see above).")
    lines.append(border)
    report = "\n".join(lines)

    (log.info if all_ok else log.error)(report)
    return all_ok
