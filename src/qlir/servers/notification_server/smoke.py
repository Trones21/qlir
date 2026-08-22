"""
Verify notification sinks before trusting them with real alerts.

    poetry run notify_smoke --list             # what is configured
    poetry run notify_smoke --all              # test every routed sink
    poetry run notify_smoke --sink team_email  # test one sink
    poetry run notify_smoke --outbox qlir-ops  # test every sink on one outbox

Sends directly through the adapter, bypassing the outbox queue, so a failure
points at the transport rather than at the pipeline. Exits non-zero if any sink
fails, which makes it usable as a setup gate in a script.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import logging
import socket
import sys

from .adapters.registry import SinkConfigError, get_sink_type
from .config import load_config, read_declared_outboxes
from qlir.servers.alerts.paths import get_alerts_root

log = logging.getLogger("notify_smoke")


def _payload(sink_name: str) -> dict:
    return {
        "type": "smoke_test",
        "message": f"qlir notification smoke test for sink '{sink_name}'",
        "sink": sink_name,
        "host": socket.gethostname(),
        "ts": datetime.now(timezone.utc).isoformat(),
    }


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="notify_smoke",
        description="Send a test alert through configured notification sinks.",
    )
    g = p.add_mutually_exclusive_group()
    g.add_argument("--sink", help="Test a single sink by name.")
    g.add_argument("--outbox", help="Test every sink routed from this outbox.")
    g.add_argument("--all", action="store_true", help="Test every routed sink.")
    g.add_argument("--list", action="store_true", help="Show configuration and exit.")
    return p.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    args = _parse_args()

    try:
        declared = read_declared_outboxes(get_alerts_root())
    except RuntimeError:
        declared = {}  # QLIR_ALERTS_DIR unset; only matters for the fallback

    try:
        cfg = load_config(declared_outboxes=sorted(declared))
    except SinkConfigError as e:
        print(f"error: {e}", file=sys.stderr)
        raise SystemExit(2)

    print(f"config: {cfg.source}\n")

    if args.list:
        print("sinks:")
        for name, spec in sorted(cfg.sinks.items()):
            used = "routed" if name in cfg.referenced_sinks else "defined but UNUSED"
            print(f"  {name:20} type={spec.type:10} ({used})")
        print("\nroutes:")
        for outbox, sinks in sorted(cfg.routes.items()):
            print(f"  {outbox:26} -> {', '.join(sinks)}")
        return

    # decide which sinks to exercise
    if args.sink:
        if args.sink not in cfg.sinks:
            print(f"error: no sink named {args.sink!r}; defined: {', '.join(sorted(cfg.sinks))}",
                  file=sys.stderr)
            raise SystemExit(2)
        targets = [args.sink]
    elif args.outbox:
        if args.outbox not in cfg.routes:
            print(f"error: no route for outbox {args.outbox!r}; routed: "
                  f"{', '.join(sorted(cfg.routes))}", file=sys.stderr)
            raise SystemExit(2)
        targets = list(cfg.routes[args.outbox])
    else:
        targets = sorted(cfg.referenced_sinks)
        if not targets:
            print("error: no sinks are referenced by any route", file=sys.stderr)
            raise SystemExit(2)

    failures = 0
    for sink_name in targets:
        spec = cfg.sinks[sink_name]
        sink_type = get_sink_type(spec.type)
        print(f"--- {sink_name} (type: {spec.type})")

        try:
            adapter = sink_type.build(spec.options)
        except Exception as e:
            failures += 1
            print(f"    FAILED to build: {e}")
            _print_setup(sink_type)
            continue

        try:
            adapter.send(_payload(sink_name))
        except Exception as e:
            failures += 1
            print(f"    FAILED to send: {type(e).__name__}: {e}")
            _print_setup(sink_type)
            continue

        print("    OK -- sent. Confirm it actually arrived on the receiving end.")

    print()
    total = len(targets)
    if failures:
        print(f"{total - failures}/{total} sink(s) OK, {failures} failed.")
        raise SystemExit(1)
    print(f"{total}/{total} sink(s) OK.")


def _print_setup(sink_type) -> None:
    if not sink_type.setup_steps:
        return
    print("    setup:")
    for i, step in enumerate(sink_type.setup_steps, 1):
        print(f"      {i}. {step}")


if __name__ == "__main__":
    main()
