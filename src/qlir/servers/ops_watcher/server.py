from __future__ import annotations

import argparse
import sys
from pathlib import Path

from .config import load_config
from .runner import run_forever, run_once
from .state import load_state, save_state


def main() -> None:
    ap = argparse.ArgumentParser(prog="qlir-ops-watcher")
    ap.add_argument("--config", required=True, help="Path to ops_watcher.toml")
    ap.add_argument("--once", action="store_true", help="Run checks once and exit")
    args = ap.parse_args()

    cfg = load_config(args.config)
    state_path = cfg.service.state_path
    state = load_state(state_path)

    try:
        if args.once:
            run_once(cfg, state)
        else:
            run_forever(cfg, state)
    finally:
        save_state(state_path, state)


if __name__ == "__main__":
    main()
