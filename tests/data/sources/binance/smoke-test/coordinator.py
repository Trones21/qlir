#!/usr/bin/env python
"""
Coordinator for Binance data server smoke-test.

Responsibilities:
- Create a temporary-ish data_root.
- Spawn data_server.py in a separate process.
- Wait until files appear under data_root (or timeout).
- Print a small summary, then terminate the data_server process.

Exit code:
- 0 on success (files observed).
- 1 on failure (timeout or child exits early with no files).

python tests/data/sources/binance/smoke-test/coordinator.py [--data-root <data_root>]

"""

from __future__ import annotations

import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import List


def _list_files_under(root: Path) -> List[Path]:
    return [p for p in root.rglob("*") if p.is_file()]


def main() -> int:
    base_dir = Path(__file__).parent

    # Fresh data_root for each run so you know files are from THIS run.
    data_root = base_dir / "_data_root"
    if data_root.exists():
        shutil.rmtree(data_root)
    data_root.mkdir(parents=True, exist_ok=True)

    data_server_path = base_dir / "data_server.py"

    cmd = [
        sys.executable,
        str(data_server_path),
        "--data-root",
        str(data_root),
    ]

    print(f"[coord] starting data_server: {' '.join(cmd)}")
    proc = subprocess.Popen(cmd)

    try:
        timeout_s = 60.0
        poll_interval_s = 2
        deadline = time.time() + timeout_s

        files: list[Path] = []

        while time.time() < deadline:
            # If the child died early, bail out and report.
            ret = proc.poll()
            if ret is not None:
                print(f"[coord] data_server exited early with code={ret}")
                break

            files = _list_files_under(data_root)
            if files:
                break

            time.sleep(poll_interval_s)

        if not files:
            print(
                f"[coord] no files observed under {data_root} "
                f"within {timeout_s} seconds",
                file=sys.stderr,
            )
            return 1

        print(f"[coord] observed {len(files)} files under data_root; first few:")
        for p in files[:10]:
            print("   ", p.relative_to(data_root))

        return 0

    finally:
        if proc.poll() is None:
            print("[coord] terminating data_server...")
            proc.terminate()
            try:
                proc.wait(timeout=10.0)
            except subprocess.TimeoutExpired:
                print("[coord] terminate timed out; killing...")
                proc.kill()
                proc.wait(timeout=5.0)

        print(f"[coord] data_server final exit code: {proc.returncode}")


if __name__ == "__main__":
    raise SystemExit(main())
