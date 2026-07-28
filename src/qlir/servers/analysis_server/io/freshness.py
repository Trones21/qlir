# analysis_server/io/freshness.py
"""
Cheap "has anything new landed?" signal for a parquet directory.

The analysis server is read-only. The agg server appends to `head.parquet` and
occasionally rotates it into a sealed chunk, so *any* new data changes either the
file count or the newest file's mtime. Fingerprinting those two things lets the
main loop skip the (expensive) ETL on iterations where nothing has arrived.

The fingerprint over-approximates "new data": every write bumps it, so it may
occasionally report a change when the latest candle did not actually advance
(e.g. an end-of-rotation head rewrite). That is the safe direction — we never
*miss* real new data, we only sometimes do an ETL we could have skipped.
"""
from __future__ import annotations

from pathlib import Path

from qlir.telemetry.telemetry import telemetry

# (file_count, max_mtime_ns). (0, 0) means "no parquet files present".
DirFingerprint = tuple[int, int]


@telemetry(console=True)
def dir_data_fingerprint(agg_dir: Path, *, pattern: str = "*.parquet") -> DirFingerprint:
    """Return a cheap change-signal for `agg_dir` without reading any row data."""
    max_mtime = 0
    count = 0
    for f in agg_dir.glob(pattern):
        count += 1
        mtime = f.stat().st_mtime_ns
        if mtime > max_mtime:
            max_mtime = mtime
    return (count, max_mtime)
