# This is the dataset-level manifest for agg (single file indexing all parts + per-slice failures).

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

def _now_iso() -> str:
    # Keep your existing helper if you have one.
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()

@dataclass
class AggManifest:
    """
    Single manifest for an agg dataset (one slice universe).
    Tracks:
      - parts: parquet files + slice_ids they contain
      - slice_failures: per-slice load/parse/materialization failures
    """
    data: dict[str, Any]

    @classmethod
    def load_or_init(cls, path: Path, dataset_meta: dict[str, Any]) -> "AggManifest":
        if path.exists():
            with path.open("r", encoding="utf-8") as f:
                return cls(json.load(f))
        return cls(
            {
                "dataset": dataset_meta,
                "created_at": _now_iso(),
                "updated_at": _now_iso(),
                "parts": [],
                "slice_failures": {},  # slice_id -> {error, failed_at, ...}
                "head": {                 # 👈 ADD THIS
                    "slice_ids": [],
                },
            }
        )

    def all_slice_ids(self) -> set[str]:
        used: set[str] = set()
        for part in self.data.get("parts", []):
            for h in part.get("slice_ids", []):
                used.add(h)
        return used
    
    def head_slice_ids(self) -> list[str]:
        head = self.data.get("head")
        if not head:
            return []
        return list(head.get("slice_ids", []))


    def mark_slice_failed(self, slice_id: str, error: str) -> None:
        self.data.setdefault("slice_failures", {})[slice_id] = {
            "error": error,
            "failed_at": _now_iso(),
        }
        self.data["updated_at"] = _now_iso()

    def add_part(
        self,
        part_filename: str,
        slice_ids: list[str],
        row_count: int,
        min_open_time: int | None,
        max_open_time: int | None,
    ) -> None:
        self.data.setdefault("parts", []).append(
            {
                "part": part_filename,
                "slice_ids": slice_ids,
                "row_count": row_count,
                "min_open_time": min_open_time,
                "max_open_time": max_open_time,
                "created_at": _now_iso(),
            }
        )
        self.data["updated_at"] = _now_iso()
