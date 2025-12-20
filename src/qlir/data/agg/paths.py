from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

@dataclass(frozen=True)
class DatasetPaths:
    """
    Paths for a single slice universe, e.g.:
      raw/.../BTCUSDT/1m/limit=500/
      agg/.../BTCUSDT/1m/limit=500/
    """
    raw_root: Path   # .../raw/BTCUSDT/1m/limit=500
    agg_root: Path   # .../agg/BTCUSDT/1m/limit=500

    @property
    def raw_manifest_path(self) -> Path:
        return self.raw_root / "manifest.json"

    @property
    def raw_responses_dir(self) -> Path:
        return self.raw_root / "responses"

    @property
    def agg_manifest_path(self) -> Path:
        return self.agg_root / "manifest.json"

    @property
    def agg_parts_dir(self) -> Path:
        return self.agg_root / "parts"
