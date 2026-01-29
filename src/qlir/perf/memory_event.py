from dataclasses import dataclass
from typing import Optional

@dataclass(frozen=True)
class MemoryEvent:
    label: Optional[str]
    df_bytes_before: Optional[int]
    df_bytes_after: Optional[int]
    rss_before: int
    rss_after: int
    elapsed_s: float

    @property
    def df_delta_bytes(self) -> Optional[int]:
        if self.df_bytes_before is None or self.df_bytes_after is None:
            return None
        return self.df_bytes_after - self.df_bytes_before

    @property
    def rss_delta_bytes(self) -> int:
        return self.rss_after - self.rss_before


def fmt_bytes(n: Optional[int]) -> Optional[str]:
    if n is None:
        return None
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024:
            return f"{n:.2f} {unit}"
        n //= 1024
    return f"{n:.2f} PB"
