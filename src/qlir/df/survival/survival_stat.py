from dataclasses import dataclass
import pandas as _pd

@dataclass(frozen=True, slots=True)
class SurvivalStat:
    bar_count: int
    survival_rate: float | str

    def __post_init__(self) -> None:
        if self.bar_count <= 0:
            raise ValueError(
                f"bar_count must be > 0, got {self.bar_count}"
            )

    def description(self) -> str:
        return (
            f"Trend has lasted {self.bar_count} candles "
            f"which is in the top {self.survival_rate}"
        )




