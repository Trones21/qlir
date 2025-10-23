from dataclasses import dataclass, field
from typing import Sequence, Literal

@dataclass
class SeriesSpec:
    col: str
    kind: Literal["line", "scatter", "bar"] = "line"
    yaxis: int = 0
    label: str | None = None

@dataclass
class BandSpec:
    y0: float | str
    y1: float | str
    alpha: float = 0.15
    label: str | None = None

@dataclass
class EventSpec:
    when_col: str
    label_col: str | None = None

@dataclass
class TableSpec:
    cols: list[str]
    rows: dict = field(default_factory=lambda: {"kind": "tail", "n": 100})

@dataclass
class Panel:
    title: str
    series: list[SeriesSpec] = field(default_factory=list)
    bands: list[BandSpec] = field(default_factory=list)
    events: list[EventSpec] = field(default_factory=list)
    table: TableSpec | None = None
    height: int = 280

@dataclass
class View:
    title: str
    panels: Sequence[Panel]