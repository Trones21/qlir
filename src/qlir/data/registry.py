# data/registry.py
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Literal, Dict, List, Mapping, Optional
import pandas as pd

Label = Literal["start", "end"]  # how the upstream timestamp is labeled

@dataclass(frozen=True)
class CandleSpec:
    venue: str
    label: Label                         # start/end label of upstream 'timestamp'
    # How to convert "resolution" to a pandas offset for bounds:
    # e.g. {"1": "1min", "5": "5min", "60": "60min", "240": "240min", "D": "1D"}
    res_to_offset: Mapping[str, str]
    # Column alias preference per field (highest priority first):
    timestamp_fields: List[str] = field(default_factory=lambda: ["timestamp", "ts", "time"])
    open_fields: List[str] = field(default_factory=lambda: ["open", "fillOpen"])
    high_fields: List[str] = field(default_factory=lambda: ["high", "fillHigh"])
    low_fields:  List[str] = field(default_factory=lambda: ["low", "fillLow"])
    close_fields: List[str] = field(default_factory=lambda: ["close", "fillClose"])
    base_volume_fields: List[str] = field(default_factory=lambda: ["baseVolume", "volume", "vol"])
    quote_volume_fields: List[str] = field(default_factory=lambda: ["quoteVolume", "notionalVolume"])
    # Rolling last-candle behavior (true ⇒ last candle’s close updates with ticks)
    rolling_last_close: bool = True
    # If true, prefer base volume; else fallback to quote when base missing
    prefer_base_volume: bool = True
    # Optional: venue default timezone for display (data stays UTC)
    display_tz: str = "UTC"

REGISTRY: Dict[str, CandleSpec] = {
    "drift": CandleSpec(
        venue="drift",
        label="start",  # daily 00:00 covers [00:00, 24:00), last bar rolls
        res_to_offset={
            "1": "1min", "5": "5min", "15": "15min", "60": "60min", "240": "240min",
            "D": "1D", "W": "7D", "M": "30D",  # M=30D nominal; override if venue returns true calendar months
        },
        timestamp_fields=["timestamp", "ts", "time"],
        rolling_last_close=True,
    ),
    # Add others here...
}
