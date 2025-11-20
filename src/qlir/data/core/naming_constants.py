# Default extension for canonical candle datasets on disk.
from qlir.time.timefreq import TimeUnit


DEFAULT_CANDLES_EXT: str = ".parquet"

CANONICAL_RESOLUTION_UNIT_MAP = {
    TimeUnit.SECOND: "s",
    TimeUnit.MINUTE: "m",
    TimeUnit.HOUR:   "h",
    TimeUnit.DAY:    "D",
}

REVERSE_CANONICAL_RESOLUTION_UNIT_MAP = {v: k for k, v in CANONICAL_RESOLUTION_UNIT_MAP.items()}
