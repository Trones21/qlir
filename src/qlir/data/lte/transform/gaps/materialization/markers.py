from typing import Tuple


ROW_MATERIALIZED_COL = "__row_materialized"
SYNTHETIC_COL = "is_synthetic"
FILL_POLICY_COL = "fill_policy"


DEFAULT_OPEN_TIMESTAMP_COL = "tz_start"
DEFAULT_OHLC_COLS: Tuple[str, str, str, str] = (
    "open",
    "high",
    "low",
    "close",
)