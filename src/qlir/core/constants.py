from qlir.core.types.OHLC_Cols import OHLC_Cols


DEFAULT_OPEN_TIMESTAMP_COL = "tz_start"
DEFAULT_OHLC_COLS = OHLC_Cols(
    open="open",
    high="high",
    low="low",
    close="close",
)