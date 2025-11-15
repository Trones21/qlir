# Avoid import-time cycle with qlir.io.reader
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    # gives type checkers a symbol, but doesn't import at runtime
    from .loader.load import load_ohlcv as load_ohlcv  # noqa: F401

def load_ohlcv(*args, **kwargs):
    # import only when actually called
    from .loader.load import load_ohlcv as _impl
    return _impl(*args, **kwargs)


from .sources.drift.to_refactor_away_drift import get_candles
from .normalize import normalize_candles
__all__ = ["normalize_candles", "get_candles"]