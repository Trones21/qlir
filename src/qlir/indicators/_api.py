from __future__ import annotations
import pandas as pd

from .atrp import atrp
from .sma import sma
from .rsi import rsi

__all__ = [
    "atrp",
    "sma",
    "rsi",  
]

_INDICATORS = {
    "atrp": atrp,
    "sma": sma,
    "rsi": rsi,
}


def _apply_indicator(
    df: pd.DataFrame,
    name: str,
    **kwargs,
) -> tuple[pd.DataFrame, str]:
    fn = _INDICATORS.get(name.lower())
    if fn is None:
        raise ValueError(
            f"Unknown indicator: {name}. "
            f"Available: {list(_INDICATORS)}"
        )
    return fn(df, **kwargs)