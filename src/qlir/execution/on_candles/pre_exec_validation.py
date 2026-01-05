from __future__ import annotations

from dataclasses import dataclass
from functools import wraps
from typing import Optional

import pandas as _pd


# ---------------------------------------------------------------------
# Validation configuration
# ---------------------------------------------------------------------

@dataclass(frozen=True)
class CandleTimeSpec:
    """
    Specifies how time is represented in a candle DataFrame.

    Default behavior:
    - Time must be the DatetimeIndex.

    Escape hatch:
    - Set require_index=False and provide ts_col.
    """
    require_index: bool = True
    ts_col: Optional[str] = None


@dataclass(frozen=True)
class CandleValidationPolicy:
    """
    Structural invariants required for candle-based execution.
    """
    require_monotonic: bool = True
    require_fixed_interval: bool = True
    require_no_gaps: bool = True


# ---------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------

def _resolve_time_index(
    df: _pd.DataFrame,
    *,
    time_spec: CandleTimeSpec,
) -> _pd.DatetimeIndex:
    """
    Resolve the time index used for validation.

    Returns a DatetimeIndex (may be derived from a column).
    """
    if time_spec.require_index:
        if not isinstance(df.index, _pd.DatetimeIndex):
            raise ValueError(
                "Candle execution requires DatetimeIndex. "
                "If time is stored in a column, set "
                "CandleTimeSpec(require_index=False, ts_col=...)."
            )
        return df.index

    if time_spec.ts_col is None:
        raise ValueError(
            "ts_col must be provided when require_index=False"
        )

    if time_spec.ts_col not in df.columns:
        raise ValueError(
            f"Timestamp column '{time_spec.ts_col}' not found in DataFrame"
        )

    ts = _pd.to_datetime(df[time_spec.ts_col], errors="raise")
    return _pd.DatetimeIndex(ts)


# ---------------------------------------------------------------------
# Public validation
# ---------------------------------------------------------------------

def validate_candle_frame(
    df: _pd.DataFrame,
    *,
    interval_s: int,
    time_spec: CandleTimeSpec = CandleTimeSpec(),
    policy: CandleValidationPolicy = CandleValidationPolicy(),
) -> None:
    """
    Validate that a candle DataFrame is safe for execution models.

    Preconditions enforced:
    - Wall-clock time
    - Monotonic increasing timestamps
    - Fixed interval
    - No gaps
    """
    if df.empty:
        raise ValueError("Candle DataFrame is empty")

    ts = _resolve_time_index(df, time_spec=time_spec)

    if policy.require_monotonic and not ts.is_monotonic_increasing:
        raise ValueError("Candle timestamps are not monotonic increasing")

    deltas = ts.to_series().diff().dropna()
    expected = _pd.Timedelta(seconds=interval_s)

    if policy.require_fixed_interval:
        if not (deltas == expected).all():
            bad = deltas[deltas != expected].iloc[0]
            raise ValueError(
                f"Non-uniform candle interval detected. "
                f"Expected {expected}, found {bad}."
            )

    if policy.require_no_gaps:
        if (deltas > expected).any():
            raise ValueError(
                "Candle gaps detected (missing wall-clock intervals)"
            )


# ---------------------------------------------------------------------
# Decorator
# ---------------------------------------------------------------------

def requires_valid_candles(
    *,
    time_spec: CandleTimeSpec = CandleTimeSpec(),
    policy: CandleValidationPolicy = CandleValidationPolicy(),
):
    """
    Decorator enforcing candle-frame execution invariants.

    Expects:
    - candles DataFrame as first positional argument OR keyword 'candles'
    - interval_s passed as a keyword argument at call time
    """
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            if "interval_s" not in kwargs:
                raise ValueError(
                    "interval_s must be provided for candle execution"
                )

            # Resolve candles DataFrame
            if "candles" in kwargs:
                df = kwargs["candles"]
            elif args:
                df = args[0]
            else:
                raise ValueError("No candle DataFrame provided")

            validate_candle_frame(
                df,
                interval_s=kwargs["interval_s"],
                time_spec=time_spec,
                policy=policy,
            )
            return fn(*args, **kwargs)
        return wrapper
    return decorator
