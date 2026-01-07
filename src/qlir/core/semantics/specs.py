# qlir/core/semantics/specs.py

from __future__ import annotations

from .row_derivation import ColumnDerivationSpec


def rolling_spec(
    *,
    op: str,
    base_col: str,
    window: int,
    scope: str = "output",
    shift: int = 0,
) -> ColumnDerivationSpec:
    """
    Semantic spec for rolling-window-derived columns.

    Parameters
    ----------
    op:
        Operation name (e.g. "sma", "ema", "atr").
    base_col:
        Column the rolling window is applied to.
    window:
        Window size (number of rows).
    scope:
        "output" or "intermediate".
    shift:
        Number of rows the result is shifted forward.
        shift=0 => self-inclusive ([i-(window-1) .. i])
        shift=1 => row-exclusive ([i-window .. i-1])
    """

    if window <= 0:
        raise ValueError("window must be positive")

    if shift < 0:
        raise ValueError("shift must be >= 0")

    # Compute inclusive bounds relative to row i
    # Example:
    #   window=3, shift=0 => [-2 .. 0]
    #   window=3, shift=1 => [-3 .. -1]
    lo = -(window - 1) - shift
    hi = -shift

    return ColumnDerivationSpec(
        op=op,
        base_cols=(base_col,),
        read_rows=(lo, hi),
        scope=scope,
        self_inclusive=(shift == 0),
    )
