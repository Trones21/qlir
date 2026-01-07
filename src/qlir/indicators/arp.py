import pandas as pd
from qlir.core.constants import DEFAULT_OHLC_COLS
from qlir.core.ops._helpers import _maybe_copy
from qlir.core.semantics.decorators import new_col_func
from qlir.core.semantics.row_derivation import ColumnDerivationSpec
from qlir.core.types.OHLC_Cols import OHLC_Cols
from qlir.indicators.sma import sma

__all__ = ["arp"]

from typing import NamedTuple
import pandas as pd



@new_col_func(
    specs=lambda *, window, window_includes_current=True, **_: ColumnDerivationSpec(
        op="arp",
        base_cols=("high", "low", "open"),
        read_rows=(
            (-(window - 1), 0) if window_includes_current
            else (-window, -1)
        ),
        scope="output",
        self_inclusive=True,
        log_suffix="arp() is ALWAYS self-inclusive"
    )
)
def arp(
    df: pd.DataFrame,
    *,
    ohlc: OHLC_Cols = DEFAULT_OHLC_COLS,
    window: int,
    keep: str | list[str] = "final",
    out_col: str | None = None,
    inplace: bool = False,
) -> tuple[pd.DataFrame, str]:
    """
    Average Range Percent (ARP).

      Steps:
      1. range_abs = high - low
      2. range_pct = range_abs / open
      3. arp      = SMA(range_pct, window)

    Computes the average intrabar range as a percentage of open:

    Notes
    -----
    This indicator was originally prototyped as "ATRP", but the QLIR
    semantic routing table rejected the attempt to spoof Wilder.

    ARP intentionally uses intrabar range only:
      - no True Range
      - no gap adjustment
      - no cross-bar dependencies

    The result is a clean, execution-safe measure of intrabar volatility
    intensity, normalized to price.

    Temporal Semantics
    ------------------
    ARP is always computed as a self-inclusive rolling statistic.

    That is, for row i, the ARP window includes row i itself:
        [i-(window-1) … i]

    ARP does not provide an execution-safe (i-1) variant by design.

    If ARP is used as a baseline for realtime comparison or event detection,
    callers MUST explicitly shift it, e.g.:

        arp_baseline = arp(...).shift(1)

    Temporal alignment is a responsibility of the consumer, not the indicator.
    """

    out = _maybe_copy(df, inplace)

    # 1. Absolute intrabar range
    out, range_col = with_range(
        out,
        high_col=ohlc.high,
        low_col=ohlc.low,
    )

    # 2. Percent normalization (same bar, open baseline)
    range_pct_col = f"{range_col}_pct"
    out[range_pct_col] = out[range_col] / out[ohlc.open]

    # 3. Average (SMA over percent range)

    out, atrp_col = sma(
        out,
        col=range_pct_col,
        window=window,
        prefix_2_default_col_name="atrp",
    )

    final_col = out_col or atrp_col

    # 4. Cleanup
    if keep != "all":
        keep_cols = {final_col} if keep == "final" else set(keep)
        drop = [c for c in (range_col, range_pct_col) if c not in keep_cols]
        out.drop(columns=drop, inplace=True)

    return out, final_col

