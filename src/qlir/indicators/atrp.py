import pandas as pd
from qlir.core.ops._helpers import _maybe_copy, one
from qlir.core.ops.temporal import with_diff
from qlir.indicators.sma import sma

__all__ = ["atrp"]

def atrp(
    df: pd.DataFrame,
    *,
    price_col: str,
    window: int,
    price_ref: str = "prev",  # or "open", "first", etc.
    keep: str | list[str] = "final",
    out_col: str | None = None,
    inplace: bool = False,
) -> tuple[pd.DataFrame, str]:
    """
    Average True Range Percent (open-to-open variant).
    """

    out = _maybe_copy(df, inplace)

    # 1. Absolute open-to-open change
    out, abs_cols = with_diff(
        out,
        price_col,
        suffix="abs_tr"
    )
    abs_col = one(abs_cols)

    # 2. Smooth (ATR)
    out, atr_col = sma(
        out,
        col=abs_col,
        window=window,
        prefix_2_default_col_name=f"atr_{window}"
    )
    
    return out, "debug"
    # 3. Percent normalization
    ref_col = _resolve_price_ref(out, price_col, price_ref)
    out[out_col or f"{atr_col}_pct"] = out[atr_col] / out[ref_col]

    final_col = out_col or f"{atr_col}_pct"

    # 4. Cleanup
    if keep != "all":
        keep_cols = {final_col} if keep == "final" else set(keep)
        drop = [c for c in [abs_col, atr_col] if c not in keep_cols]
        out.drop(columns=drop, inplace=True)

    return out, final_col
