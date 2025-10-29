import pandas as pd
import qlir.core.ops.pointwise as pt 
from qlir.utils.logdf import logdf
def tag_breakouts_simple(
    df: pd.DataFrame,
    price_col: str = "close",
    *,
    lookback: int = 5,
    min_move: float = 0.05,
    up_col: str = "breakout_up",
    down_col: str = "breakout_down",
    inplace: bool = False,
) -> pd.DataFrame:
    """
    Adds two columns:
      - up_col:   True where pct_change >= +min_move
      - down_col: True where pct_change <= -min_move
    """
    if lookback <= 0:
        raise ValueError("lookback must be a positive integer")
    if min_move < 0:
        raise ValueError("min_move must be non-negative")
    if price_col not in df.columns:
        raise KeyError(f"Column '{price_col}' not found")

    raw = df if inplace else df.copy()
    
    suffix=f"pct_{lookback}"
    with_pct_df = pt.add_pct_change(raw, "close", 20, suffix=suffix)
    pct_col = f"{price_col}__{suffix}"

    #Rename and add breakout series 
    tmp = with_pct_df
    tmp[up_col] = tmp[pct_col] >= min_move
    tmp[down_col] = tmp[pct_col] <= -min_move 

    # Clean up NaNs
    tmp[up_col] = tmp[up_col].fillna(False)
    tmp[down_col] = tmp[down_col].fillna(False)

    return tmp