from __future__ import annotations
import pandas as pd

__all__ = ["with_rsi_regime_flags"]


def with_rsi_regime_flags(
    df: pd.DataFrame,
    *,
    rsi_col: str = "rsi",
    overbought: float = 70.0,
    oversold: float = 30.0,
) -> pd.DataFrame:
    out = df.copy()
    out["rsi_overbought"] = (out[rsi_col] >= overbought).astype("int8")
    out["rsi_oversold"] = (out[rsi_col] <= oversold).astype("int8")
    # Simple streak of being in a regime
    regime = (
        out["rsi_overbought"].replace({0: pd.NA}).fillna(out["rsi_oversold"] * -1)
    )
    out["rsi_regime"] = regime.fillna(0).astype("int8")  # 1=OB, -1=OS, 0=neutral
    return out