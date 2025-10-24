# common/temporal.py
import pandas as pd

def temporal_derivatives(
    df: pd.DataFrame,
    *,
    cols: list[str],
    prefix: str | None = None,
    include_pct: bool = False,
    in_place: bool = False,
) -> pd.DataFrame:
    """
    Compute first temporal derivatives (slopes) for selected columns.
    Optionally include percent change versions.
    """
    out = df if in_place else df.copy()

    for col in cols:
        name = f"{prefix + '_' if prefix else ''}{col}_slope"
        out[name] = out[col].diff()

        if include_pct:
            out[f"{name}_pct"] = out[col].pct_change()

    return out
