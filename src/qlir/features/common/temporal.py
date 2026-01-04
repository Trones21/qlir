# common/temporal.py
import pandas as pd

def slope(
    df: pd.DataFrame,
    *,
    cols: list[str],
    prefix: str | None = None,
    include_pct: bool = False,
    in_place: bool = False,
) -> tuple[pd.DataFrame, tuple[str, ...]]:
    """
    Compute first temporal derivatives (slopes) for selected columns.
    Optionally include percent change versions.
    """
    out = df if in_place else df.copy()
    new_cols = set()
    for col in cols:
        name = f"{prefix + '_' if prefix else ''}{col}_slope"
        out[name] = out[col].diff()
        new_cols.add(name)

        if include_pct:
            out[f"{name}_pct"] = out[col].pct_change()
            new_cols.add(f"{name}_pct")

    return out, tuple(new_cols)
