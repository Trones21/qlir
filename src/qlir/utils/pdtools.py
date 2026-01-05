import pandas as _pd


def null_if(df: _pd.DataFrame, cond_col: str, target_cols: list[str], negate=True):
    mask = ~df[cond_col] if negate else df[cond_col]
    for c in target_cols:
        df[c] = df[c].mask(mask, _pd.NA)
    return df