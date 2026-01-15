# ----------------------------
# Helpers
# ----------------------------

from typing import List, Union
import warnings

from pandas import DataFrame, Series, api

from qlir.core.types.union import ColsLike


def _numeric_cols(df: DataFrame) -> List[str]:
    return [c for c in df.columns if api.types.is_numeric_dtype(df[c])]

def _normalize_cols(df: DataFrame, cols: ColsLike) -> List[str]:
    if cols is None:
        return _numeric_cols(df)
    if isinstance(cols, str):
        cols = [cols]
    valid = [c for c in cols if c in df.columns]
    invalid = [c for c in cols if c not in df.columns]
    if invalid:
        warnings.warn(f"Ignoring missing columns: {invalid}", RuntimeWarning)
    return valid

def _maybe_copy(df: DataFrame, inplace: bool) -> DataFrame:
    return df if inplace else df.copy()

def _safe_name(base: str, *parts: Union[str, int]) -> str:
    # join non-empty parts with '__'
    extras = [str(p) for p in parts if p is not None and str(p) != ""]
    return f"{base}__{'__'.join(extras)}" if extras else base

# ----------------------------
# Return Prepper 
# ----------------------------

def _add_columns_from_series_map(
    out: DataFrame,
    *,
    use_cols: list[str],
    series_by_col: dict[str, Series],
    suffix: str,
) -> tuple[DataFrame, tuple[str, ...]]:
    new_col_names: list[str] = []

    for c in use_cols:
        name = _safe_name(c, suffix)
        out[name] = series_by_col[c]
        new_col_names.append(name)

    return out, tuple(new_col_names)




def one(col_names: tuple[str, ...]) -> str:
    '''Many of these funcs return of tuple iterable of str, but when you pass in a single column you only get a single new col name back, this is a litte helper that ensures there is only one value and pulls it out 
    Example Usage:
    df, diff_cols = with_diff(df, price_col)
    diff_col = one(diff_cols)
    '''
    if len(col_names) != 1:
        raise ValueError(f"Expected exactly one column, got {len(col_names)}")
    return col_names[0]
