# qlir/df/condition_set/construction.py

"""
Condition Set Construction

Row-wise logical construction of boolean condition sets.

This module:
- combines existing boolean columns into named boolean artifacts
- preserves rows
- performs no temporal reasoning
- performs no coercion unless explicitly allowed

Condition sets produced here are intended for downstream use in:
- persistence / run analysis
- gating logic
- aggregation
- execution logic
"""

from __future__ import annotations

import pandas as pd


# ----------------------------
# validation helpers
# ----------------------------

def _assert_cols_exist(df: pd.DataFrame, cols: list[str]) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise KeyError(
            f"Condition columns not found: {missing}. "
            f"Available columns: {list(df.columns)}"
        )


def _assert_boolean_no_na(df: pd.DataFrame, cols: list[str]) -> None:
    for c in cols:
        s = df[c]
        if s.dtype != bool and str(s.dtype) != "boolean":
            raise TypeError(
                f"Condition column '{c}' must be boolean dtype; got {s.dtype}"
            )
        if s.isna().any():
            raise ValueError(
                f"Condition column '{c}' contains NaNs; "
                "condition sets require explicit True/False values"
            )


def _validate_inputs(
    df: pd.DataFrame,
    cols: list[str],
    *,
    strict: bool,
) -> None:
    _assert_cols_exist(df, cols)
    if strict:
        _assert_boolean_no_na(df, cols)


# ----------------------------
# construction primitives
# ----------------------------

def all_of(
    df: pd.DataFrame,
    cols: list[str],
    *,
    out_col: str,
    strict: bool = True,
) -> tuple[pd.DataFrame, str]:
    """
    Logical AND across multiple boolean columns.

    True only if all conditions are True on the row.
    """
    if not cols:
        raise ValueError("all_of requires at least one column")

    _validate_inputs(df, cols, strict=strict)

    if out_col in df.columns:
        raise ValueError(f"Output column already exists: {out_col}")

    df = df.copy()
    df[out_col] = df[cols].all(axis=1)

    return df, out_col


def any_of(
    df: pd.DataFrame,
    cols: list[str],
    *,
    out_col: str,
    strict: bool = True,
) -> tuple[pd.DataFrame, str]:
    """
    Logical OR across multiple boolean columns.

    True if any condition is True on the row.
    """
    if not cols:
        raise ValueError("any_of requires at least one column")

    _validate_inputs(df, cols, strict=strict)

    if out_col in df.columns:
        raise ValueError(f"Output column already exists: {out_col}")

    df = df.copy()
    df[out_col] = df[cols].any(axis=1)

    return df, out_col


def at_least_k_of(
    df: pd.DataFrame,
    cols: list[str],
    *,
    k: int,
    out_col: str,
    strict: bool = True,
) -> tuple[pd.DataFrame, str]:
    """
    True if at least k of the provided conditions are True on the row.
    """
    if not cols:
        raise ValueError("at_least_k_of requires at least one column")

    if k <= 0:
        raise ValueError("k must be >= 1")

    if k > len(cols):
        raise ValueError(
            f"k={k} exceeds number of columns ({len(cols)})"
        )

    _validate_inputs(df, cols, strict=strict)

    if out_col in df.columns:
        raise ValueError(f"Output column already exists: {out_col}")

    df = df.copy()
    df[out_col] = df[cols].sum(axis=1) >= k

    return df, out_col


def negate(
    df: pd.DataFrame,
    col: str,
    *,
    out_col: str,
    strict: bool = True,
) -> tuple[pd.DataFrame, str]:
    """
    Logical NOT of a boolean condition column.
    """
    _validate_inputs(df, [col], strict=strict)

    if out_col in df.columns:
        raise ValueError(f"Output column already exists: {out_col}")

    df = df.copy()
    df[out_col] = ~df[col]

    return df, out_col
