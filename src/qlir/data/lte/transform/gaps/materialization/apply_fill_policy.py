# transform/gaps/materialize.py
from __future__ import annotations

from typing import Tuple

import pandas as _pd

from qlir.data.lte.transform.gaps.materialization.assert_materialization_complete import assert_materialization_complete

from ..blocks import find_missing_blocks
from ..context import build_fill_context
from .markers import ROW_MATERIALIZED_COL, FILL_POLICY_COL, SYNTHETIC_COL, DEFAULT_OHLC_COLS
from ...policy.base import FillPolicy


def apply_fill_policy(
    df: _pd.DataFrame,
    *,
    ohlc_cols: Tuple[str, str, str, str] = DEFAULT_OHLC_COLS,
    interval_s: int,
    policy: FillPolicy,
    strict: bool = True,
) -> _pd.DataFrame:
    """
    Apply a fill policy to materialized (missing) rows.

    Preconditions
    -------------
    - materialize_missing_rows() has already been run
    - missing rows are marked via `_INTERNAL_MATERIALIZED_COL`

    This function:
    - finds contiguous missing blocks
    - builds FillContext objects
    - delegates OHLC generation to the policy
    - writes values back into the DataFrame
    - tags synthetic rows

    Parameters
    ----------
    df : _pd.DataFrame
        Materialized DataFrame.
    ts_col : str
        Name of the timestamp column or index name.
    ohlc_cols : tuple[str, str, str, str]
        (open, high, low, close) column names.
    interval_s : int
        Wall-clock interval in seconds.
    policy : FillPolicy
        Policy used to generate synthetic rows.
    strict : bool
        If True, raises on any invariant violation.

    Returns
    -------
    _pd.DataFrame
        DataFrame with filled OHLC values.
    """
    assert_materialization_complete(df)

    out = df.copy()

    open_col, high_col, low_col, close_col = ohlc_cols

    # Ensure synthetic tracking columns exist
    if SYNTHETIC_COL not in out.columns:
        out[SYNTHETIC_COL] = False
    if FILL_POLICY_COL not in out.columns:
        out[FILL_POLICY_COL] = None

    blocks = find_missing_blocks(out)

    for block in blocks:
        ctx = build_fill_context(
            df=out,
            block=block,
            ohlc_cols=ohlc_cols,
            interval_s=interval_s,
            context_window_per_side=getattr(policy, "vol_window", 0),
            strict=strict,
        )

        generated = policy.generate(ctx)

        if generated.empty:
            continue

        # Align on index (timestamps)
        idx = generated.index

        # Write OHLC values
        out.loc[idx, open_col] = generated["open"].values
        out.loc[idx, high_col] = generated["high"].values
        out.loc[idx, low_col] = generated["low"].values
        out.loc[idx, close_col] = generated["close"].values

        # Tag metadata
        out.loc[idx, SYNTHETIC_COL] = True
        out.loc[idx, FILL_POLICY_COL] = policy.name

    return out
