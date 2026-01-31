# qlir/df/scalars/units.py

from __future__ import annotations

from typing import Union, Protocol, TypeVar, overload

import numpy as np
import pandas as pd



# ---- public API -------------------------------------------------------------
@overload
def delta_in_bps(delta_move: float, ref_price: float) -> float: ...

@overload
def delta_in_bps(delta_move: np.ndarray, ref_price: np.ndarray) -> np.ndarray: ...

@overload
def delta_in_bps(delta_move: pd.Series, ref_price: pd.Series) -> pd.Series: ...


def delta_in_bps(delta_move, ref_price):
    """
    Convert an price move into basis points (bps)
    relative to a reference price.

    This function is intentionally written in a *scalar style*
    but is fully compatible with vectorized inputs such as
    NumPy arrays and Pandas Series.

    Parameters
    ----------
    delta_move
        deltaolute price move(s). May be a scalar, NumPy array,
        or Pandas Series.

    ref_price
        Reference price(s) used for normalization. Must be
        broadcast-compatible with `delta_move`.

    Returns
    -------
    Same type as input
        Basis-point representation of the move(s), where:

            1 bp = 0.01%

        Computed as:
            (delta_move / ref_price) * 10_000

    Notes
    -----
    Why this works for Pandas / NumPy inputs:

    - Pandas Series and NumPy arrays overload arithmetic
      operators (/, *) to perform element-wise operations.
    - Index alignment (for Pandas) is preserved automatically.
    - No Python-level loops are used; execution occurs in
      vectorized NumPy/C code.

    This makes the function safe and efficient for use inside
    large DataFrame pipelines.
    """
    return (delta_move / ref_price) * 10_000


@overload
def delta_in_pct(delta_move: float, ref_price: float) -> float: ...

@overload
def delta_in_pct(delta_move: np.ndarray, ref_price: np.ndarray) -> np.ndarray: ...

@overload
def delta_in_pct(delta_move: pd.Series, ref_price: pd.Series) -> pd.Series: ...


def delta_in_pct(delta_move, ref_price):
    """
    Convert a signed deltaolute price move into a percentage (%) move
    relative to a reference price.

    Parameters
    ----------
    delta_move
        Signed scalar price move(s). Scalar or vectorized.

    ref_price
        Reference price(s). Scalar or vectorized.

    Returns
    -------
    Same type as input
        Percentage representation of the move(s).

    Notes
    -----
    This function shares the same vectorization and alignment
    semantics as `delta_in_bps`. See that docstring for details.
    """
    return (delta_move / ref_price) * 100.0


