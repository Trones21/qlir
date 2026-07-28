"""
Primitive operations of the technical-analysis library, as matched pandas/polars
micro-benchmarks tagged by **op-class**.

The unit of measurement here is the OPERATION, not a composed pipeline. The
engine winner is a property of the op-class (rolling, groupby-transform,
cumulative, ...), so a map over op-classes generalizes to any pipeline built from
them — instead of a pile of per-pipeline numbers.

Each op is implemented twice with equivalent semantics; parity is checked by
tests/benchmarks/test_op_parity.py. `parity` says how to compare the two:
  values  -> compare numeric output (optionally row-sorted first)
  shape   -> compare row count only (order/tie-break differs across engines)
  none    -> not comparable (e.g. the conversion cost, which is directional)
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

import numpy as np
import pandas as pd
import polars as pl

WINDOW = 20
GROUP_SIZE = 50  # rows per synthetic group for groupby ops


@dataclass(frozen=True)
class Op:
    name: str
    cls: str
    pandas: Callable
    polars: Callable
    parity: str = "values"
    sort_rows: bool = False


def _group_key(n: int) -> np.ndarray:
    return np.arange(n) // GROUP_SIZE


# --- pandas impls ----------------------------------------------------------

def _p_rolling_mean(df):
    return df["close"].rolling(WINDOW).mean()

def _p_rolling_std(df):
    return df["close"].rolling(WINDOW).std()

def _p_ewm(df):
    return df["close"].ewm(span=WINDOW, adjust=False).mean()

def _p_diff(df):
    return df["close"].diff()

def _p_cummax(df):
    return df["close"].cummax()

def _p_segment_id(df):
    up = (df["close"].diff() > 0)
    return (up != up.shift(fill_value=False)).cumsum()

def _p_gt_transform(df):
    g = _group_key(len(df))
    return df["close"].groupby(g).transform("max")

def _p_gb_agg(df):
    g = _group_key(len(df))
    return df["close"].groupby(g).max()

def _p_sort(df):
    return df.sort_values("close")

def _p_unique(df):
    k = (df["close"] * 10).astype("int64")
    return df.assign(_k=k).drop_duplicates("_k")

def _p_filter(df):
    return df[df["close"] > df["close"].mean()]

def _p_to_polars(df):
    return pl.from_pandas(df)


# --- polars impls ----------------------------------------------------------

def _pl_rolling_mean(df):
    return df.select(pl.col("close").rolling_mean(WINDOW))

def _pl_rolling_std(df):
    return df.select(pl.col("close").rolling_std(WINDOW))

def _pl_ewm(df):
    return df.select(pl.col("close").ewm_mean(span=WINDOW, adjust=False))

def _pl_diff(df):
    return df.select(pl.col("close").diff())

def _pl_cummax(df):
    return df.select(pl.col("close").cum_max())

def _pl_segment_id(df):
    return (
        df.with_columns((pl.col("close").diff() > 0).fill_null(False).alias("_up"))
        .select((pl.col("_up") != pl.col("_up").shift(fill_value=False)).cum_sum())
    )

def _pl_gt_transform(df):
    g = _group_key(len(df))
    return df.with_columns(pl.Series("_g", g)).select(pl.col("close").max().over("_g"))

def _pl_gb_agg(df):
    g = _group_key(len(df))
    return (
        df.with_columns(pl.Series("_g", g))
        .group_by("_g")
        .agg(pl.col("close").max())
        .get_column("close")
    )

def _pl_sort(df):
    return df.sort("close")

def _pl_unique(df):
    return df.with_columns((pl.col("close") * 10).cast(pl.Int64).alias("_k")).unique(subset="_k")

def _pl_filter(df):
    return df.filter(pl.col("close") > pl.col("close").mean())

def _pl_to_pandas(df):
    return df.to_pandas()


_OPS = [
    Op("rolling_mean", "rolling", _p_rolling_mean, _pl_rolling_mean),
    Op("rolling_std", "rolling", _p_rolling_std, _pl_rolling_std),
    Op("ewm_mean", "ewm", _p_ewm, _pl_ewm),
    Op("diff", "elementwise", _p_diff, _pl_diff),
    Op("cummax", "cumulative", _p_cummax, _pl_cummax),
    Op("segment_id", "segment", _p_segment_id, _pl_segment_id),
    Op("groupby_transform", "groupby-transform", _p_gt_transform, _pl_gt_transform),
    Op("groupby_agg", "reduction", _p_gb_agg, _pl_gb_agg, sort_rows=True),
    Op("sort", "reshape", _p_sort, _pl_sort, sort_rows=True),
    Op("unique_rows", "dedup", _p_unique, _pl_unique, parity="shape"),
    Op("filter_mask", "filter", _p_filter, _pl_filter, sort_rows=True),
    Op("convert", "conversion", _p_to_polars, _pl_to_pandas, parity="none"),
]

OPS = {op.name: op for op in _OPS}
NAMES = tuple(OPS)


def get(name: str, engine: str) -> Callable:
    return getattr(OPS[name], engine)


def unit_class(name: str) -> str:
    return OPS[name].cls
