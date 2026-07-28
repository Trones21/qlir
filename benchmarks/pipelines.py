"""
Matched pandas / polars analysis pipelines for benchmarking.

Each pipeline is implemented twice with equivalent semantics, so we only ever
compare timings of implementations that produce the same answer (guarded by
tests/benchmarks/test_pipeline_parity.py). They are deliberately simple but
represent distinct computational profiles:

  - indicators: rolling windows + ewm (indicator computation)
  - events:     condition -> segment id -> groupby aggregates + run length
                (event/segment analysis; the shape polars tends to win on)

Functions are named `<pipeline>_<engine>` so the worker can resolve them by name.
"""
from __future__ import annotations

import pandas as pd
import polars as pl

WINDOW = 20
NUM_STD = 2.0

PIPELINES = ("indicators", "events")


# ---------------------------------------------------------------------------
# indicators: rolling mean/std + ewm
# ---------------------------------------------------------------------------

def indicators_pandas(df: pd.DataFrame) -> pd.DataFrame:
    close = df["close"]
    sma = close.rolling(WINDOW).mean()
    std = close.rolling(WINDOW).std()
    out = pd.DataFrame(
        {
            "sma": sma,
            "ema": close.ewm(span=WINDOW, adjust=False).mean(),
            "bb_std": std,
            "bb_upper": sma + NUM_STD * std,
            "bb_lower": sma - NUM_STD * std,
        }
    )
    return out


def indicators_polars(df: pl.DataFrame) -> pl.DataFrame:
    return df.select(
        pl.col("close").rolling_mean(WINDOW).alias("sma"),
        pl.col("close").ewm_mean(span=WINDOW, adjust=False).alias("ema"),
        pl.col("close").rolling_std(WINDOW).alias("bb_std"),
    ).with_columns(
        (pl.col("sma") + NUM_STD * pl.col("bb_std")).alias("bb_upper"),
        (pl.col("sma") - NUM_STD * pl.col("bb_std")).alias("bb_lower"),
    )


# ---------------------------------------------------------------------------
# events: condition -> segment id -> per-segment run length + range
# ---------------------------------------------------------------------------

def events_pandas(df: pd.DataFrame) -> pd.DataFrame:
    close = df["close"]
    sma = close.rolling(WINDOW).mean()
    cond = (close > sma).fillna(False)
    seg = (cond != cond.shift(fill_value=False)).cumsum()
    out = pd.DataFrame({"seg": seg, "cond": cond})
    out["run_len"] = out.groupby("seg").cumcount() + 1
    # Vectorized (Cython) transforms, not a Python lambda — the idiomatic/fair
    # pandas way to compute a per-group range.
    grp = close.groupby(seg)
    out["seg_range"] = grp.transform("max") - grp.transform("min")
    return out[["seg", "cond", "run_len", "seg_range"]]


def events_polars(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.with_columns(pl.col("close").rolling_mean(WINDOW).alias("sma"))
        .with_columns((pl.col("close") > pl.col("sma")).fill_null(False).alias("cond"))
        .with_columns(
            (pl.col("cond") != pl.col("cond").shift(fill_value=False)).cum_sum().alias("seg")
        )
        .with_columns(
            (pl.int_range(pl.len()).over("seg") + 1).alias("run_len"),
            (pl.col("close").max().over("seg") - pl.col("close").min().over("seg")).alias(
                "seg_range"
            ),
        )
        .select("seg", "cond", "run_len", "seg_range")
    )


NAMES = PIPELINES


def get(pipeline: str, engine: str):
    return globals()[f"{pipeline}_{engine}"]


def unit_class(name: str) -> str:
    return "pipeline"
