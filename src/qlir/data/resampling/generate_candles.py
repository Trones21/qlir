
from qlir.core.types.named_df import NamedDF
from qlir.time.timefreq import TimeFreq, TimeUnit 
from qlir.data.quality.candles.candles import infer_freq, ensure_homogeneous_candle_size, detect_missing_candles
import pandas as _pd
from typing import Iterable, Dict

import pandas as _pd
from typing import Iterable, Dict
from dataclasses import dataclass

from qlir.df.utils import materialize_index, insert_column, move_column
from qlir.time.ensure_utc import ensure_utc_series_strict
from qlir.logging.logdf import logdf

import logging 

log = logging.getLogger(__name__)

def _generate(
    df: _pd.DataFrame,
    *,
    dataset_tf: TimeFreq,
    out_unit: TimeUnit,
    out_candle_sizes: Iterable[int],
    dt_col: str = "tz_start",
) -> Dict[str, _pd.DataFrame]:
    """
    Core resampling routine that uses the QLIRs TimeFreq/infer_freq
    instead of pandas' frequency inference.

    Parameters
    ----------
    df : _pd.DataFrame
        Input dataframe with homogeneous time-based rows.
    inferred_tf : TimeFreq
        Result of your infer_freq(df), describes the base cadence (e.g. 1 minute).
    out_unit : TimeUnit
        Target unit: "second" | "minute" | "hour" | "day".
    counts : iterable[int]
        Multipliers of the out_unit to generate.
    dt_col : str
        Name of the datetime column to index by (defaults to your "tz_start").

    Returns
    -------
    dict[str, _pd.DataFrame]
        Keys are pandas-style frequency strings (e.g. "7min", "4H").
    """

    # ensure datetime index
    if df.index.name != dt_col:
        log.info(f"Setting index to datetime column '{dt_col}' with UTC normalization")
        df[dt_col] = ensure_utc_series_strict(df[dt_col])
        df = df.set_index(dt_col)
    df = df.sort_index()

    # standard OHLCV aggregation
    ohlc_map = {
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "sum",
    }

    # map TimeFreq unit strings to pandas suffixes
    unit_to_symbol = {
        "second": "S",
        "minute": "min",
        "hour": "h",
        "day": "D",
    }

    if out_unit.value not in unit_to_symbol:
        raise ValueError(f"Unsupported out_unit: {out_unit.value}")

    out: Dict[str, _pd.DataFrame] = {}

    for size in out_candle_sizes:
        freq_str = f"{size}{unit_to_symbol[out_unit.value]}"

        candles = (
            df
            .resample(freq_str)
            .agg(ohlc_map)
            .dropna(how="any")
        )

        # materialize the timestamp column 
        candles = materialize_index(candles, dt_col)
        
        # add metadata columns so downstream knows what happened
        candles["meta__candle_freq"] = freq_str
        candles["meta__derived_from_freq"] = dataset_tf.as_pandas_str  # e.g. "1min"
        
        candles = move_column(candles, "meta__candle_freq", 0)
        candles = move_column(candles, "meta__derived_from_freq", 0)

        out[freq_str] = candles

    return out


def generate_candles_from_1m(
    df,
    *,
    out_unit: TimeUnit,
    out_agg_candle_sizes: Iterable[int],
    dt_col: str = "tz_start",
):
    freq: TimeFreq | None = infer_freq(df)

    if freq is None:
        logdf(NamedDF(df=df, name="infer_freq_failure"), level="critical")
        raise ValueError("candle_quality.infer_freq returned None, check the dataset you passed")

    if freq.count != 1 or freq.unit != TimeUnit.MINUTE:
        logdf(NamedDF(df=df, name="infer_freq_failure"), level="critical")
        raise ValueError(f"Incorrect dataset frequency. Expected 1 minute data, got (freq.to_dict() here): {freq.to_dict()} ")
    
    gaps = detect_missing_candles(df, freq)
    if gaps:
        logdf(NamedDF(df=df, name="canlde_gaps_failure"), level="critical")
        raise ValueError({"message":"Found gaps in 1 minute data", "gaps": gaps})

    ensure_homogeneous_candle_size(df)

    return _generate(df, dataset_tf=TimeFreq(1, TimeUnit.MINUTE), out_unit=out_unit, out_candle_sizes=out_agg_candle_sizes, dt_col=dt_col)


def generate_candles(
    df,
    *,
    in_unit: TimeFreq,
    out_unit: TimeUnit,
    counts: Iterable[int],
    dt_col: str = "tz_start",
):
    freq = infer_freq(df)
    
    if freq is None:
        logdf(NamedDF(df=df, name="infer_freq_failure"), level="critical")
        raise ValueError("candle_quality.infer_freq returned None, check the dataset you passed")
    
    # Check that dataset candle size passed matches the in_unit passed 
    if freq.count != in_unit.count or freq.unit != in_unit.unit:
        logdf(NamedDF(df=df, name="infer_freq_failure"), level="critical")
        raise ValueError(f"input freq mismatch: inferred {freq}, got arg {in_unit}")

    gaps = detect_missing_candles(df, freq)
    if gaps:
        logdf(NamedDF(df=df, name="candle_gaps_failure"), level="critical")
        raise ValueError({"message":"Found gaps in {in_unit} data", "number_of_gaps": len(gaps), "gaps": gaps})

    # Final Data Quality Check - 
    # Passed after detect candle gaps b/c if passed before, 
    # then this would actually raise a Value error on the first "gap" and make the call to detect candle gaps useless/non-functional
    # which isnt the best UX 
    ensure_homogeneous_candle_size(df)

    return _generate(df, dataset_tf=in_unit, out_unit=out_unit, out_candle_sizes=counts, dt_col=dt_col)
