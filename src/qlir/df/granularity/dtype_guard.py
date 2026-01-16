import pandas as pd
from pandas.api.types import (
    is_bool_dtype,
    is_numeric_dtype,
    is_datetime64_any_dtype,
    is_timedelta64_dtype,
)

from qlir.df.granularity.metric_spec import Aggregation, MetricSpec

def _validate_metric_dtype(
    df: pd.DataFrame,
    metric: MetricSpec,
) -> None:
    col = metric.col
    agg = metric.agg
    dtype = df[col].dtype

    if agg == Aggregation.COUNT_TRUE:
        if not is_bool_dtype(dtype):
            raise TypeError(
                f"Aggregation '{agg.value}' requires boolean column, "
                f"got '{col}' with dtype {dtype}"
            )

    elif agg in (Aggregation.MIN, Aggregation.MAX):
        if not (
            is_numeric_dtype(dtype)
            or is_datetime64_any_dtype(dtype)
            or is_timedelta64_dtype(dtype)
        ):
            raise TypeError(
                f"Aggregation '{agg.value}' requires numeric, datetime, "
                f"or timedelta column, got '{col}' with dtype {dtype}"
            )

    elif agg == Aggregation.MEDIAN:
        if not is_numeric_dtype(dtype):
            raise TypeError(
                f"Aggregation '{agg.value}' requires numeric column, "
                f"got '{col}' with dtype {dtype}"
            )

    elif agg == Aggregation.SUM:
        if not (is_numeric_dtype(dtype) or is_bool_dtype(dtype)):
            raise TypeError(
                f"Aggregation '{agg.value}' requires numeric or boolean column, "
                f"got '{col}' with dtype {dtype}"
            )

    elif agg in (Aggregation.FIRST, Aggregation.LAST):
        pass  # any dtype allowed

    else:
        raise AssertionError(f"Unhandled aggregation: {agg}")