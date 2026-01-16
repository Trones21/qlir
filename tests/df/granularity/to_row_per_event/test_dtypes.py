import pandas as pd
import pytest

from qlir.df.granularity.to_row_per_event.to_row_per_event import to_row_per_event
from qlir.df.granularity.metric_spec import MetricSpec, Aggregation


def test_count_true_requires_boolean():
    df = pd.DataFrame(
        {
            "event": [1, 1, 2],
            "not_bool": [1, 0, 1],
        }
    )

    metrics = [
        MetricSpec(col="not_bool", agg=Aggregation.COUNT_TRUE),
    ]

    with pytest.raises(TypeError):
        to_row_per_event(df, event_id_col="event", metrics=metrics)


def test_median_requires_numeric():
    df = pd.DataFrame(
        {
            "event": [1, 1, 2],
            "x": ["a", "b", "c"],
        }
    )

    metrics = [
        MetricSpec(col="x", agg=Aggregation.MEDIAN),
    ]

    with pytest.raises(TypeError):
        to_row_per_event(df, event_id_col="event", metrics=metrics)


def test_min_max_allow_datetime():
    df = pd.DataFrame(
        {
            "event": [1, 1, 2],
            "ts": pd.to_datetime(
                ["2024-01-01", "2024-01-02", "2024-01-10"]
            ),
        }
    )

    metrics = [
        MetricSpec(col="ts", agg=Aggregation.MIN),
        MetricSpec(col="ts", agg=Aggregation.MAX),
    ]

    out = to_row_per_event(df, event_id_col="event", metrics=metrics)

    assert out["ts_min"].iloc[0] < out["ts_max"].iloc[0]
