import pandas as pd
import pytest

from qlir.df.granularity.to_row_per_event.to_row_per_event import to_row_per_event
from qlir.df.granularity.metric_spec import MetricSpec, Aggregation


def test_missing_event_id_column():
    df = pd.DataFrame({"x": [1, 2, 3]})

    with pytest.raises(KeyError):
        to_row_per_event(
            df,
            event_id_col="event",
            metrics=[MetricSpec(col="x", agg=Aggregation.MAX)],
        )


def test_missing_metric_column():
    df = pd.DataFrame({"event": [1, 1, 2]})

    with pytest.raises(KeyError):
        to_row_per_event(
            df,
            event_id_col="event",
            metrics=[MetricSpec(col="nope", agg=Aggregation.MAX)],
        )


def test_duplicate_output_column_names():
    df = pd.DataFrame(
        {
            "event": [1, 1, 2],
            "x": [1, 2, 3],
        }
    )

    metrics = [
        MetricSpec(col="x", agg=Aggregation.MIN, out="dup"),
        MetricSpec(col="x", agg=Aggregation.MAX, out="dup"),
    ]

    with pytest.raises(ValueError):
        to_row_per_event(df, event_id_col="event", metrics=metrics)
