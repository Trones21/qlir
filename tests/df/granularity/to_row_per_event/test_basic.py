import pandas as pd

from qlir.df.granularity.to_row_per_event.to_row_per_event import to_row_per_event
from qlir.df.granularity.metric_spec import MetricSpec, Aggregation


def test_basic_event_aggregation():
    df = pd.DataFrame(
        {
            "leg_id": [1, 1, 1, 2, 2],
            "timestamp": [10, 11, 12, 20, 21],
            "mae": [1.0, 2.0, 4.0, 0.5, 3.5],
            "mae_gt_3": [False, False, True, False, True],
        }
    )

    metrics = [
        MetricSpec(col="timestamp", agg=Aggregation.MIN, out="start_ts"),
        MetricSpec(col="timestamp", agg=Aggregation.MAX, out="end_ts"),
        MetricSpec(col="mae", agg=Aggregation.MAX, out="max_mae"),
        MetricSpec(col="mae_gt_3", agg=Aggregation.COUNT_TRUE),
    ]

    out = to_row_per_event(
        df,
        event_id_col="leg_id",
        metrics=metrics,
        include_src_row_count=True,
    )

    assert list(out.columns) == [
        "leg_id",
        "src_row_count",
        "start_ts",
        "end_ts",
        "max_mae",
        "mae_gt_3_count_true",
    ]

    row1 = out[out["leg_id"] == 1].iloc[0]
    assert row1.src_row_count == 3
    assert row1.start_ts == 10
    assert row1.end_ts == 12
    assert row1.max_mae == 4.0
    assert row1.mae_gt_3_count_true == 1

    row2 = out[out["leg_id"] == 2].iloc[0]
    assert row2.src_row_count == 2
    assert row2.start_ts == 20
    assert row2.end_ts == 21
    assert row2.max_mae == 3.5
    assert row2.mae_gt_3_count_true == 1
