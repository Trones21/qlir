import pandas as _pd
import pytest

from qlir.data.lte.transform.gaps.materialization.materialize_missing_rows import materialize_missing_rows


def test_materialize_missing_rows_wall_clock_minutes():
    """
    Missing wall-clock minutes should be materialized and marked.
    Existing rows must remain untouched.
    """

    # 12:00, 12:01, 12:05
    # Missing: 12:02, 12:03, 12:04
    idx = _pd.to_datetime(
        [
            "2024-01-01 12:00:00",
            "2024-01-01 12:01:00",
            "2024-01-01 12:05:00",
        ]
    )

    df = _pd.DataFrame(
        {
            "open": [100.0, 101.0, 105.0],
            "close": [100.5, 101.5, 105.5],
        },
        index=idx,
    )

    out = materialize_missing_rows(df, interval_s=60)

    expected_index = _pd.date_range(
        start="2024-01-01 12:00:00",
        end="2024-01-01 12:05:00",
        freq="1min",
    )

    assert out.index.equals(expected_index)
    assert "__row_materialized" in out.columns

    materialized = out["__row_materialized"]

    # Original rows
    assert materialized.loc["2024-01-01 12:00:00"] == False
    assert materialized.loc["2024-01-01 12:01:00"] == False
    assert materialized.loc["2024-01-01 12:05:00"] == False

    # Materialized rows
    assert materialized.loc["2024-01-01 12:02:00"] == True
    assert materialized.loc["2024-01-01 12:03:00"] == True
    assert materialized.loc["2024-01-01 12:04:00"] == True

    # No value filling occurs
    assert _pd.isna(out.loc["2024-01-01 12:02:00", "open"])
    assert _pd.isna(out.loc["2024-01-01 12:03:00", "close"])

    # Existing values unchanged
    assert out.loc["2024-01-01 12:00:00", "open"] == 100.0
    assert out.loc["2024-01-01 12:05:00", "close"] == 105.5


def test_materialize_empty_dataframe_returns_copy():
    df = _pd.DataFrame(columns=["open", "close"])
    out = materialize_missing_rows(df, interval_s=60)

    assert out.empty


def test_materialize_no_gaps_marks_all_false():
    idx = _pd.date_range(
        start="2024-01-01 12:00:00",
        periods=3,
        freq="1min",
    )

    df = _pd.DataFrame({"open": [1, 2, 3]}, index=idx)

    out = materialize_missing_rows(df, interval_s=60)

    assert out.index.equals(idx)
    assert out["__row_materialized"].eq(False).all()


def test_materialize_strict_requires_monotonic_index():
    idx = _pd.to_datetime(
        [
            "2024-01-01 12:01:00",
            "2024-01-01 12:00:00",
        ]
    )

    df = _pd.DataFrame({"open": [1, 2]}, index=idx)

    with pytest.raises(ValueError):
        materialize_missing_rows(df, interval_s=60, strict=True)
