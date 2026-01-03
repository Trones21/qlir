import pandas as pd
import pytest

from qlir.df.distributions.condition_set_persistence import (
    condition_set_persistence_df,
)

def test_condition_set_persistence_basic():
    """
    Sequence:
        F T T F T F T T T F

    True runs:
        - length 2
        - length 1
        - length 3
    """

    df = pd.DataFrame(
        {
            "cond": [
                False,
                True,
                True,
                False,
                True,
                False,
                True,
                True,
                True,
                False,
            ]
        },
        index=pd.date_range("2024-01-01", periods=10, freq="T"),
    )

    result = condition_set_persistence_df(
        df=df,
        condition_col="cond",
        condition_set_name="test_condition",
    )

    # Expected run-length counts
    # lengths: [1, 2, 3] → counts all 1
    expected = pd.DataFrame(
        {
            "condition_set": ["test_condition"] * 3,
            "duration_bars": [1, 2, 3],
            "count": [1, 1, 1],
            "pct_of_groups": [1 / 3, 1 / 3, 1 / 3],
            "pct_of_groups_running": [1.0, 2 / 3, 1 / 3],
        }
    )

    # Sort for deterministic comparison
    result = result.sort_values("duration_bars").reset_index(drop=True)

    # Column presence
    assert list(result.columns) == list(expected.columns)

    # Exact matches where appropriate
    pd.testing.assert_series_equal(
        result["duration_bars"], expected["duration_bars"], check_dtype=False
    )
    pd.testing.assert_series_equal(
        result["count"], expected["count"], check_dtype=False
    )
    pd.testing.assert_series_equal(
        result["condition_set"], expected["condition_set"]
    )

    # Floating comparisons
    pd.testing.assert_series_equal(
        result["pct_of_groups"],
        expected["pct_of_groups"],
        check_exact=False,
        rtol=1e-8,
    )
    pd.testing.assert_series_equal(
        result["pct_of_groups_running"],
        expected["pct_of_groups_running"],
        check_exact=False,
        rtol=1e-8,
    )

    # Sanity checks
    assert abs(result["pct_of_groups"].sum() - 1.0) < 1e-12
    assert result["pct_of_groups_running"].iloc[0] == 1.0
    assert result["pct_of_groups_running"].is_monotonic_decreasing
