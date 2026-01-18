import pandas as _pd

from qlir.data.lte.transform.gaps.materialization.materialize_missing_rows import (
    materialize_missing_rows,
)
from qlir.data.lte.transform.gaps.materialization.apply_fill_policy import apply_fill_policy
from qlir.data.lte.transform.policy.constant import ConstantFillPolicy
from qlir.data.lte.transform.gaps.materialization.markers import (
    ROW_MATERIALIZED_COL,
    SYNTHETIC_COL,
    FILL_POLICY_COL,
)


def test_end_to_end_constant_fill_policy():
    """
    End-to-end test covering:

    - wall-clock materialization
    - missing block detection
    - strict FillContext construction
    - constant fill policy
    - index-as-bar-open-time invariant
    """

    # ------------------------------------------------------------
    # Step 1: Build sparse input (missing middle candles)
    # ------------------------------------------------------------
    df = _pd.DataFrame(
        {
            "timestamp": [
                "2024-01-01 10:00:00",
                "2024-01-01 10:03:00",
            ],
            "open": [100.0, 103.0],
            "high": [102.0, 105.0],
            "low": [99.0, 102.0],
            "close": [101.0, 104.0],
        }
    )

    df["timestamp"] = _pd.to_datetime(df["timestamp"], utc=True)
    df = df.set_index("timestamp")

    # ------------------------------------------------------------
    # Step 2: Materialize missing wall-clock rows
    # ------------------------------------------------------------
    df = materialize_missing_rows(
        df,
        interval_s=60,
    )

    # Expect 4 rows total: 10:00, 10:01, 10:02, 10:03
    assert len(df) == 4

    # Missing rows should be marked
    assert df.loc["2024-01-01 10:01:00", ROW_MATERIALIZED_COL] ==  True
    assert df.loc["2024-01-01 10:02:00", ROW_MATERIALIZED_COL] == True
    assert df.loc["2024-01-01 10:00:00", ROW_MATERIALIZED_COL] == False
    assert df.loc["2024-01-01 10:03:00", ROW_MATERIALIZED_COL] ==  False

    # ------------------------------------------------------------
    # Step 3: Apply constant fill policy
    # ------------------------------------------------------------
    df = apply_fill_policy(
        df,
        interval_s=60,
        policy=ConstantFillPolicy(),
    )

    # ------------------------------------------------------------
    # Step 4: Validate filled values
    # ------------------------------------------------------------
    # Filled rows should carry forward previous close (101)
    for ts in ["2024-01-01 10:01:00", "2024-01-01 10:02:00"]:
        row = df.loc[ts]
        assert row["open"] == 101.0
        assert row["high"] == 101.0
        assert row["low"] == 101.0
        assert row["close"] == 101.0

        assert row[SYNTHETIC_COL] == True
        assert row[FILL_POLICY_COL] == "constant"

    # Real rows must remain untouched
    assert df.loc["2024-01-01 10:00:00", "close"] == 101.0
    assert df.loc["2024-01-01 10:03:00", "open"] == 103.0

    # ------------------------------------------------------------
    # Step 5: Index semantics sanity check
    # ------------------------------------------------------------
    assert isinstance(df.index, _pd.DatetimeIndex)
    assert df.index.tz is not None
