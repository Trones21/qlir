import pandas as pd
import numpy as np
import pytest

from qlir.column_bundles.excursion import excursion
from qlir.core.types.direction import Direction
from qlir.core.types.excursion_type import ExcursionType

@pytest.fixture
def df_up_legs():
    """
    Two UP legs with deterministic excursion behavior.

    Leg 1: 3 bars, max excursion at idx=2
    Leg 2: 2 bars, max excursion at idx=1
    """
    return pd.DataFrame({
        "open_sma_14_up_leg_id": [1, 1, 1, 2, 2],
        "open_sma_14_down_leg_id": [np.nan] * 5,
        "open": [100, 100, 100, 200, 200],
        "high": [101, 103, 105, 201, 205],
        "low":  [99,  99,  99,  198, 198],
    })


def test_excursion_creates_expected_columns(df_up_legs):
    prefix = "open_sma_14"

    out = excursion(
        df=df_up_legs,
        trendname_or_col_prefix=prefix,
        direction=Direction.UP,
        mae_or_mfe=ExcursionType.MFE,
    )

    base = f"{prefix}_up_mfe"

    expected_cols = {
        "open_sma_14_up_leg_id",
        "open",
        "high",
        "low",
        f"{base}_intra_leg_idx",
        f"{base}_leg_of_n_bars",
        f"{base}_grp_1st_open",
        f"{base}_exc",
        f"{base}_exc_bps",
        f"is_{base}_row",
    }

    missing = expected_cols - set(out.columns)
    assert not missing, f"Missing columns: {missing}"


def test_intra_leg_idx_starts_at_zero_and_increments(df_up_legs):
    prefix = "open_sma_14"
    base = f"{prefix}_up_mfe"

    out = excursion(
        df=df_up_legs,
        trendname_or_col_prefix=prefix,
        direction=Direction.UP,
        mae_or_mfe=ExcursionType.MFE,
    )

    for leg_id, g in out.groupby("open_sma_14_up_leg_id"):
        idx = g[f"{base}_intra_leg_idx"].to_numpy()
        assert idx[0] == 0
        assert np.all(idx == np.arange(len(idx)))


def test_leg_of_n_bars_matches_group_size(df_up_legs):
    prefix = "open_sma_14"
    base = f"{prefix}_up_mfe"

    out = excursion(
        df=df_up_legs,
        trendname_or_col_prefix=prefix,
        direction=Direction.UP,
        mae_or_mfe=ExcursionType.MFE,
    )

    for leg_id, g in out.groupby("open_sma_14_up_leg_id"):
        n = len(g)
        assert (g[f"{base}_leg_of_n_bars"] == n).all()


def test_exactly_one_excursion_row_per_leg(df_up_legs):
    prefix = "open_sma_14"
    base = f"{prefix}_up_mfe"

    out = excursion(
        df=df_up_legs,
        trendname_or_col_prefix=prefix,
        direction=Direction.UP,
        mae_or_mfe=ExcursionType.MFE,
    )

    counts = (
        out.groupby("open_sma_14_up_leg_id")[f"is_{base}_row"]
        .sum()
    )

    assert (counts == 1).all()

def test_excursion_row_matches_max_excursion(df_up_legs):
    prefix = "open_sma_14"
    base = f"{prefix}_up_mfe"

    out = excursion(
        df=df_up_legs,
        trendname_or_col_prefix=prefix,
        direction=Direction.UP,
        mae_or_mfe=ExcursionType.MFE,
    )

    for leg_id, g in out.groupby("open_sma_14_up_leg_id"):
        max_idx = g[f"{base}_exc_bps"].idxmax()
        marked_idx = g.index[g[f"is_{base}_row"]][0]
        assert max_idx == marked_idx


def test_up_direction_ignores_down_legs(df_up_legs):
    prefix = "open_sma_14"

    out = excursion(
        df=df_up_legs,
        trendname_or_col_prefix=prefix,
        direction=Direction.UP,
        mae_or_mfe=ExcursionType.MFE,
    )

    # DOWN leg ids should not appear
    assert out["open_sma_14_up_leg_id"].notna().all()
