import pandas as pd
from qlir.features.boll.width import bb_width_pressure


def test_bb_width_pressure_basic():
    df = pd.DataFrame({
        "bb_width_step": [1, 1, 0, 1, -1, -1, -1]
    })

    out = bb_width_pressure(
        df,
        step_col="bb_width_step",
        m=3,
        k=2,
    )

    # up_ok: last 3 bars contain >=2 +1
    assert out["bb_width_up_ok"].tolist() == [
        False,  # insufficient history
        False,
        True,   # [1,1,0]
        True,   # [1,0,1]
        False,  # [0,1,-1]
        False,
        False,
    ]

    # dn_ok: last 3 bars contain >=2 -1
    assert out["bb_width_dn_ok"].tolist() == [
        False,
        False,
        False,
        False,
        False,
        True,   # [-1,-1,-1]
        True,
    ]


def test_bb_width_pressure_requires_full_window():
    df = pd.DataFrame({
        "bb_width_step": [1, 1, 1]
    })

    out = bb_width_pressure(
        df,
        m=5,
        k=3,
    )

    assert not out["bb_width_up_ok"].any()
    assert not out["bb_width_dn_ok"].any()


def test_bb_width_pressure_exact_k_of_m():
    df = pd.DataFrame({
        "bb_width_step": [1, 0, 1, 0, 1]
    })

    out = bb_width_pressure(
        df,
        m=5,
        k=3,
    )

    # exactly 3 positives in window
    assert out["bb_width_up_ok"].iloc[-1] is True
    assert out["bb_width_dn_ok"].iloc[-1] is False


def test_bb_width_pressure_zero_steps():
    df = pd.DataFrame({
        "bb_width_step": [0, 0, 0, 0, 0]
    })

    out = bb_width_pressure(df, m=3, k=2)

    assert not out["bb_width_up_ok"].any()
    assert not out["bb_width_dn_ok"].any()
