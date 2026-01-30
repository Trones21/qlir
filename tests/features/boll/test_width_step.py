import pandas as pd
import pytest

from qlir.features.boll.width import bb_width_step


def test_bb_width_step_basic():
    df = pd.DataFrame({
        "bb_width_dw_bps": [-5, -1.5, -0.5, 0.0, 0.4, 1.2, 3.0]
    })

    out = bb_width_step(
        df,
        dw_bps_col="bb_width_dw_bps",
        eps_bps=1.0,
    )

    assert out["bb_width_step"].tolist() == [
        -1,  # -5
        -1,  # -1.5
         0,  # -0.5
         0,  #  0.0
         0,  #  0.4
         1,  #  1.2
         1,  #  3.0
    ]


def test_bb_width_step_exact_threshold():
    df = pd.DataFrame({
        "bb_width_dw_bps": [-1.0, 1.0]
    })

    out = bb_width_step(
        df,
        dw_bps_col="bb_width_dw_bps",
        eps_bps=1.0,
    )

    # threshold is inclusive
    assert out["bb_width_step"].tolist() == [-1, 1]


def test_bb_width_step_all_noise():
    df = pd.DataFrame({
        "bb_width_dw_bps": [-0.2, 0.1, 0.0, 0.3, -0.4]
    })

    out = bb_width_step(
        df,
        dw_bps_col="bb_width_dw_bps",
        eps_bps=1.0,
    )

    assert out["bb_width_step"].eq(0).all()


def test_bb_width_step_dtype_and_index():
    df = pd.DataFrame(
        {"bb_width_dw_bps": [2.0, -2.0]},
        index=["a", "b"],
    )

    out = bb_width_step(df, eps_bps=1.0)

    assert out["bb_width_step"].dtype == "int8"
    assert out.index.equals(df.index)
