# test_macd_full_pyramidal_annotation.py
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest


def _df(hist, gid, index=None):
    if index is None:
        index = list(range(len(hist)))
    return pd.DataFrame({"hist": hist, "gid": gid}, index=pd.Index(index, name="idx"))


def _declared_columns(registry) -> list[str]:
    # ColRegistry.items() -> (key, ColKeyDecl)
    return [decl.column for _, decl in registry.items()]


def _declared_keys(registry) -> set[str]:
    return set(registry.keys())


def test_returns_annotateddf_and_registry_is_consistent():
    import qlir.features.macd.histogram_pyramid as m

    df = _df(hist=[-1, -2, -4, -3], gid=[1, 1, 1, 1])
    res = m.macd_full_pyramidal_annotation(df, hist_col="hist", group_col="gid", out_prefix="pyr_")

    # AnnotatedDF contract
    assert hasattr(res, "df")
    assert hasattr(res, "new_cols")
    assert getattr(res, "label", None) == "macd_full_pyramidal_annotation"

    out = res.df
    reg = res.new_cols

    # registry has expected keys
    expected_keys = {
        "pyr_apex_idx",
        "pyr_apex_val",
        "pyr_is_front",
        "pyr_is_back",
        "pyr_ord",
        "pyr_front_ord",
        "pyr_front_len",
        "pyr_back_ord",
        "pyr_back_len",
        "pyr_viol_front",
        "pyr_viol_back",
    }
    assert _declared_keys(reg) == expected_keys

    # Every declared column exists in df.columns
    declared_cols = _declared_columns(reg)
    missing = [c for c in declared_cols if c not in out.columns]
    assert not missing, f"declared columns missing from df: {missing}"

    # Derived convenience columns exist (not necessarily in registry)
    assert "pyr_viol_any" in out.columns
    assert "pyr_viol_total" in out.columns


def test_apex_identification_and_broadcast():
    import qlir.features.macd.histogram_pyramid as m

    # group 1: abs max at idx=2 (|-3|=3)
    # group 2: abs max at idx=6 (|5|=5)
    df = _df(
        hist=[-1, -2, -3, -2,  1,  2,  5,  4],
        gid =[ 1,  1,  1,  1,  2,  2,  2,  2],
    )
    out = m.macd_full_pyramidal_annotation(df, hist_col="hist", group_col="gid").df

    assert (out.loc[out.gid == 1, "pyr_apex_idx"] == 2).all()
    assert (out.loc[out.gid == 1, "pyr_apex_val"] == 3).all()

    assert (out.loc[out.gid == 2, "pyr_apex_idx"] == 6).all()
    assert (out.loc[out.gid == 2, "pyr_apex_val"] == 5).all()


def test_front_back_classification():
    import qlir.features.macd.histogram_pyramid as m

    # group 1 apex at idx=2
    df = _df(hist=[-1, -2, -4, -3, -2], gid=[1, 1, 1, 1, 1])
    out = m.macd_full_pyramidal_annotation(df, hist_col="hist", group_col="gid").df

    # idx 0,1 => front; idx 3,4 => back; idx 2 => apex => neither
    assert bool(out.loc[0, "is_pyr_front"]) is True
    assert bool(out.loc[1, "is_pyr_front"]) is True
    assert bool(out.loc[2, "is_pyr_front"]) is False

    assert bool(out.loc[3, "is_pyr_back"]) is True
    assert bool(out.loc[4, "is_pyr_back"]) is True
    assert bool(out.loc[2, "is_pyr_back"]) is False


def test_ordinals_entire_front_back():
    import qlir.features.macd.histogram_pyramid as m

    # Apex at idx=2 (abs max 4)
    df = _df(hist=[-1, -2, -4, -3, -2], gid=[1, 1, 1, 1, 1])
    out = m.macd_full_pyramidal_annotation(df, hist_col="hist", group_col="gid").df

    # Entire pyramid ord should be 0..4
    assert out["pyr_ord"].tolist() == [0, 1, 2, 3, 4]

    # Front side: idx 0,1 => front_ord 0,1 and front_len 2
    assert out.loc[0, "pyr_front_ord"] == 0
    assert out.loc[1, "pyr_front_ord"] == 1
    assert out.loc[0, "pyr_front_len"] == 2
    assert out.loc[1, "pyr_front_len"] == 2

    # Apex row: NaN front/back ord and len 0
    assert np.isnan(out.loc[2, "pyr_front_ord"])
    assert np.isnan(out.loc[2, "pyr_back_ord"])
    assert out.loc[2, "pyr_front_len"] == 0
    assert out.loc[2, "pyr_back_len"] == 0

    # Back side: idx 3,4 => back_ord 0,1 and back_len 2
    assert out.loc[3, "pyr_back_ord"] == 0
    assert out.loc[4, "pyr_back_ord"] == 1
    assert out.loc[3, "pyr_back_len"] == 2
    assert out.loc[4, "pyr_back_len"] == 2


def test_violations_front_side():
    import qlir.features.macd.histogram_pyramid as m

    # Front expectation: abs(hist) should be non-decreasing.
    # abs on front: [1, 3, 2] -> decrease at 3rd front element => violation True there.
    # apex later at idx=3 (abs 5)
    df = _df(hist=[-1, -3, -2, -5, -4], gid=[1, 1, 1, 1, 1])
    out = m.macd_full_pyramidal_annotation(df, hist_col="hist", group_col="gid").df

    assert bool(out.loc[0, "pyr_viol_front"]) is False
    assert bool(out.loc[1, "pyr_viol_front"]) is False
    assert bool(out.loc[2, "pyr_viol_front"]) is True

    # derived convenience
    assert bool(out.loc[2, "pyr_viol_any"]) is True
    assert int(out.loc[2, "pyr_viol_total"]) == 1


def test_violations_back_side():
    import qlir.features.macd.histogram_pyramid as m

    # Back expectation: abs(hist) should be non-increasing away from apex.
    # abs on back: [3, 4] increases => violation True on the second back row.
    df = _df(hist=[-1, -2, -5, -3, -4], gid=[1, 1, 1, 1, 1])
    out = m.macd_full_pyramidal_annotation(df, hist_col="hist", group_col="gid").df

    assert bool(out.loc[3, "pyr_viol_back"]) is False
    assert bool(out.loc[4, "pyr_viol_back"]) is True

    # derived convenience
    assert bool(out.loc[4, "pyr_viol_any"]) is True
    assert int(out.loc[4, "pyr_viol_total"]) == 1


def test_open_last_segment_apex_can_move_next_iteration():
    import qlir.features.macd.histogram_pyramid as m

    # Iteration 1: abs [3,4,5,2] => apex idx=2; idx=3 becomes back with back_ord=0; no back violations (single)
    df1 = _df(hist=[-3, -4, -5, -2], gid=[1, 1, 1, 1])
    out1 = m.macd_full_pyramidal_annotation(df1, hist_col="hist", group_col="gid").df

    assert (out1["pyr_apex_idx"] == 2).all()
    assert bool(out1.loc[3, "is_pyr_back"]) is True
    assert out1.loc[3, "pyr_back_ord"] == 0
    assert bool(out1.loc[3, "pyr_viol_any"]) is False
    assert int(out1.loc[3, "pyr_viol_total"]) == 0

    # Iteration 2: new max at idx=4 => apex shifts; idx=3 flips back to front; now 5->2 decrease => front violation at idx=3
    df2 = _df(hist=[-3, -4, -5, -2, -6], gid=[1, 1, 1, 1, 1])
    out2 = m.macd_full_pyramidal_annotation(df2, hist_col="hist", group_col="gid").df

    assert (out2["pyr_apex_idx"] == 4).all()
    assert bool(out2.loc[3, "is_pyr_front"]) is True
    assert bool(out2.loc[3, "is_pyr_back"]) is False
    assert bool(out2.loc[3, "pyr_viol_front"]) is True
    assert bool(out2.loc[3, "pyr_viol_any"]) is True
    assert int(out2.loc[3, "pyr_viol_total"]) == 1


def test_trigger_first_backside_clean_example():
    import qlir.features.macd.histogram_pyramid as m

    # abs [3,4,5,2] => apex idx=2, idx=3 is first back row and should be clean
    df = _df(hist=[-3, -4, -5, -2], gid=[1, 1, 1, 1])
    out = m.macd_full_pyramidal_annotation(df, hist_col="hist", group_col="gid").df

    first_backside_clean = (
        out["is_pyr_back"]
        & (out["pyr_back_ord"] == 0)
        & (~out["pyr_viol_any"])
    )

    assert int(first_backside_clean.sum()) == 1
    assert bool(first_backside_clean.loc[3]) is True