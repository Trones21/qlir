import pandas as pd
import pytest

from qlir.df.condition_set.construction import (
    all_of,
    any_of,
    at_least_k_of,
    negate,
)


def test_all_of_basic():
    df = pd.DataFrame({
        "a": [True, True, False],
        "b": [True, False, False],
    })

    out, col = all_of(df, ["a", "b"], out_col="all_ab")

    assert col == "all_ab"
    assert out["all_ab"].tolist() == [True, False, False]


def test_all_of_missing_column_raises():
    df = pd.DataFrame({"a": [True, False]})

    with pytest.raises(KeyError):
        all_of(df, ["a", "b"], out_col="x")


def test_all_of_non_boolean_raises():
    df = pd.DataFrame({
        "a": [1, 0, 1],
        "b": [True, False, True],
    })

    with pytest.raises(TypeError):
        all_of(df, ["a", "b"], out_col="x")

def test_all_of_with_nan_raises():
    df = pd.DataFrame({
        "a": [True, None, False],
        "b": [True, True, True],
    })

    with pytest.raises(ValueError):
        all_of(df, ["a", "b"], out_col="x")



def test_any_of_basic():
    df = pd.DataFrame({
        "a": [False, False, True],
        "b": [False, True, False],
    })

    out, col = any_of(df, ["a", "b"], out_col="any_ab")

    assert out["any_ab"].tolist() == [False, True, True]



def test_at_least_k_of_basic():
    df = pd.DataFrame({
        "a": [True, False, True],
        "b": [True, True, False],
        "c": [False, False, True],
    })

    out, col = at_least_k_of(df, ["a", "b", "c"], k=2, out_col="k2")

    assert out["k2"].tolist() == [True, False, True]



def test_at_least_k_of_k_too_large():
    df = pd.DataFrame({
        "a": [True],
        "b": [False],
    })

    with pytest.raises(ValueError):
        at_least_k_of(df, ["a", "b"], k=3, out_col="x")



def test_at_least_k_of_k_zero_raises():
    df = pd.DataFrame({"a": [True]})

    with pytest.raises(ValueError):
        at_least_k_of(df, ["a"], k=0, out_col="x")



def test_negate_basic():
    df = pd.DataFrame({
        "a": [True, False, True],
    })

    out, col = negate(df, "a", out_col="not_a")

    assert out["not_a"].tolist() == [False, True, False]



def test_out_col_collision_raises():
    df = pd.DataFrame({
        "a": [True, False],
        "b": [True, True],
        "x": [False, False],
    })

    with pytest.raises(ValueError):
        all_of(df, ["a", "b"], out_col="x")



def test_non_strict_allows_nullable_boolean():
    df = pd.DataFrame({
        "a": pd.Series([True, None, False], dtype="boolean"),
        "b": pd.Series([True, True, True], dtype="boolean"),
    })

    out, _ = all_of(df, ["a", "b"], out_col="x", strict=False)

    assert out["x"].isna().any()
