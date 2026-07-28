"""
Guard that each benchmark pipeline's pandas and polars implementations produce
the same result. A faster wrong answer is not a win, so timings are only
meaningful if this passes.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

pl = pytest.importorskip("polars")  # skip if the bench extra isn't installed

from benchmarks import pipelines
from benchmarks.gen_data import generate


@pytest.mark.parametrize("name", pipelines.PIPELINES)
def test_pandas_polars_parity(name):
    pdf = generate(5_000, seed=1)
    pl_df = pl.from_pandas(pdf)

    got_pd = pipelines.get(name, "pandas")(pdf).reset_index(drop=True)
    got_pl = pipelines.get(name, "polars")(pl_df).to_pandas()

    assert list(got_pd.columns) == list(got_pl.columns)

    for col in got_pd.columns:
        a = got_pd[col].to_numpy()
        b = got_pl[col].to_numpy()
        if np.issubdtype(a.dtype, np.floating):
            np.testing.assert_allclose(a, b, rtol=1e-9, atol=1e-9, equal_nan=True,
                                       err_msg=f"{name}.{col} differs")
        else:
            # ints / bools: exact
            assert np.array_equal(a.astype("float64"), b.astype("float64")), f"{name}.{col} differs"
