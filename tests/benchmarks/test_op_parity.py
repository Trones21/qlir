"""
Guard that each op's pandas and polars implementations do the same work, so
their timings are comparable. Uses an engine-structure-agnostic fingerprint
(nan-sum of numeric output + row count) so Series-vs-DataFrame and group-order
differences don't matter.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

pl = pytest.importorskip("polars")

from benchmarks import ops
from benchmarks.gen_data import generate


def _fingerprint(x) -> tuple[float, int]:
    if isinstance(x, (pl.DataFrame, pl.Series)):
        x = x.to_pandas()
    if isinstance(x, pd.Series):
        x = x.to_frame()
    num = x.select_dtypes("number").to_numpy(dtype="float64")
    return float(np.nansum(num)), int(len(x))


COMPARABLE = [n for n in ops.NAMES if ops.OPS[n].parity != "none"]


@pytest.mark.parametrize("name", COMPARABLE)
def test_op_parity(name):
    op = ops.OPS[name]
    pdf = generate(5_000, seed=2)

    a_sum, a_n = _fingerprint(op.pandas(pdf))
    b_sum, b_n = _fingerprint(op.polars(pl.from_pandas(pdf)))

    assert a_n == b_n, f"{name}: row count {a_n} vs {b_n}"
    if op.parity == "values":
        assert np.isclose(a_sum, b_sum, rtol=1e-6, atol=1e-6), f"{name}: sum {a_sum} vs {b_sum}"
