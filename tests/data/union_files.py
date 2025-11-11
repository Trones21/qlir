import pytest
pytestmark = pytest.mark.local

import pandas as pd
from pathlib import Path
import logging

from qlir.utils.logdf import logdf
log = logging.getLogger(__name__)

# this is your actual function
from qlir.io.union_files import union_file_datasets


def test_union_csvs_with_different_columns(tmp_path: Path):
    """
    Create two CSVs with the same extension (so the dir is homogeneous),
    but with different columns.

    We want to make sure the returned DataFrame has the union of columns,
    and rows line up the way pandas concat does (missing cols -> NaN).
    """
    data_dir = tmp_path / "csvs"
    data_dir.mkdir()

    df1 = pd.DataFrame(
        {
            "id": [1],
            "col_a": ["foo"],
        }
    )
    df2 = pd.DataFrame(
        {
            "id": [2],
            "col_b": ["bar"],
        }
    )

    df1.to_csv(data_dir / "part1.csv", index=False)
    df2.to_csv(data_dir / "part2.csv", index=False)

    # call your real implementation
    result = union_file_datasets(str(data_dir))

    # we should have 2 rows (1 from each file)
    assert len(result) == 2

    # the columns should be the union of the two file schemas
    assert set(result.columns) == {"id", "col_a", "col_b"}

    # Sort by id before asserting (otherwise row order is not guaranteed since union func doesnt handle this)
    result = result.sort_values("id").reset_index(drop=True)
    
    # first row came from df1
    r1 = result.iloc[0]
    logdf(result)
    assert r1["id"] == 1
    assert r1["col_a"] == "foo"
    # this column didn't exist in df1, so should be NaN
    assert pd.isna(r1["col_b"])

    # second row came from df2
    r2 = result.iloc[1]
    assert r2["id"] == 2
    assert r2["col_b"] == "bar"
    # this column didn't exist in df2, so should be NaN
    assert pd.isna(r2["col_a"])


def test_union_parquet_fast_path(tmp_path: Path):
    """
    Create two parquet files and make sure we hit the parquet path.
    Note: The function uses pyarrow.dataset(...) when the dir is homogeneous parquet.
    """
    data_dir = tmp_path / "parquet"
    data_dir.mkdir()

    df1 = pd.DataFrame({"id": [1], "val": [10]})
    df2 = pd.DataFrame({"id": [2], "val": [20]})

    df1.to_parquet(data_dir / "part1.parquet", index=False)
    df2.to_parquet(data_dir / "part2.parquet", index=False)

    result = union_file_datasets(str(data_dir))

    # should have both rows
    assert len(result) == 2
    assert set(result.columns) == {"id", "val"}

    # order isn't super important, but let's just check contents
    # turn DataFrame into set of tuples
    rows = {tuple(x) for x in result[["id", "val"]].to_numpy().tolist()}
    assert rows == {(1, 10), (2, 20)}
