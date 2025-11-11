# tests/integration/test_add_new_candles_to_dataset.py
from datetime import datetime, timedelta, timezone
from pathlib import Path
import pandas as pd
import pytest
from qlir.data.sources.drift import add_new_candles_to_dataset
from qlir.io.writer import write, write_dataset_meta 
from qlir.io.reader import read

@pytest.mark.integration
def test_add_new_candles_to_dataset_grows_file(tmp_path: Path):
    """
    Real integration test.
    Preconditions:
    - your get_candles(...) actually works against the source
    - the source has candles newer than our last tz_start
    """

    # 1) build an "old" dataset so the fetch has room to add more
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    start = now - timedelta(minutes=60)

    existing_df = pd.DataFrame(
        {
            "tz_start": [
                start,
                start + timedelta(minutes=1),
                start + timedelta(minutes=2),
            ],
            "open": [1.0, 1.1, 1.2],
            "high": [1.0, 1.1, 1.2],
            "low": [1.0, 1.1, 1.2],
            "close": [1.0, 1.1, 1.2],
            "volume": [10, 11, 12],
        }
    )

    # 2) write it to disk using your real writer
    dataset_path = tmp_path / "SOL-PERP_1m.parquet"
    write(existing_df, dataset_path)

    # 3) write meta so add_new_candles_to_dataset can infer symbol/resolution
    write_dataset_meta(dataset_path, symbol="SOL-PERP", resolution="1m")

    original_len = len(existing_df)

    # 4) call the real function (this will hit your real get_candles)
    updated_df = add_new_candles_to_dataset(str(dataset_path))

    # 5) re-read from disk to be sure it was actually written
    on_disk = read(dataset_path)

    # 6) core invariant: it should have grown
    assert len(updated_df) > original_len, (
        f"expected more than {original_len} rows, got {len(updated_df)}"
    )
    assert len(on_disk) == len(updated_df)

    # optional: check sortedness if your union_and_sort guarantees it
    assert on_disk["tz_start"].is_monotonic_increasing
