# tests/integration/test_data_roundtrip.py
import logging
log = logging.getLogger(__name__)

import os
import pandas as _pd
import pytest

from qlir.io.writer import write
from qlir.data.sources.drift.fetch import get_candles_all


# @pytest.mark.integration
# @pytest.mark.network
# def test_data_roundtrip(tmp_path):
#     """
#     Integration test: fetch → normalize → write CSV → read → validate contract.
#     """
#     log.info("Starting integration test with tmp_path=%s", tmp_path)
#     normalized = get_candles_all
#     out_path = tmp_path / "roundtrip.csv"
#     write(normalized, out_path)
#     log.info("Data shape after normalization: %s", normalized)

#     # 3. Read CSV back in (mimic downstream consumer)
#     back = _pd.read_csv(out_path, parse_dates=["tz_start", "tz_end"])

#     validate_contract(back)

#     # # Optional: roundtrip equality check
#     # _pd.testing.assert_index_equal(normalized.index, back.index, obj="index equality")
#     # for col in ["open", "high", "low", "close", "volume"]:
#     #     _pd.testing.assert_series_equal(
#     #         normalized[col].astype(float),
#     #         back[col].astype(float),
#     #         check_exact=False,
#     #         rtol=1e-8,
#     #         obj=f"column {col}",
#     #     )
    
def validate_contract(df: _pd.DataFrame):
    """Validate schema, types, and logical consistency."""
    required = {"tz_start", "tz_end", "open", "high", "low", "close", "volume"}
    assert required.issubset(df.columns), f"Missing columns: {required - set(df.columns)}"
    assert df["tz_start"].is_monotonic_increasing
    assert (df["high"] >= df["low"]).all()
    assert (df["volume"] >= 0).all()
