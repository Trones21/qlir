# tests/conftest.py
import numpy as np
import pandas as pd
import pytest
from pathlib  import Path
from qlir.io.reader import read
import logging
log = logging.getLogger(__name__)

@pytest.fixture(scope="session")
def static_data() -> pd.DataFrame:
    """
    Loads the canonical static dataset for tests once per session.
    """
    path = Path(__file__).parent / "data" / "sol_1m.test_csv"
    log.info(f"Loading test data from {path}")
    df = read(path)
    return df


# def _make_ohlcv(
#     start="2025-01-01 00:00:00Z",
#     periods=100,
#     freq="1min",
#     tz="UTC",
#     seed=42,
#     base=100.0,
#     vol_mean=250,
#     vol_std=30,
#     zero_vol_idx=(),
#     with_timestamp_col=False,
# ) -> pd.DataFrame:
#     """
#     Deterministic synthetic OHLCV generator (tz-aware index).
#     - Price is a bounded random walk around `base`.
#     - High/Low wrap open/close with a small spread.
#     - Volume is positive except at `zero_vol_idx`.
#     """
#     rng = np.random.default_rng(seed)
#     ts = pd.date_range(start=start, periods=periods, freq=freq, tz=tz)

#     # Random walk for close
#     steps = rng.normal(0, 0.2, size=periods)  # gentle 1-min drift
#     close = base + np.cumsum(steps)

#     # Open is prior close (first equals first close for simplicity)
#     open_ = np.r_[close[0], close[:-1]]

#     # Small spread around max/min(open, close)
#     spread = np.abs(rng.normal(0.05, 0.02, size=periods))
#     high = np.maximum(open_, close) + spread
#     low = np.minimum(open_, close) - spread

#     # Volume
#     volume = np.maximum(0, rng.normal(vol_mean, vol_std, size=periods)).astype(int)
#     if zero_vol_idx:
#         volume = volume.astype(float)  # avoid int assignment issues
#         volume[list(zero_vol_idx)] = 0

#     df = pd.DataFrame(
#         {"open": open_, "high": high, "low": low, "close": close, "volume": volume},
#         index=ts,
#     )
#     df.index.name = "timestamp"

#     if with_timestamp_col:
#         df = df.reset_index()  # keep a real timestamp column if you want to test that path

#     return df


# @pytest.fixture
# def ohlcv_1m_100() -> pd.DataFrame:
#     """
#     100 rows of 1-minute OHLCV, tz-aware UTC index, all rows have volume > 0.
#     Used by: test_vwap_hlc3_basic, test_vwap_hlc3_idempotent
#     """
#     return _make_ohlcv(periods=100, zero_vol_idx=())


# @pytest.fixture
# def ohlcv_with_zero_vol() -> pd.DataFrame:
#     """
#     Like ohlcv_1m_100 but injects zero-volume rows (not at index 0).
#     Used by: test_vwap_hlc3_zero_volume
#     """
#     # Choose a few interior indices to be zero vol
#     zeros = (10, 11, 25, 60, 61, 62)
#     df = _make_ohlcv(periods=120, zero_vol_idx=zeros)
#     # Ensure first row has volume > 0 as expected by tests
#     assert df.iloc[0]["volume"] > 0
#     return df


# @pytest.fixture
# def ohlcv_1m_cross_midnight() -> pd.DataFrame:
#     """
#     Minimal data that crosses a session boundary in UTC.
#     Used if you want to parametrize session reset tests.
#     """
#     return _make_ohlcv(start="2025-01-01 23:58:00Z", periods=5, freq="1min")


# @pytest.fixture
# def ohlcv_1m_100_tscol() -> pd.DataFrame:
#     """
#     Same as ohlcv_1m_100, but with a concrete 'timestamp' column
#     (no DatetimeIndex). Useful if your code supports both styles.
#     """
#     return _make_ohlcv(periods=100, with_timestamp_col=True)
