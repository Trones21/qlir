import pandas as _pd
import pytest

@pytest.fixture
def drift_candles_df():
    return _pd.DataFrame([
        {
            "ts": 1763596800,
            "fillOpen": 136.900000,
            "fillHigh": 144.851800,
            "fillClose": 143.085200,
            "fillLow": 136.272500,
            "oracleOpen": 136.930000,
            "oracleHigh": 144.665866,
            "oracleClose": 143.134701,
            "oracleLow": 136.297922,
            "quoteVolume": 3.006753e+07,
            "baseVolume": 211825.66,
        },
        {
            "ts": 1763510400,
            "fillOpen": 140.432700,
            "fillHigh": 142.622500,
            "fillClose": 136.900000,
            "fillLow": 130.349881,
            "oracleOpen": 140.637017,
            "oracleHigh": 142.714247,
            "oracleClose": 136.930000,
            "oracleLow": 130.450492,
            "quoteVolume": 1.408030e+08,
            "baseVolume": 1032738.61,
        },
    ])