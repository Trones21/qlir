from qlir.indicators.boll import with_bollinger
from qlir.utils.logdf import logdf
import pytest
pytestmark = pytest.mark.local


def test_boll_cols(static_data):
    out = with_bollinger(static_data)
    logdf(out, 40, name="Bollinger")
    for c in ["boll_mid", "boll_upper", "boll_lower"]:
        assert c in out.columns
