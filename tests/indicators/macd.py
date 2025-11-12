from qlir.indicators.macd import with_macd
import logging
log = logging.getLogger(__name__)
from qlir.utils.logdf import logdf
import pytest
pytestmark = pytest.mark.local

def test_macd_columns(static_data):
    out = with_macd(static_data)
    logdf(out, 40, name="With MACD")


    for c in ["macd", "macd_signal", "macd_hist"]:
        assert c in out.columns