from qlir.core.types.named_df import NamedDF
from qlir.indicators.boll import with_bollinger
from qlir.logging.logdf import logdf
import pytest
pytestmark = pytest.mark.local


def test_boll_cols(static_data):
    out = with_bollinger(static_data)
    logdf(NamedDF(out, name="Bollinger"), max_rows=40)
    for c in ["boll_mid", "boll_upper", "boll_lower"]:
        assert c in out.columns
