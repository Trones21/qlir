from qlir.indicators.boll import with_bollinger
from qlir.features.boll.candle_relations import with_candle_line_relations, with_candle_relation_mece
from qlir.logging.logdf import logdf
import candle_relations_reference_implementations as ref_imp
import logging
log = logging.getLogger(__name__)
import pytest
pytestmark = pytest.mark.local

def test_candle_line_relations(static_data):
    with_boll = with_bollinger(static_data)
    out = with_candle_line_relations(with_boll)
    #log.info(list(out.columns))
    # selected = out[["boll_valid", "bb_lower_state"]]
    # logdf(selected, 40, name="Candle Relations")

    expected = ref_imp.ref_imp_with_candle_line_relations(with_boll)
    assert False, "Assert not yet written"


def test_candle_relation_mece(static_data):
    with_boll = with_bollinger(static_data)
    with_rel = with_candle_relation_mece(with_boll)
    # selected = with_rel[["boll_upper", "boll_position"]]
    # #log.info(list(with_rel.columns))
    # logdf(selected, 40)

    expected = ref_imp.ref_imp_with_candle_relation_mece(with_boll)
    assert False, "Assert not yet written"