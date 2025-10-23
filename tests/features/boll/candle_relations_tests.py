from qlir.indicators.boll import add_bollinger
from qlir.features.boll.candle_relations import add_candle_line_relations, add_candle_relation_mece
from qlir.utils.logdf import logdf
import candle_relations_reference_implementations as ref_imp
import logging
log = logging.getLogger(__name__)
import pytest

@pytest.mark.skip
def test_candle_line_relations(static_data):
    with_boll = add_bollinger(static_data)
    out = add_candle_line_relations(with_boll)
    #log.info(list(out.columns))
    # selected = out[["boll_valid", "bb_lower_state"]]
    # logdf(selected, 40, name="Candle Relations")

    expected = ref_imp.ref_imp_add_candle_line_relations(with_boll)
    assert False, "Assert not yet written"


@pytest.mark.skip
def test_candle_relation_mece(static_data):
    with_boll = add_bollinger(static_data)
    with_rel = add_candle_relation_mece(with_boll)
    # selected = with_rel[["boll_upper", "boll_position"]]
    # #log.info(list(with_rel.columns))
    # logdf(selected, 40)

    expected = ref_imp.ref_imp_add_candle_relation_mece(with_boll)
    assert False, "Assert not yet written"