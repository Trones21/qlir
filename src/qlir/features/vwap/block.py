from __future__ import annotations
import pandas as _pd
from ...indicators.vwap import with_vwap_hlc3_session
from .relations import flag_relations
from ..common.session import with_session_id
from ..common.running import with_counts_running, with_streaks
from .slope import with_vwap_slope
from .distances import with_distance_metrics

__all__ = ["with_vwap_feature_block"]


def with_vwap_feature_block(
    df: _pd.DataFrame,
    *,
    tz: str = "UTC",
    touch_eps: float = 5e-4,
    norm_window: int | None = 200,
) -> _pd.DataFrame:
    out = with_vwap_hlc3_session(df, tz=tz)
    out = flag_relations(out, touch_eps=touch_eps)
    out = with_session_id(out, tz=tz)
    out = with_counts_running(out, group_col="session", rel_col="relation")
    out = with_streaks(out, group_col="session", rel_col="relation")
    out = with_vwap_slope(out)
    out = with_distance_metrics(out, norm_window=norm_window)
    return out