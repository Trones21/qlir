from __future__ import annotations
import pandas as pd
from ...indicators.vwap import add_vwap_session
from .relations import flag_relations
from ..common.session import add_session_id
from ..common.running import add_counts_running, add_streaks
from .slope import add_vwap_slope
from .distances import add_distance_metrics

__all__ = ["add_vwap_feature_block"]


def add_vwap_feature_block(
    df: pd.DataFrame,
    *,
    tz: str = "UTC",
    touch_eps: float = 5e-4,
    norm_window: int | None = 200,
) -> pd.DataFrame:
    out = add_vwap_session(df, tz=tz)
    out = flag_relations(out, touch_eps=touch_eps)
    out = add_session_id(out, tz=tz)
    out = add_counts_running(out, session_col="session", rel_col="relation")
    out = add_streaks(out, session_col="session", rel_col="relation")
    out = add_vwap_slope(out)
    out = add_distance_metrics(out, norm_window=norm_window)
    return out