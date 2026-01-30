import pandas as pd

from qlir.core.registries.columns.announce_and_register import announce_column_lifecycle
from qlir.core.registries.columns.lifecycle import ColumnLifecycleEvent
from qlir.core.registries.columns.registry import ColKeyDecl, ColRegistry
from qlir.core.semantics.events import log_column_event
from qlir.core.types.annotated_df import AnnotatedDF
from qlir.df.utils import _ensure_columns

def bb_width_step(
    df: pd.DataFrame,
    *,
    dw_bps_col: str = "bb_width_dw_bps",
    out_step_col: str = "bb_width_step",
    eps_bps: float = 1.0,
) -> AnnotatedDF:
    """
    Classify per-bar BB width change into {-1, 0, +1} steps using a noise floor.

    step = +1 if dW >= eps
    step = -1 if dW <= -eps
    step =  0 otherwise

    Parameters
    ----------
    df : pd.DataFrame
        Input dataframe.
    dw_bps_col : str
        Column containing per-bar BB width delta in bps (W_t - W_{t-1}).
    out_step_col : str
        Output column name for step classification.
    eps_bps : float
        Noise floor in bps. Changes smaller than eps are treated as 0.

    Returns
    -------
    pd.DataFrame
        Same df with out_step_col added.
    """
    _ensure_columns(df=df, cols=[dw_bps_col], caller="bb_width_step")
    dw = df[dw_bps_col]

    step = pd.Series(0, index=df.index, dtype="int8")
    step = step.mask(dw >= eps_bps, 1)
    step = step.mask(dw <= -eps_bps, -1)

    out = df.copy()
    out[out_step_col] = step

    new_cols = ColRegistry()
    new_cols.add("out_step_col", out_step_col)
    log_column_event(caller="bb_width_step", ev=ColumnLifecycleEvent(key="out_step_col", col=out_step_col, event="created"))

    return AnnotatedDF(df=out, new_cols=new_cols, label="with_bb_width_step")


def bb_width_pressure(
    df: pd.DataFrame,
    *,
    step_col: str = "bb_width_step",
    m: int = 5,
    k: int = 3,
    out_up_ok: str = "bb_width_up_ok",
    out_dn_ok: str = "bb_width_dn_ok",
) -> AnnotatedDF:
    """
    Debounce step signal using k-of-m logic.

    up_ok  = count(step==+1 in last m) >= k
    dn_ok  = count(step==-1 in last m) >= k
    """
    step = df[step_col]

    up_cnt = (step == 1).rolling(m, min_periods=m).sum()
    dn_cnt = (step == -1).rolling(m, min_periods=m).sum()

    out = df.copy()
    out[out_up_ok] = (up_cnt >= k)
    out[out_dn_ok] = (dn_cnt >= k)
    
    new_cols = ColRegistry()
    announce_column_lifecycle(caller="bb_width_pressure", registry=new_cols, 
        decls=[
            ColKeyDecl(key="out_up_ok", column=out_up_ok), 
            ColKeyDecl(key="out_dn_ok", column=out_dn_ok)
            ],
        event="created")
        
    return AnnotatedDF(df=out, new_cols=new_cols, label="with_bb_width_pressure")
