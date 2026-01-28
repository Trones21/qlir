from qlir.servers.analysis_server.df_materialization.registrar import register_df
from . import registry as reg 

def register_all() -> None:
    # ---- core structure / direction --------------------------------------
    # register_df("df_sma_14_direction", sma_14_direction)

    # ---- persistence / survival ------------------------------------------
    # register_df("df_open_sma_14_survival", sma_survival)

    # ---- volatility / expansion ------------------------------------------
    # register_df("df_bb_width", bb_width)
    # register_df("df_atr_regime", atr_regime)

    # ---- experimental -----------------------------------------------------
    # register_df("df_path_length", path_length)

    # -- initial test ---
    register_df("df_path_len", reg.build_df_path_len_cols)
    register_df("df_boll", reg.build_df_boll)
    return 