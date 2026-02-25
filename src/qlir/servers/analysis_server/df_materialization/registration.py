from qlir.servers.analysis_server.df_materialization.registrar import register_df

from .builders import (
    build_df_path_len_cols,
    build_df_boll
)


from qlir.exceptions import QLIRRegistrationError, RegistryNotEmptyDetails


def ensure_df_registry_empty_guard(df_registry: dict, *, registry_name="DF_REGISTRY") -> None:
    if df_registry:
        keys = tuple(sorted(df_registry.keys()))
        details = RegistryNotEmptyDetails(registry_name=registry_name, keys=keys)

        msg = (
            f"{registry_name} is not empty, but ensure_df_registry_empty_guard() was called.\n"
            "Hint: you may have accidentally put registrations directly inside the registry.\n"
            "Correct usage:\n"
            "  1) Import DF builders to registration.py\n"
            "  2) Add register_df calls/wrappers using the builders and names of required dfs\n"
            f"Current {registry_name} keys: {keys}"
        )
        err = QLIRRegistrationError(message=msg, details=details)
        raise err


def df_registration_entrypoint() -> None:
    """This is intended to be the only df registration method called from server.py (Because we have the wrapper which ensures the the DF_REGISTRY is first empty)"""
    from .registry import DF_REGISTRY
    ensure_df_registry_empty_guard(df_registry=DF_REGISTRY)

     # ---- Add Registrations / Registration Wrappers Here --------------------------------------
    example()

    return 




def example():
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
    #register_df()
    register_df("df_path_len", build_df_path_len_cols)
    register_df("df_boll", build_df_boll)