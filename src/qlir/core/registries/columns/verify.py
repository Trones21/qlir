from qlir.core.registries.columns.registry import ColRegistry
import pandas as _pd

def verify_declared_cols_exist(*, df: _pd.DataFrame, registry: ColRegistry, caller: str) -> None:
    missing = [decl.column for decl in registry.values() if decl.column not in df.columns]
    if missing:
        raise AssertionError(
            f"{caller}: registry declared columns missing from df.columns: {missing}. "
            f"Returned keys={sorted(registry.keys())}"
        )