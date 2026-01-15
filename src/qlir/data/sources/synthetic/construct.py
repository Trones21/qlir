import pandas as _pd

# Synthetic construction assumes complete time indices but allows incomplete values.
# Synthetic construction produces blends, not fills.

# Synthetic construction performs cross-source blending, not gap filling.

# This module creates new synthetic price sources by aggregating multiple
# fully-aligned input sources. Aggregation may overwrite observed prices
# and does not preserve exchange-level provenance.

# This module does NOT:
# - fill missing OHLC values for a single source
# - repair temporal gaps
# - perform backfilling using secondary sources

# To backfill gaps in a single data source using other sources,
# use the `gaps` module.


def construct(
    *,
    sources: dict[str, _pd.DataFrame],
    policy: "SyntheticConstructionPolicy",
    ohlc_cols: tuple[str, str, str, str],
    interval_s: int,
) -> _pd.DataFrame:
    """
    Construct a synthetic price source from multiple aligned sources.

    Preconditions
    -------------
    - All input DataFrames:
        - share the same fully-materialized wall-clock index
        - may contain missing OHLC values
        - are ETL-normalized
    - No temporal gaps exist.
    - No repair or backfilling is performed here.

    Semantics
    ---------
    - Missing OHLC values are handled (or ignored) by the construction policy (but this is NOT synthetic gap filling)
    - Observed prices may be overwritten by construction logic.
    - Inputs are never mutated.

    Returns
    -------
    _pd.DataFrame
        A new DataFrame representing a synthetic source.
    """
    assert all(
        df.index.equals(next(iter(sources.values())).index)
        for df in sources.values()
    )
