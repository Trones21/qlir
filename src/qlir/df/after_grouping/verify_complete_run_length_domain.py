import pandas as _pd

def verify_complete_run_length_domain(
    df: _pd.DataFrame,
    *,
    run_len_col: str = "run_len",
) -> None:
    """
    Verify that the run-length domain is complete and gapless.

    Enforces that there is exactly one row for each integer run length
    between min(run_len) and max(run_len), inclusive.

    Missing or duplicate run lengths raise immediately. No rows are
    added or modified.

    Parameters
    ----------
    df : pandas.DataFrame
        Input distribution table.
    run_len_col : str
        Column containing integer run lengths.

    Raises
    ------
    ValueError
        If run lengths are non-integer, missing, or duplicated.
    """

    run_lens = df[run_len_col]

    # Must be integer-typed (or integer-valued)
    if not _pd.api.types.is_integer_dtype(run_lens):
        if not (run_lens.dropna() == run_lens.dropna().astype(int)).all():
            raise ValueError("run_len column contains non-integer values")

    min_n = int(run_lens.min())
    max_n = int(run_lens.max())

    expected = set(range(min_n, max_n + 1))
    observed = set(run_lens.tolist())

    missing = expected - observed
    duplicates = run_lens[run_lens.duplicated()].unique()

    if missing:
        raise ValueError(
            f"Missing run lengths in [{min_n}, {max_n}]: "
            f"{sorted(missing)[:10]}{'…' if len(missing) > 10 else ''}"
        )

    if len(duplicates) > 0:
        raise ValueError(
            f"Duplicate run lengths found: {sorted(map(int, duplicates))}"
        )
