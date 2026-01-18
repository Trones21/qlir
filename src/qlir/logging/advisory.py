import logging
log = logging.getLogger(__name__)

def advise_column_contract(
    *,
    recommended_fmt: str,
    recommended_value: str,
    actual: str,
    caller: str,
) -> None:
    """
    Emit a column naming advisory when the actual column name deviates
    from the recommended naming convention.

    Shows both the recommended format expression and its evaluated value
    to aid refactors and contract visibility.
    """
    if recommended_value == actual:
        return

    log.info(
        "[ADVISE][COL_FMT] %s\n"
        "  recommended_fmt   = %s\n"
        "  recommended_value = %r\n"
        "  actual            = %r",
        caller,
        recommended_fmt,
        recommended_value,
        actual,
    )
