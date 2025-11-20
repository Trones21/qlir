def backoff_step(
    *,
    max_retries: int,
    current_try: int,
    current_backoff: float,
    base_delay: float = 0.5,
    max_delay: float = 8.0,
):
    """
    Return:
      - should_sleep: bool
      - sleep_time: float
      - next_backoff: float
      - next_try: int

    No I/O. No sleeping. Just state transitions.
    """
    # First try -> use base delay
    delay = current_backoff if current_try > 1 else base_delay

    if current_try > max_retries:
        return False, 0.0, delay, current_try

    # Cap delay
    delay = min(delay, max_delay)

    next_backoff = min(delay * 2, max_delay)
    next_try = current_try + 1

    return True, delay, next_backoff, next_try
