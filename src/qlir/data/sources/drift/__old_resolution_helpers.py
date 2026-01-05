
## Not sure these are needed anymore

def normalize_drift_resolution_token(res: str | int) -> str:
    """
    Accept flexible inputs (1, "1m", "60", "D", "W", "M", 3600s, etc.)
    and return one of: {"1","5","15","60","240","D","W","M"}.
    """
    if isinstance(res, int):
        if res in (1, 5, 15, 60, 240):
            return str(res)
        if res % 60 == 0:
            mins = res // 60
            if mins in (1, 5, 15, 60, 240):
                return str(mins)
        raise ValueError(f"Unrecognized numeric resolution: {res}")

    r = str(res).strip().lower()

    if r in {"d", "day", "daily"}:
        return "D"
    if r in {"w", "wk", "week", "weekly"}:
        return "W"
    if r in {"m", "mo", "mon", "month", "monthly"}:
        return "M"

    if r.endswith("m"):
        mins = int(r[:-1])
        if mins in (1, 5, 15, 60, 240):
            return str(mins)
    if r.endswith("h"):
        hrs = int(r[:-1])
        mins = hrs * 60
        if mins in (60, 240):
            return str(mins)

    if r.isdigit():
        if r in DRIFT_ALLOWED:
            return r
        mins = int(r)
        if mins in (1, 5, 15, 60, 240):
            return str(mins)

    raise ValueError(
        f"Resolution {res!r} is not supported. Use one of {sorted(DRIFT_ALLOWED)} or compatible aliases."
    )


def infer_drift_resolution_token_from_df(df: _pd.DataFrame) -> str:
    """
    Infer Drift resolution token from tz_start diffs.
    Returns one of {"1","5","15","60","240","D","W","M"}.
    """
    if df.empty:
        raise ValueError("Cannot infer resolution from empty DataFrame.")

    ts = _pd.to_datetime(df["tz_start"], utc=True).sort_values().drop_duplicates()
    if len(ts) < 2:
        raise ValueError("Need at least two rows to infer resolution.")

    diffs = ts.diff().dropna().dt.total_seconds().astype(int)
    step = int(diffs.mode().iloc[0]) if not diffs.mode().empty else int(diffs.min())

    sec_to_token = {
        60: "1",
        300: "5",
        900: "15",
        3600: "60",
        14400: "240",
        86400: "D",
        604800: "W",
    }
    if step in sec_to_token:
        return sec_to_token[step]

    day = 86400
    if 25 * day <= step <= 35 * day:
        return "M"

    raise ValueError(f"Could not map inferred step {step}s to Drift token.")


def _step_seconds_for_token(token: str) -> Optional[int]:
    """Nominal step size for each token (None for D/W/M)."""
    minute_map = {"1": 60, "5": 300, "15": 900, "60": 3600, "240": 14400}
    return minute_map.get(token)

