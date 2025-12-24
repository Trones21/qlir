from datetime import datetime, timezone

def format_ts_ms_and_human(ts_ms: int) -> str:
    iso = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    return f"{ts_ms:_} ms ({iso})"

def format_ts_human(ts_ms: int) -> str:
    iso = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    return f"{iso}"



def format_delta_ms(delta_ms: int) -> str:
    sign = "+" if delta_ms >= 0 else "-"
    abs_ms = abs(delta_ms)
    sec = abs_ms / 1000
    return f"{sign}{abs_ms:_} ms ({sign}{sec:.3f} s)"

