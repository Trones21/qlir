BASE = "https://api.binance.com/api/v3/klines"


def interval_to_ms(interval: str) -> int:
    """
    Only supports '1s' and '1m'.
    """
    if interval == "1s":
        return 1_000
    if interval == "1m":
        return 60_000
    raise ValueError(f"Unsupported interval (only '1s' and '1m' allowed): {interval}")


def generate_kline_url_slices(
    symbol: str,
    interval: str,
    start_ms: int,
    end_ms: int,
    limit: int = 1000,
) -> list[str]:
    """
    Generate Binance /api/v3/klines URLs for a single symbol+interval, with
    non-overlapping time slices of up to `limit` candles each.

    Assumptions:
      - interval is '1s' or '1m'
      - limit is always 1000

    Args:
        symbol: e.g. "BTCUSDT"
        interval: "1s" or "1m"
        start_ms: earliest timestamp (ms) you want klines from
        end_ms: latest timestamp (ms) you want klines up to
        limit: max candles per request (binance max; you fix this as 1000)

    Returns:
        List[str]: all URLs needed to cover [start_ms, end_ms].
    """
    if start_ms > end_ms:
        return []

    interval_ms = interval_to_ms(interval)
    slice_span = interval_ms * limit  # time covered by one request

    urls: list[str] = []
    current_start = start_ms

    while current_start <= end_ms:
        # Inclusive end bound for this slice
        slice_end = current_start + slice_span - 1
        if slice_end > end_ms:
            slice_end = end_ms

        url = (
            f"{BASE}"
            f"?symbol={symbol}"
            f"&interval={interval}"
            f"&startTime={current_start}"
            f"&endTime={slice_end}"
            f"&limit={limit}"
        )
        urls.append(url)

        current_start += slice_span

    return urls
