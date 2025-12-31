from urllib.parse import parse_qs, urlparse

from qlir.data.sources.binance.endpoints.klines.manifest.validation.contracts.slice_facts_parts import SliceInvariantsParts


class RequestedURLParseError(ValueError):
    def __init__(
        self,
        message: str,
        *,
        url: str,
        param: str | None = None,
    ):
        super().__init__(message)
        self.url = url
        self.param = param


def parse_requested_kline_url(url: str) -> SliceInvariantsParts:
    """
    Parse a Binance klines requested_url and extract canonical slice facts.

    Required query params:
      - symbol
      - interval
      - limit
      - startTime

    Returns SliceFacts if valid, raises RequestedURLParseError otherwise.
    """
    parsed = urlparse(url)

    if not parsed.query:
        raise RequestedURLParseError(
            f"Requested URL has no query string: {url}",
            url=url,            
        )

    qs = parse_qs(parsed.query)

    def require_param(name: str) -> str:
        if name not in qs or not qs[name]:
            raise RequestedURLParseError(
                f"Missing required query param '{name}' in URL: {url}",
                url=url,
                param=name
            )
        return qs[name][0]

    symbol = require_param("symbol")
    interval = require_param("interval")

    try:
        limit = int(require_param("limit"))
    except ValueError as exc:
        raise RequestedURLParseError(
            f"Invalid limit in requested_url: {qs.get('limit')}",
            url=url,
            param="limit"
        ) from exc

    try:
        start_time = int(require_param("startTime"))
    except ValueError as exc:
        raise RequestedURLParseError(
            f"Invalid startTime in requested_url: {qs.get('startTime')}",
            url=url,
            param="startTime"
        ) from exc

    if limit <= 0:
        raise RequestedURLParseError(
            f"limit must be > 0 (got {limit})"
            ,
            url=url,
            param="limit"
        )

    if start_time < 0:
        raise RequestedURLParseError(
            f"startTime must be non-negative (got {start_time})",
            url=url,
            param="startTime"
        )

    return {
        "symbol": symbol,
        "interval": interval,
        "start_time": start_time,
        "limit": limit,
    }

