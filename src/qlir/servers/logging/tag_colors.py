from qlir.utils.str.color import Ansi

TAG_COLOR_MAP: dict[str, set[str]] = {
    Ansi.DIM: {
        "DETAILS",
        "DEBUG_CTX",
    },
    Ansi.CYAN: {
        "HTTP",
        "IO",
    },
    Ansi.YELLOW: {
        "PARTIAL",
        "RETRY",
        "STALE",
    },
    Ansi.RED: {
        "ERROR",
        "FAILED",
    },
}

def color_for_tag(tag: str) -> str | None:
    for color, tags in TAG_COLOR_MAP.items():
        if tag in tags:
            return color
    return None