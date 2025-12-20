class Ansi:
    RESET = "\033[0m"

    BOLD = "\033[1m"
    DIM = "\033[2m"

    RED = "\033[31m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    BLUE = "\033[34m"
    MAGENTA = "\033[35m"
    CYAN = "\033[36m"
    GRAY = "\033[90m"

def colorize(text: str, *styles: str, enabled: bool = True) -> str:
    if not enabled or not styles:
        return text
    return "".join(styles) + text + Ansi.RESET


# Semantic Helpers
def ok(text: str) -> str:
    return colorize(text, Ansi.GREEN)

def warn(text: str) -> str:
    return colorize(text, Ansi.YELLOW)

def err(text: str) -> str:
    return colorize(text, Ansi.RED, Ansi.BOLD)
