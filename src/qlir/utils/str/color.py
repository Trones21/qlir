import re
from typing import Iterable


class Ansi:
    ESC = "\033["

    RESET = ESC + "0m"

    # Attributes
    BOLD = ESC + "1m"
    DIM  = ESC + "2m"

    # Some Background Colors
    BG_RED    = ESC + "41m"
    BG_YELLOW = ESC + "43m"
    BG_BLUE   = ESC + "44m"

    # Standard foreground colors
    BLACK   = ESC + "30m"
    RED     = ESC + "31m"
    GREEN   = ESC + "32m"
    YELLOW  = ESC + "33m"
    BLUE    = ESC + "34m"
    MAGENTA = ESC + "35m"
    CYAN    = ESC + "36m"
    WHITE   = ESC + "37m"

    GRAY_DIM = ESC + "2;37m"

    # Bright foreground colors
    GRAY_BRIGHT   = ESC + "90m"
    RED_BRIGHT     = ESC + "91m"
    GREEN_BRIGHT   = ESC + "92m"
    YELLOW_BRIGHT  = ESC + "93m"
    BLUE_BRIGHT    = ESC + "94m"
    MAGENTA_BRIGHT = ESC + "95m"
    CYAN_LIGHT     = ESC + "96m"
    WHITE_BRIGHT   = ESC + "97m"

    PINK_HOT       = ESC + "38;5;198m"
    PINK_HOT_DIM   = ESC + "2;38;5;198m"

    TEAL_256_LIGHT = ESC + "38;5;80m"
    TEAL_256_SOFT  = ESC + "38;5;79m"
    TEAL_256_DIM   = ESC + "2;38;5;80m"

    
# Semantic Helpers
def ok(text: str) -> str:
    return colorize(text, Ansi.GREEN)

def warn(text: str) -> str:
    return colorize(text, Ansi.YELLOW)

def err(text: str) -> str:
    return colorize(text, Ansi.RED, Ansi.BOLD)

# ------------------------------------------------------------
# Core coloring primitive
# ------------------------------------------------------------

def colorize(text: str, *styles: str, enabled: bool = True) -> str:
    """
    Apply ANSI styles to an entire string, with a single reset at the end.
    """
    if not enabled or not styles:
        return text
    return "".join(styles) + text + Ansi.RESET


# ------------------------------------------------------------
# Style composition helper (NO reset)
# ------------------------------------------------------------

def style(*codes: str) -> str:
    """
    Combine ANSI codes into a reusable style string.
    Does NOT include reset.
    """
    return "".join(codes)


# ------------------------------------------------------------
# Substring styling (THIS is the important part)
# ------------------------------------------------------------

def style_matches(
    text: str,
    needles: Iterable[str],
    styles: Iterable[str],
    *,
    case_sensitive: bool = False,
) -> str:
    """
    Style EVERY occurrence of any needle found in text.
    Only matched substrings are styled; everything else is untouched.
    """
    needles = list(needles)
    if not needles or not styles:
        return text

    flags = 0 if case_sensitive else re.IGNORECASE
    pattern = "|".join(re.escape(n) for n in needles)
    regex = re.compile(pattern, flags)

    prefix = "".join(styles)
    reset = Ansi.RESET

    def replacer(match: re.Match) -> str:
        return f"{prefix}{match.group(0)}{reset}"

    return regex.sub(replacer, text)


# ------------------------------------------------------------
# Rule application (stacking, multiline-safe)
# ------------------------------------------------------------

def apply_style_rules(
    text: str,
    rules: list[dict],
    *,
    enabled: bool = True,
) -> str:
    """
    Apply a sequence of substring-styling rules to text.
    Rules stack; every rule applies to every occurrence.
    Multiline-safe.
    """
    if not enabled or not rules:
        return text

    # Split but preserve line endings
    lines = text.splitlines(keepends=True)

    styled_lines: list[str] = []

    for line in lines:
        for rule in rules:
            line = style_matches(
                line,
                rule.get("match", []),
                rule.get("styles", []),
                case_sensitive=rule.get("case_sensitive", False),
            )
        styled_lines.append(line)

    return "".join(styled_lines)

# Another way of creating semantic helpers (pass these constants to colorize)
DEBUG_STYLE = style(Ansi.CYAN_LIGHT, Ansi.DIM)

