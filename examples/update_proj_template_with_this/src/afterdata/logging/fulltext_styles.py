from qlir.utils.str.color import colorize, Ansi

FULLTEXT_STYLE_RULES = [
    {
        "match": ["PARTIAL"],
        "styles": [Ansi.PINK_HOT, Ansi.BOLD],
        "case_sensitive": False,
    },
    {
        "match": ["manifest", "validation"],
        "styles": [Ansi.TEAL_256_LIGHT],
        "case_sensitive": False,
    },
    {
        "match": ["ERROR", "exception"],
        "styles": [Ansi.RED, Ansi.BOLD],
        "case_sensitive": False,
    },
]