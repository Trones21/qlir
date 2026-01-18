from qlir.utils.str.color import Ansi

FULLTEXT_STYLE_RULES = [
    {
        "match": ["PARTIAL"],
        "styles": [Ansi.PINK_HOT, Ansi.BOLD],
        "case_sensitive": True,
    },
    {
        "match": ["MANIFEST", "Manifest", "validation"],
        "styles": [Ansi.TEAL_256_LIGHT],
        "case_sensitive": True,
    },
    {
        "match": ["ERROR", "Error", "exception"],
        "styles": [Ansi.RED, Ansi.BOLD],
        "case_sensitive": True,
    },
    {
        "match": ["CREATED"],
        "styles": [Ansi.GREEN],
        "case_sensitive": True,
    },
    {
        "match": ["REMOVED"],
        "styles": [Ansi.MAGENTA],
        "case_sensitive": True,
    },
]