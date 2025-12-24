def term_fmt(text: str, indent: int = 0) -> str:
    import shutil
    import textwrap

    width = shutil.get_terminal_size(fallback=(120, 20)).columns
    return textwrap.fill(
        text,
        width=width,
        subsequent_indent=" " * indent,
        replace_whitespace=False,
        drop_whitespace=False,
    )