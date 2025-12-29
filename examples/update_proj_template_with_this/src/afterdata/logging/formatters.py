import logging
from qlir.utils.str.color import colorize, apply_style_rules
from afterdata.logging.level_colors import LEVEL_COLOR
from afterdata.logging.tag_colors import color_for_tag
from .fulltext_styles import FULLTEXT_STYLE_RULES


class ColorizingBracketFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        # ---- level ----
        level = record.levelname
        level_color = LEVEL_COLOR.get(record.levelno)
        if level_color:
            level = colorize(level, level_color)

        # ---- tags (optional, multi-level) ----
        if hasattr(record, "tag"):
            tags = _normalize_tags(record.tag)  # type: ignore

            rendered_tags: list[str] = []
            for t in tags:
                c = color_for_tag(t)
                rendered_tags.append(colorize(t, c) if c else t)

            bracket = f"{level}:{':'.join(rendered_tags)}"
        else:
            bracket = level

        record.bracket = bracket

        # ---- message-only substring styling ----
        original_msg = record.msg
        original_args = record.args

        try:
            # Get the fully interpolated message
            msg = record.getMessage()

            # Apply full-text substring rules (library code)
            msg = apply_style_rules(
                msg,
                FULLTEXT_STYLE_RULES,
            )

            # Inject back so Formatter can render normally
            record.msg = msg
            record.args = None  # prevent double interpolation

            return super().format(record)

        finally:
            # Restore record to avoid side effects
            record.msg = original_msg
            record.args = original_args


def _normalize_tags(tag) -> list[str]:
    if isinstance(tag, (list, tuple)):
        return [str(t) for t in tag]
    return [str(tag)]
