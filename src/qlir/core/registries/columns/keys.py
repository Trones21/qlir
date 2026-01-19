from dataclasses import dataclass, field
import logging
from typing import Iterable

log = logging.getLogger(__name__)


@dataclass
class ColumnKeyCatalog:
    _known_keys: set[str] = field(default_factory=set)

    def register(self, key: str) -> None:
        self._known_keys.add(key)

    def register_many(self, keys: Iterable[str]) -> None:
        for k in keys:
            self.register(k)

    def warn_if_unknown(self, key: str, *, context: str | None = None) -> None:
        if key in self._known_keys:
            return

        # msg = (
        #     f"Unknown column key referenced: '{key}'. "
        #     "This key is not registered in the global column catalog."
        # )
        # if context:
        #     msg += f" (context={context})"
        # log.warning(msg)


COLUMN_KEYS = ColumnKeyCatalog()

