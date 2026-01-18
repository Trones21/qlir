import logging
from dataclasses import dataclass

from .keys import COLUMN_KEYS, ColumnKeyCatalog

log = logging.getLogger(__name__)

@dataclass(frozen=True)
class ColKeyDecl:
    key: str
    column: str | None = None


class ColRegistry:
    def __init__(
        self,
        *,
        catalog: ColumnKeyCatalog = COLUMN_KEYS,
        owner: str | None = None,
    ):
        self._catalog = catalog
        self._owner = owner
        self._decls: dict[str, ColKeyDecl] = {}

    def resolve(self, key: str) -> str | None:
        decl = self.lookup(key)
        return None if decl is None else decl.column

    def keys(self) -> list[str]:
        return list(self._decls.keys())

    def _ctx(self, op: str) -> str:
        return f"{self._owner}.{op}" if self._owner else op

    def _fmt_owner(self) -> str:
        return f" (owner={self._owner})" if self._owner else ""

    def declare(self, key: str, column: str | None = None) -> ColKeyDecl:
        self._catalog.warn_if_unknown(key, context=self._ctx("declare"))
        decl = ColKeyDecl(key=key, column=column)
        self._decls[key] = decl
        return decl

    def lookup(self, key: str) -> ColKeyDecl | None:
        self._catalog.warn_if_unknown(key, context=self._ctx("lookup"))

        decl = self._decls.get(key)
        if decl is None:
            log.warning(
                "Column key '%s' was requested but not returned by this call%s. "
                "Returned keys: %s",
                key,
                self._fmt_owner(),
                sorted(self._decls.keys()),
            )
        return decl
    
    def _unique_key(self, base: str) -> str:
        """
        Generate a unique key by suffixing '__dup', '__dup2', etc.
        """
        if base not in self._decls:
            return base

        i = 1
        while True:
            suffix = "__dup" if i == 1 else f"__dup{i}"
            candidate = f"{base}{suffix}"
            if candidate not in self._decls:
                return candidate
            i += 1

    def extend(self, other: "ColRegistry") -> None:
        """
        Merge declarations from another ColRegistry into this one.

        - Never overwrites existing keys
        - On conflict, suffixes the incoming key
        - Logs a warning describing the collision
        """
        for key, decl in other._decls.items():
            if key not in self._decls:
                self._decls[key] = decl
                continue

            # Conflict: key already exists
            new_key = self._unique_key(key)

            log.warning(
                "Column key collision on '%s'%s. "
                "Keeping existing key and registering incoming one as '%s'. "
                "Existing column=%s, Incoming column=%s",
                key,
                self._fmt_owner(),
                new_key,
                self._decls[key].column,
                decl.column,
            )

            self._decls[new_key] = ColKeyDecl(
                key=new_key,
                column=decl.column,
            )




