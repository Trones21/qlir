import logging
from dataclasses import dataclass

from .keys import COLUMN_KEYS, ColumnKeyCatalog

log = logging.getLogger(__name__)

@dataclass(frozen=True)
class ColKeyDecl:
    key: str
    column: str | None = None


class ColRegistry:
    """
    Declaration binding a semantic column key to a concrete DataFrame column name.

    A ColKeyDecl represents *what* a column is (by semantic key) and *where*
    it lives in a DataFrame (by column name). The key is stable and cataloged;
    the column name may be None if the key was declared but not produced.
    """
    def __init__(
        self,
        *,
        catalog: ColumnKeyCatalog = COLUMN_KEYS,
        owner: str | None = None,
    ):
        self._catalog = catalog
        self._owner = owner
        self._decls: dict[str, ColKeyDecl] = {}


    def __iter__(self):
        return iter(self._decls)

    def __len__(self) -> int:
        return len(self._decls)

    def __contains__(self, key: str) -> bool:
        return key in self._decls
    
    def items(self):
        return self._decls.items()

    def values(self):
        return self._decls.values()

    def get_column(self, key: str) -> str:
        """
        Return the concrete DataFrame column name for a semantic key.

        Raises
        ------
        KeyError
            If the key was not declared by this operation or has no column bound.
        """
        decl = self.lookup(key)
        if decl is None or decl.column is None:
            raise KeyError(
                f"Column key '{key}' was not produced by this operation"
                f"{self._fmt_owner()}. Available keys: {sorted(self._decls.keys())}"
            )
        return decl.column

    def get_columns(self, keys: list[str]) -> list[str]:
        """
        Return concrete DataFrame column names for semantic keys.

        Parameters
        ----------
        keys : list[str]
            Semantic column keys produced by this operation.

        Returns
        -------
        list[str]
            Concrete DataFrame column names, in the same order as `keys`.

        Raises
        ------
        KeyError
            If any key was not declared by this operation or has no column bound.
        """
        cols: list[str] = []
        
        if not keys:
            raise ValueError("get_columns() requires at least one key")
        
        for key in keys:
            decl = self.lookup(key)
            if decl is None or decl.column is None:
                raise KeyError(
                    f"Column key '{key}' was not produced by this operation"
                    f"{self._fmt_owner()}. Available keys: {sorted(self._decls.keys())}"
                )
            cols.append(decl.column)

        return cols
    
    def keys(self) -> list[str]:
        return list(self._decls.keys())

    def _ctx(self, op: str) -> str:
        return f"{self._owner}.{op}" if self._owner else op

    def _fmt_owner(self) -> str:
        return f" (owner={self._owner})" if self._owner else ""

    def add(self, key: str, column: str | None = None) -> ColKeyDecl:
        """
        Declare that a semantic column key was produced.

        Registers a key with its resolved column name. If the key is not part
        of the column catalog, a warning is emitted.

        Parameters
        ----------
        key:
            Semantic column key being declared.
        column:
            Concrete DataFrame column name, or None if the key was declared
            but not materialized.

        Returns
        -------
        ColKeyDecl
            The declaration object for the key.
        """
        self._catalog.warn_if_unknown(key, context=self._ctx("declare"))
        decl = ColKeyDecl(key=key, column=column)
        self._decls[key] = decl
        return decl

    def lookup(self, key: str) -> ColKeyDecl | None:
        """
        Look up a column declaration by semantic key.

        Validates the key against the catalog and returns the corresponding
        declaration if present. If the key is cataloged but was not returned
        by this operation, a warning is logged.

        Parameters
        ----------
        key:
            Semantic column key to look up.

        Returns
        -------
        ColKeyDecl or None
            The declaration if present; None if the key was not returned.
        """
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




