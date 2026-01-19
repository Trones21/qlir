from dataclasses import dataclass
from typing import Optional, overload
import pandas as pd

from qlir.core.registries.columns.registry import ColRegistry

from dataclasses import dataclass
from typing import Iterator


@dataclass(frozen=True)
class AnnotatedDF:
    """
    A DataFrame paired with explicit metadata about newly created columns.

    AnnotatedDF represents the result of a structural DataFrame operation:
    the DataFrame itself plus a registry describing which semantic columns
    were created by the operation.

    Attributes
    ----------
    df:
        The concrete pandas DataFrame produced by the operation.
    new_cols:
        Registry of semantic keys to concrete column names for columns
        created by this operation only (a structural delta, not a schema).
    label:
        Optional human-readable label describing the operation.
    """

    df: pd.DataFrame
    new_cols: ColRegistry
    label: Optional[str] = None

    def df_and(self, key: str) -> tuple[pd.DataFrame, str]:
        """
        Return the DataFrame and the concrete column name for a single semantic key.

        This is a strongly-typed convenience method for the common case where
        an operation produces exactly one column and the caller wants
        procedural-style unpacking.

        Parameters
        ----------
        key:
            Semantic key of the newly created column.

        Returns
        -------
        (DataFrame, str)
            The DataFrame and the concrete column name.

        Raises
        ------
        KeyError
            If the key was not declared or has no concrete column bound.
        """
        return self.df, self.new_cols.get_column(key)

    # ---- overloads (static typing views) ----

    @overload
    def unwrap(self, key: str) -> tuple[pd.DataFrame, str]: ...

    @overload
    def unwrap(self, key1: str, key2: str) -> tuple[pd.DataFrame, str, str]: ...

    @overload
    def unwrap(self, key1: str, key2: str, key3: str) -> tuple[pd.DataFrame, str, str, str]: ...

    @overload
    def unwrap(self, *keys: str) -> tuple[pd.DataFrame, ...]: ...

    def unwrap(self, *keys: str):
        """
        Return the DataFrame followed by concrete column names for the given keys.

        This method provides ergonomic, tuple-style unpacking while preserving
        AnnotatedDF as the primary return type for structural functions.

        The first element of the returned tuple is always the DataFrame;
        subsequent elements are concrete column names corresponding to the
        provided semantic keys.

        Notes
        -----
        - `unwrap` is intended for call-site ergonomics.
        - Static typing guarantees are strongest for the 1–3 key overloads.
        - For a fully typed single-column case, prefer `df_and()`.

        Parameters
        ----------
        *keys:
            One or more semantic column keys declared in `new_cols`.

        Returns
        -------
        tuple
            A tuple of the form `(df, col1, col2, ...)`.

        Raises
        ------
        KeyError
            If any key was not declared or has no concrete column bound.
        """
        cols = [self.new_cols.get_column(k) for k in keys]
        return (self.df, *cols)
