# qlir/exceptions.py
from dataclasses import dataclass
from typing import Any


class QLIRError(Exception):
    """Base class for all QLIR errors."""

class QLIRInvariantError(QLIRError):
    """Raised when an internal invariant or required usage contract is violated."""

class QLIRRegistrationError(QLIRInvariantError):
    """Raised when registry/registration wiring is incorrect."""
    def __init__(self, message: str, *, details: Any | None = None):
        super().__init__(message)
        self.details = details


@dataclass(frozen=True)
class RegistryNotEmptyDetails:
    registry_name: str
    keys: tuple[str, ...]