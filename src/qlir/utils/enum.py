from enum import Enum
from typing import Any, Type, TypeVar

E = TypeVar("E", bound=Enum)

def serialize_enum(obj: Any) -> Any:
    if isinstance(obj, Enum):
        return obj.value
    return obj


def deserialize_enum(enum_cls: Type[E], raw: Any) -> E:
    if isinstance(raw, enum_cls):
        return raw
    if isinstance(raw, str):
        try:
            return enum_cls(raw)
        except ValueError as exc:
            raise ValueError(
                f"Invalid {enum_cls.__name__} literal: {raw!r}"
            ) from exc
    raise TypeError(
        f"Expected {enum_cls.__name__} or str, got {type(raw).__name__}"
    )


def enum_for_log(e):
    return e.value if isinstance(e, Enum) else e