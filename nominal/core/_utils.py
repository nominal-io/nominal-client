from __future__ import annotations

import importlib.metadata
import platform
import sys
from typing import Iterable, Protocol, TypeVar, runtime_checkable

from .._utils import logger

T = TypeVar("T")


@runtime_checkable
class HasRid(Protocol):
    rid: str


def rid_from_instance_or_string(value: HasRid | str) -> str:
    if isinstance(value, str):
        return value
    elif isinstance(value, HasRid):
        return value.rid
    raise TypeError(f"{value!r} is not a string nor an instance with a 'rid' attribute")


def update_dataclass(self: T, other: T, fields: Iterable[str]) -> None:
    """Update dataclass attributes, copying from `other` into `self`.

    Uses __dict__ to update `self` to update frozen dataclasses.
    """
    for field in fields:
        self.__dict__[field] = getattr(other, field)


def construct_user_agent_string() -> str:
    """Constructs a user-agent string with system & Python metadata.
    E.g.: nominal-python/1.0.0b0 (macOS-14.4-arm64-arm-64bit) cpython/3.12.4
    """
    try:
        v = importlib.metadata.version("nominal")
        p = platform.platform()
        impl = sys.implementation
        py = platform.python_version()
        return f"nominal-python/{v} ({p}) {impl.name}/{py}"
    except Exception as e:
        # I believe all of the above are cross-platform, but just in-case...
        logger.error("failed to construct user-agent string", exc_info=e)
        return "nominal-python/unknown"
