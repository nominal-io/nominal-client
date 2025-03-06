from __future__ import annotations

import importlib.metadata
import platform
import sys
from itertools import islice
from typing import Iterable, Protocol, Sequence, TypeVar, runtime_checkable

from nominal._utils import logger
from nominal.core.stream import BatchItem

T = TypeVar("T")


@runtime_checkable
class HasRid(Protocol):
    rid: str


def batched(iterable: Iterable[T], n: int, *, strict: bool = False) -> Iterable[tuple[T, ...]]:
    """Batches an iterable into chunks of size n.

    Args:
        iterable: The input iterable to batch
        n: The size of each batch
        strict: If True, raises ValueError if the final batch is incomplete

    Returns:
        An iterable of tuples, where each tuple contains n items from the input iterable
        (except possibly the last batch if strict=False)

    Raises:
        ValueError: If n < 1 or if strict=True and the final batch is incomplete
    """
    if n < 1:
        raise ValueError("n must be at least one")
    iterator = iter(iterable)
    while batch := tuple(islice(iterator, n)):
        if strict and len(batch) != n:
            raise ValueError("batched(): incomplete batch")
        yield batch


def _to_api_batch_key(item: BatchItem) -> tuple[str, Sequence[tuple[str, str]], str]:
    return item.channel_name, sorted(item.tags.items()) if item.tags is not None else [], type(item.value).__name__


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
