from collections.abc import Iterable
from typing import Protocol, TypeVar

T_co = TypeVar("T_co", covariant=True)


class IterableNotStr(Iterable[T_co], Protocol[T_co]):
    """An iterable container whose membership test accepts arbitrary objects.

    `str` deliberately does not satisfy this protocol: its `__contains__` method
    accepts only `str`, unlike normal containers such as `list[str]`.
    """

    def __contains__(self, item: object) -> bool:
        """Return whether the container holds an item."""
