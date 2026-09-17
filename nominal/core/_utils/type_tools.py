from collections.abc import Reversible, Sequence, Sized
from typing import Protocol, TypeVar, overload

T_co = TypeVar("T_co", covariant=True)


class SequenceNotStr(Reversible[T_co], Sized, Protocol[T_co]):
    """A structural sequence whose membership test accepts arbitrary objects.

    `str` deliberately does not satisfy this protocol: its `__contains__` method
    accepts only `str`, unlike normal containers such as `list[str]`.
    """

    def __contains__(self, item: object) -> bool:
        """Return whether the container holds an item."""

    @overload
    def __getitem__(self, index: int, /) -> T_co: ...
    @overload
    def __getitem__(self, index: slice, /) -> Sequence[T_co]: ...
    def __getitem__(self, index: int | slice, /) -> T_co | Sequence[T_co]:
        """Return the item or subsequence at the given index."""
