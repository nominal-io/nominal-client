from __future__ import annotations

from itertools import islice
from typing import Iterable, TypeVar

T = TypeVar("T")


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
