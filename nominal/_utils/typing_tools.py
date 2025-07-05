from collections.abc import Callable
from typing import Any, TypeVar, cast

from typing_extensions import ParamSpec

T = TypeVar("T")
P = ParamSpec("P")


def copy_signature_from(_origin: Callable[P, T]) -> Callable[[Callable[..., Any]], Callable[P, T]]:
    """Modify the type signature of variadic function args to match that exactly of another function"""

    def decorator(target: Callable[..., Any]) -> Callable[P, T]:
        return cast(Callable[P, T], target)

    return decorator
