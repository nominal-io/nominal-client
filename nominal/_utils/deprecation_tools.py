from __future__ import annotations

import inspect
import logging
import warnings
from functools import wraps
from typing import Callable, ParamSpec, TypeVar

logger = logging.getLogger(__name__)

Param = ParamSpec("Param")
T = TypeVar("T")

_WARN_ON_DEPRECATED_MARKER = "__warn_on_deprecated_argument_wrapper__"
_WARN_ON_DEPRECATED_STACKLEVEL_OFFSET = "__warn_on_deprecated_argument_stacklevel_offset__"


class _NotProvided:
    """Sentinel class for detecting when a deprecated keyword argument was not provided by the caller"""


def _stacklevel_for_wrapper(func: Callable[..., object]) -> int:
    return 2 + getattr(func, _WARN_ON_DEPRECATED_STACKLEVEL_OFFSET, 0)


def _increment_inner_warn_wrapper_offsets(func: Callable[..., object]) -> None:
    """Account for outer warn_on_deprecated_argument decorators stacked on top of inner ones."""
    seen: set[int] = set()
    current: object | None = func

    while current is not None and id(current) not in seen:
        seen.add(id(current))

        if getattr(current, _WARN_ON_DEPRECATED_MARKER, False):
            current_offset = getattr(current, _WARN_ON_DEPRECATED_STACKLEVEL_OFFSET, 0)
            setattr(current, _WARN_ON_DEPRECATED_STACKLEVEL_OFFSET, current_offset + 1)

        current = getattr(current, "__wrapped__", None)


def warn_on_deprecated_argument(
    argument_name: str, warning_message: str
) -> Callable[[Callable[Param, T]], Callable[Param, T]]:
    """Decorator to warn when a deprecated argument is used.

    Args:
        argument_name: Name of the argument that is deprecated
        warning_message: Custom warning message to display when the deprecated argument is used

    Returns:
        A decorator function that warns when the deprecated argument is used
    """

    def decorator(func: Callable[Param, T]) -> Callable[Param, T]:
        _increment_inner_warn_wrapper_offsets(func)
        sig = inspect.signature(func)
        param_names = list(sig.parameters.keys())

        @wraps(func)
        def wrapper(*args: Param.args, **kwargs: Param.kwargs) -> T:
            if argument_name in kwargs:
                warnings.warn(warning_message, UserWarning, stacklevel=_stacklevel_for_wrapper(wrapper))
            elif len(args) > len(param_names) - 1:
                # TODO(drake): validate that this actually resolves to the deprecated kwarg
                warnings.warn(warning_message, UserWarning, stacklevel=_stacklevel_for_wrapper(wrapper))

            return func(*args, **kwargs)

        setattr(wrapper, _WARN_ON_DEPRECATED_MARKER, True)
        setattr(wrapper, _WARN_ON_DEPRECATED_STACKLEVEL_OFFSET, 0)
        return wrapper

    return decorator


def deprecate_arguments(
    deprecated_args: list[str], new_kwarg: str, new_method: Callable[..., T]
) -> Callable[[Callable[Param, T]], Callable[Param, T]]:
    """Decorator to deprecate specific positional and keyword arguments in favor of a keyword-only argument.

    This decorator handles the case where a method has arguments that are being deprecated
    in favor of a keyword-only argument. If any deprecated arguments are provided (either as
    positional or keyword arguments), it will:
    1. Issue a warning
    2. Execute the original method (which contains the legacy logic)
    3. If no deprecated arguments are provided but the new keyword argument is,
       it will call new_method with the new keyword argument.
    4. If only self is passed (for instance methods), it will call new_method.

    Args:
        deprecated_args: List of argument names that are being deprecated
        new_kwarg: Name of the new keyword-only argument that replaces the deprecated args
        new_method: Function to call when using the new approach. This is the new implementation
                   that will be used when the new keyword argument is provided.

    Returns:
        A decorator function
    """

    def decorator(method: Callable[Param, T]) -> Callable[Param, T]:
        sig = inspect.signature(method)
        param_names = list(sig.parameters.keys())

        min_deprecated_index = float("inf")
        for arg in deprecated_args:
            if arg in param_names:
                min_deprecated_index = min(min_deprecated_index, param_names.index(arg))

        def wrapper(*args: Param.args, **kwargs: Param.kwargs) -> T:
            has_deprecated_kwargs = any(arg_name in kwargs for arg_name in deprecated_args)
            has_deprecated_positional = (
                len(args) > min_deprecated_index if min_deprecated_index != float("inf") else False
            )

            if has_deprecated_kwargs or has_deprecated_positional:
                warnings.warn(f"Use the '{new_kwarg}' keyword argument instead.", UserWarning, stacklevel=2)
                return method(*args, **kwargs)

            if new_kwarg in kwargs:
                return new_method(*args, **{new_kwarg: kwargs[new_kwarg]})

            if len(args) == 1 and not kwargs:
                return new_method(*args)

            return method(*args, **kwargs)

        return wrapper

    return decorator
