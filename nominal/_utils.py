from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from typing import Any, BinaryIO, Callable, Iterator, TypeVar

from typing_extensions import ParamSpec

from nominal.core import filetype

logger = logging.getLogger(__name__)


Param = ParamSpec("Param")
T = TypeVar("T")


def __getattr__(attr: str) -> Any:
    import warnings

    deprecated_attrs = {"FileType": filetype.FileType, "FileTypes": filetype.FileTypes}
    if attr in deprecated_attrs:
        warnings.warn(
            (
                f"nominal._utils.{attr} is deprecated and will be removed in a future version, use "
                f"nominal.core.{attr} instead."
            ),
            UserWarning,
            stacklevel=2,
        )
        return deprecated_attrs[attr]


@contextmanager
def reader_writer() -> Iterator[tuple[BinaryIO, BinaryIO]]:
    rd, wd = os.pipe()
    r = open(rd, "rb")
    w = open(wd, "wb")
    try:
        yield r, w
    finally:
        w.close()
        r.close()


def deprecate_argument(argument_name: str) -> Callable[[Callable[Param, T]], Callable[Param, T]]:
    """Decorator to warn when a deprecated argument is used.

    Args:
        argument_name: Name of the argument that is deprecated

    Returns:
        A decorator function that warns when the deprecated argument is used
    """

    def decorator(func: Callable[Param, T]) -> Callable[Param, T]:
        import inspect
        import warnings
        from functools import wraps
        from typing import cast

        sig = inspect.signature(func)
        param_names = list(sig.parameters.keys())

        @wraps(func)
        def wrapper(*args: Param.args, **kwargs: Param.kwargs) -> T:
            # Check if deprecated argument is in kwargs
            if argument_name in kwargs:
                warnings.warn(
                    f"The '{argument_name}' argument is deprecated and will be removed in a future version.",
                    UserWarning,
                    stacklevel=2,
                )
                # Create a new kwargs dict without the deprecated argument
                filtered_kwargs = kwargs.copy()
                filtered_kwargs.pop(argument_name)
                # Cast to satisfy the type checker
                return func(*args, **cast(Param.kwargs, filtered_kwargs))

            elif len(args) > len(param_names) - 1:
                warnings.warn(
                    f"The '{argument_name}' argument is deprecated and will be removed in a future version.",
                    UserWarning,
                    stacklevel=2,
                )
                # Only keep the non-deprecated positional arguments
                filtered_args = cast(Param.args, args[: len(param_names) - 1])
                return func(*filtered_args, **kwargs)

            # If the deprecated argument is not used, just call the function normally
            return func(*args, **kwargs)

        return wrapper

    return decorator


def deprecate_keyword_argument(new_name: str, old_name: str) -> Callable[[Callable[Param, T]], Callable[Param, T]]:
    def _deprecate_keyword_argument_decorator(f: Callable[Param, T]) -> Callable[Param, T]:
        def wrapper(*args: Param.args, **kwargs: Param.kwargs) -> T:
            if old_name in kwargs:
                import warnings

                warnings.warn(
                    (
                        f"The '{old_name}' keyword argument is deprecated and will be removed in a "
                        f"future version, use '{new_name}' instead."
                    ),
                    UserWarning,
                    stacklevel=2,
                )
                kwargs[new_name] = kwargs.pop(old_name)
            return f(*args, **kwargs)

        return wrapper

    return _deprecate_keyword_argument_decorator


def deprecate_arguments(
    deprecated_args: list[str], new_kwarg: str, fallback_method: Callable[..., Any] | None = None
) -> Callable[[Callable[Param, T]], Callable[Param, T]]:
    """Decorator to deprecate specific positional and keyword arguments in favor of a keyword-only argument.

    This decorator handles the case where a method has arguments that are being deprecated
    in favor of a keyword-only argument. If any deprecated arguments are provided (either as
    positional or keyword arguments), it will:
    1. Issue a warning
    2. Execute the original method (which contains the legacy logic)
    3. If no deprecated arguments are provided but the new keyword argument is,
       it will call the fallback method with the new keyword argument.

    Args:
        deprecated_args: List of argument names that are being deprecated
        new_kwarg: Name of the new keyword-only argument that replaces the deprecated args
        fallback_method: Optional function to call when using the new approach. If None,
                         the original method will be called.

    Returns:
        A decorator function
    """

    def decorator(method: Callable[Param, T]) -> Callable[Param, T]:
        from typing import cast

        def wrapper(*args: Param.args, **kwargs: Param.kwargs) -> T:
            # For instance methods, we need at least self/cls
            # So we check if there are more args than just self/cls

            # Get the argument names from the function signature
            # Check if any positional args beyond self/cls are provided
            has_positional_args = len(args) > 1

            # Check if any deprecated kwargs are provided
            has_deprecated_kwargs = any(arg_name in kwargs for arg_name in deprecated_args)

            using_deprecated = has_positional_args or has_deprecated_kwargs

            if using_deprecated:
                import warnings

                warnings.warn(
                    f"Use the '{new_kwarg}' keyword argument instead.",
                    UserWarning,
                    stacklevel=2,
                )
                # Execute the original method with its legacy logic
                return method(*args, **kwargs)
            # Check if the new kwarg is provided and we have a fallback method
            if new_kwarg in kwargs and fallback_method is not None:
                # Pass the instance as first argument if this is an instance method
                if len(args) == 1:  # self/cls is present
                    return cast(T, fallback_method(args[0], **{new_kwarg: kwargs[new_kwarg]}))
            # If neither positional args nor new kwarg are used, or if no fallback method is provided
            return method(*args, **kwargs)

        return wrapper

    return decorator
