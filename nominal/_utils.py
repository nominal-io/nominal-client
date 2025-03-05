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

            # Check if deprecated argument is passed as positional
            elif len(args) > len(param_names) - 1:  # -1 because we're removing one parameter
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


def deprecate_positional_args_with_fallback(
    deprecated_args: list[str], new_kwarg: str, fallback_method: Callable | None = None
) -> Callable[[Callable[Param, T]], Callable[Param, T]]:
    """Decorator to deprecate positional arguments in favor of a keyword argument.

    This decorator handles the case where a method has positional arguments that are being deprecated
    in favor of a keyword argument. If any of the deprecated arguments are provided, it will:
    1. Issue a warning
    2. Execute the original method (which contains the legacy logic)
    3. If none of the deprecated arguments are provided but the new keyword argument is,
       it will call the fallback method with the new keyword argument.

    Args:
        deprecated_args: List of names of the positional arguments being deprecated
        new_kwarg: Name of the new keyword argument that replaces the deprecated ones
        fallback_method: Optional function to call when using the new approach. If None,
                         the original method will be called.

    Returns:
        A decorator function
    """

    def decorator(method: Callable[Param, T]) -> Callable[Param, T]:
        import inspect
        from typing import cast

        sig = inspect.signature(method)
        param_names = list(sig.parameters.keys())
        is_instance_method = len(param_names) > 0 and param_names[0] in ("self", "cls")

        def wrapper(*args: Param.args, **kwargs: Param.kwargs) -> T:
            # Check if any deprecated args are explicitly provided in kwargs
            using_deprecated = any(arg_name in kwargs for arg_name in deprecated_args)

            # If not found in kwargs, check if they're provided as positional args
            if not using_deprecated and args:
                # Get the names of parameters that would receive positional args
                # Skip the first parameter (self/cls) for instance/class methods
                offset = 1 if is_instance_method and len(args) > 0 else 0

                # Check if any positional args map to deprecated parameters
                for i, _ in enumerate(args[offset:], start=offset):
                    if i < len(param_names) and param_names[i] in deprecated_args:
                        using_deprecated = True
                        break

            if using_deprecated:
                import warnings

                warnings.warn(
                    (
                        f"The positional arguments {', '.join(f'{arg!r}' for arg in deprecated_args)} "
                        f"are deprecated and will be removed in a future version. "
                        f"Use the '{new_kwarg}' keyword argument instead."
                    ),
                    UserWarning,
                    stacklevel=2,
                )
                # Execute the original method with its legacy logic
                return method(*args, **kwargs)

            # If we're here, none of the deprecated args were used
            # Check if the new kwarg is provided and we have a fallback method
            if new_kwarg in kwargs and fallback_method is not None:
                # Pass the instance as first argument if this is an instance method
                if is_instance_method and len(args) > 0:
                    return cast(T, fallback_method(args[0], **{new_kwarg: kwargs[new_kwarg]}))
                else:
                    return cast(T, fallback_method(**{new_kwarg: kwargs[new_kwarg]}))

            # If neither deprecated args nor new kwarg are used, or if no fallback method is provided,
            # just call the original method
            return method(*args, **kwargs)

        return wrapper

    return decorator
