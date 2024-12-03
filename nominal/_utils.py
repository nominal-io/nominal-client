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
