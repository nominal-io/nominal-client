from __future__ import annotations

import logging
import mimetypes
import os
from contextlib import contextmanager
from pathlib import Path
from typing import BinaryIO, Callable, Iterator, NamedTuple, TypeVar

from typing_extensions import ParamSpec

logger = logging.getLogger(__name__)


Param = ParamSpec("Param")
T = TypeVar("T")


class FileType(NamedTuple):
    extension: str
    mimetype: str

    @classmethod
    def from_path(cls, path: Path | str, default_mimetype: str = "application/octect-stream") -> FileType:
        ext = "".join(Path(path).suffixes)
        mimetype, _encoding = mimetypes.guess_type(path)
        if mimetype is None:
            return cls(ext, default_mimetype)
        return cls(ext, mimetype)

    @classmethod
    def from_path_dataset(cls, path: Path | str) -> FileType:
        path_string = str(path) if isinstance(path, Path) else path
        if path_string.endswith(".csv"):
            return FileTypes.CSV
        if path_string.endswith(".csv.gz"):
            return FileTypes.CSV_GZ
        if path_string.endswith(".parquet"):
            return FileTypes.PARQUET
        raise ValueError(f"dataset path '{path}' must end in .csv, .csv.gz, or .parquet")


class FileTypes:
    CSV: FileType = FileType(".csv", "text/csv")
    CSV_GZ: FileType = FileType(".csv.gz", "text/csv")
    # https://issues.apache.org/jira/browse/PARQUET-1889
    PARQUET: FileType = FileType(".parquet", "application/vnd.apache.parquet")
    MP4: FileType = FileType(".mp4", "video/mp4")
    BINARY: FileType = FileType("", "application/octet-stream")


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
                    f"The '{old_name}' keyword argument is deprecated and will be removed in a future version, use '{new_name}' instead.",
                    UserWarning,
                    stacklevel=2,
                )
                kwargs[new_name] = kwargs.pop(old_name)
            return f(*args, **kwargs)

        return wrapper

    return _deprecate_keyword_argument_decorator
