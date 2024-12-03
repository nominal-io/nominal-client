from __future__ import annotations

import logging
import mimetypes
from pathlib import Path
from typing import NamedTuple

logger = logging.getLogger(__name__)


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
    BINARY: FileType = FileType("", "application/octet-stream")
    CSV: FileType = FileType(".csv", "text/csv")
    CSV_GZ: FileType = FileType(".csv.gz", "text/csv")
    JSON: FileType = FileType(".json", "application/json")
    MP4: FileType = FileType(".mp4", "video/mp4")
    MCAP: FileType = FileType(".mcap", "application/octet-stream")
    # https://issues.apache.org/jira/browse/PARQUET-1889
    PARQUET: FileType = FileType(".parquet", "application/vnd.apache.parquet")
