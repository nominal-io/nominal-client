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
        file_type = cls.from_path(path)
        if file_type not in (FileTypes.CSV, FileTypes.CSV_GZ, FileTypes.PARQUET):
            raise ValueError(f"dataset path '{path}' must end in .csv, .csv.gz, or .parquet")

        return file_type

    @classmethod
    def from_video(cls, path: Path | str) -> FileType:
        file_type = cls.from_path(path)
        if file_type not in (FileTypes.MKV, FileTypes.MP4, FileTypes.TS):
            raise ValueError(f"video path '{path}' must end in .mp4, .mkv, or .ts")

        return file_type


class FileTypes:
    BINARY: FileType = FileType("", "application/octet-stream")
    CSV: FileType = FileType(".csv", "text/csv")
    CSV_GZ: FileType = FileType(".csv.gz", "text/csv")
    JSON: FileType = FileType(".json", "application/json")
    MKV: FileType = FileType(".mkv", "video/x-matroska")
    MP4: FileType = FileType(".mp4", "video/mp4")
    MCAP: FileType = FileType(".mcap", "application/octet-stream")
    # https://issues.apache.org/jira/browse/PARQUET-1889
    PARQUET: FileType = FileType(".parquet", "application/vnd.apache.parquet")
    TS: FileType = FileType(".ts", "video/mp2t")
