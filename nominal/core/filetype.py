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
        path = Path(path)

        # Note: not using path.suffix because this fails for files with multiple suffixes,
        #       and lowercase to handle files with mixed capitalization on extensions
        ext_str = "".join(path.suffixes).lower()

        # Attempt to match the file's extension(s) with those already explicitly listed under FileTypes
        for file_type in FileTypes.__dict__.values():
            if not isinstance(file_type, cls):
                # Skip any member variables which are not actually FileTypes
                continue
            elif not file_type.extension:
                # Filetype for binary data has no extension, and should be used only
                # as a final fallback
                continue

            # If the file ends with the given file extension, regardless of other suffixes it may have
            # preceeding, then return the file type.
            if ext_str.endswith(file_type.extension):
                return file_type

        # Infer mimetype from filepath
        mimetype, _encoding = mimetypes.guess_type(path)

        # If no mimetype could be inferred, use the default
        if mimetype is None:
            return cls(ext_str, default_mimetype)

        # If no extension could be matched against the explicitly listed filetypes,
        # infer the extension using the mimetype
        extension = mimetypes.guess_extension(mimetype)
        if extension is None:
            return cls(ext_str, mimetype)

        # return the inferred extension and mimetype
        return cls(extension, mimetype)

    def is_csv(self) -> bool:
        return self in FileTypes._CSV_TYPES

    def is_parquet_file(self) -> bool:
        return self in FileTypes._PARQUET_FILE_TYPES

    def is_parquet_archive(self) -> bool:
        return self in FileTypes._PARQUET_ARCHIVE_TYPES

    def is_parquet(self) -> bool:
        return self.is_parquet_file() or self.is_parquet_archive()

    def is_journal(self) -> bool:
        return self in FileTypes._JOURNAL_TYPES

    def is_video(self) -> bool:
        return self in FileTypes._VIDEO_TYPES

    @classmethod
    def from_path_dataset(cls, path: Path | str) -> FileType:
        file_type = cls.from_path(path)
        if not file_type.is_parquet_file() and not file_type.is_csv():
            allowed_extensions = (*FileTypes._PARQUET_FILE_TYPES, *FileTypes._CSV_TYPES)
            raise ValueError(f"dataset path '{path}' must end in one of {allowed_extensions}")

        return file_type

    @classmethod
    def from_tabular(cls, path: Path | str) -> FileType:
        file_type = cls.from_path(path)
        if not file_type.is_csv() and not file_type.is_parquet():
            allowed_extensions = (
                *FileTypes._PARQUET_ARCHIVE_TYPES,
                *FileTypes._PARQUET_FILE_TYPES,
                *FileTypes._CSV_TYPES,
            )
            raise ValueError(f"tabular path '{path}' must end in one of {[f.extension for f in allowed_extensions]}")

        return file_type

    @classmethod
    def from_path_journal_json(cls, path: Path | str) -> FileType:
        file_type = cls.from_path(path)
        if not file_type.is_journal():
            raise ValueError(
                f"journal jsonl path '{path}' must end in one of {[f.extension for f in FileTypes._JOURNAL_TYPES]}"
            )

        return file_type

    @classmethod
    def from_video(cls, path: Path | str) -> FileType:
        file_type = cls.from_path(path)
        if not file_type.is_video():
            raise ValueError(f"video path '{path}' must end in one of {[f.extension for f in FileTypes._VIDEO_TYPES]}")

        return file_type


class FileTypes:
    BINARY: FileType = FileType("", "application/octet-stream")
    CSV: FileType = FileType(".csv", "text/csv")
    CSV_GZ: FileType = FileType(".csv.gz", "text/csv")
    DATAFLASH: FileType = FileType(".bin", "application/octet-stream")
    JSON: FileType = FileType(".json", "application/json")
    MKV: FileType = FileType(".mkv", "video/x-matroska")
    MP4: FileType = FileType(".mp4", "video/mp4")
    MCAP: FileType = FileType(".mcap", "application/octet-stream")
    # https://issues.apache.org/jira/browse/PARQUET-1889
    PARQUET: FileType = FileType(".parquet", "application/vnd.apache.parquet")
    PARQUET_GZ: FileType = FileType(".parquet.gz", "application/octet-stream")
    PARQUET_TAR_GZ: FileType = FileType(".parquet.tar.gz", "application/x-tar")
    PARQUET_TAR: FileType = FileType(".parquet.tar", "application/x-tar")
    PARQUET_ZIP: FileType = FileType(".parquet.zip", "application/zip")
    TS: FileType = FileType(".ts", "video/mp2t")
    JOURNAL_JSONL: FileType = FileType(".jsonl", "application/jsonl")
    JOURNAL_JSONL_GZ: FileType = FileType(".jsonl.gz", "application/jsonl")

    _CSV_TYPES = (CSV, CSV_GZ)
    _PARQUET_FILE_TYPES = (PARQUET_GZ, PARQUET)
    _PARQUET_ARCHIVE_TYPES = (PARQUET_TAR_GZ, PARQUET_TAR, PARQUET_ZIP)
    _JOURNAL_TYPES = (JOURNAL_JSONL, JOURNAL_JSONL_GZ)
    _VIDEO_TYPES = (MKV, MP4, TS)
