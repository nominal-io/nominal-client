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

    @classmethod
    def from_path_dataset(cls, path: Path | str) -> FileType:
        file_type = cls.from_path(path)
        if file_type not in (FileTypes.CSV, FileTypes.CSV_GZ, FileTypes.PARQUET):
            raise ValueError(f"dataset path '{path}' must end in .csv, .csv.gz, or .parquet")

        return file_type

    @classmethod
    def from_path_journal_json(cls, path: Path | str) -> FileType:
        file_type = cls.from_path(path)
        if file_type not in (FileTypes.JOURNAL_JSONL, FileTypes.JOURNAL_JSONL_GZ):
            raise ValueError(f"journal jsonl path '{path}' must end in .jsonl or .jsonl.gz")

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
    DATAFLASH: FileType = FileType(".bin", "application/octet-stream")
    JSON: FileType = FileType(".json", "application/json")
    MKV: FileType = FileType(".mkv", "video/x-matroska")
    MP4: FileType = FileType(".mp4", "video/mp4")
    MCAP: FileType = FileType(".mcap", "application/octet-stream")
    # https://issues.apache.org/jira/browse/PARQUET-1889
    PARQUET: FileType = FileType(".parquet", "application/vnd.apache.parquet")
    TS: FileType = FileType(".ts", "video/mp2t")
    JOURNAL_JSONL: FileType = FileType(".jsonl", "application/jsonl")
    JOURNAL_JSONL_GZ: FileType = FileType(".jsonl.gz", "application/jsonl")
