import importlib.metadata

from nominal.io import ts
from nominal.io.core import (
    Asset,
    Attachment,
    Channel,
    Checklist,
    CheckViolation,
    Connection,
    DataReview,
    DataReviewBuilder,
    Dataset,
    Log,
    LogSet,
    NominalClient,
    Run,
    User,
    Video,
    Workbook,
    WriteStream,
)

__all__ = [
    "ts",
    # classes: when adding a new class, also add a filter to "hide" it in docs/reference/toplevel.md
    "Asset",
    "Attachment",
    "Channel",
    "Checklist",
    "Connection",
    "Dataset",
    "NominalClient",
    "Run",
    "User",
    "Video",
    "LogSet",
    "Log",
    "Workbook",
    "DataReview",
    "CheckViolation",
    "DataReviewBuilder",
    "WriteStream",
    "__version__",
]


try:
    __version__ = importlib.metadata.version("nominal")
except importlib.metadata.PackageNotFoundError:
    __version__ = "unknown"
del importlib
