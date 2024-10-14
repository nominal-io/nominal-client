from . import ts
from .core import Attachment, Check, Checklist, ChecklistBuilder, Dataset, Log, LogSet, NominalClient, Run, Video
from .nominal import (
    checklist_builder,
    create_run,
    create_run_csv,
    download_attachment,
    get_attachment,
    get_checklist,
    get_dataset,
    get_default_client,
    get_log_set,
    get_run,
    get_video,
    search_runs,
    set_base_url,
    upload_attachment,
    upload_csv,
    upload_pandas,
    upload_polars,
    upload_tdms,
    upload_video,
)

__all__ = [
    "ts",
    "create_run",
    "create_run_csv",
    "checklist_builder",
    "download_attachment",
    "get_attachment",
    "get_checklist",
    "get_dataset",
    "get_default_client",
    "get_log_set",
    "get_run",
    "get_video",
    "search_runs",
    "set_base_url",
    "upload_attachment",
    "upload_csv",
    "upload_pandas",
    "upload_polars",
    "upload_tdms",
    "upload_video",
    # classes: when adding a new class, also add a filter to "hide" it in docs/reference/toplevel.md
    "Attachment",
    "Check",
    "Checklist",
    "ChecklistBuilder",
    "Dataset",
    "NominalClient",
    "Run",
    "User",
    "Video",
    "LogSet",
    "Log",
]
