from . import ts
from .core import Attachment, Dataset, NominalClient, Run, Video
from .nominal import (
    create_run,
    create_run_csv,
    download_attachment,
    get_attachment,
    get_dataset,
    get_default_client,
    get_run,
    get_video,
    search_runs,
    set_base_url,
    upload_attachment,
    upload_csv,
    upload_pandas,
    upload_polars,
    upload_video,
)

__all__ = [
    "ts",
    "create_run",
    "create_run_csv",
    "download_attachment",
    "get_attachment",
    "get_dataset",
    "get_default_client",
    "get_run",
    "get_video",
    "search_runs",
    "set_base_url",
    "upload_attachment",
    "upload_csv",
    "upload_pandas",
    "upload_polars",
    "upload_video",
    # classes: when adding a new class, also add a filter to "hide" it in docs/reference/toplevel.md
    "Attachment",
    "Dataset",
    "NominalClient",
    "Run",
    "Video",
]
