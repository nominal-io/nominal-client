from . import timedomain
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
    "set_base_url",
    "get_default_client",
    "upload_pandas",
    "upload_polars",
    "upload_csv",
    "get_dataset",
    "create_run",
    "create_run_csv",
    "get_run",
    "search_runs",
    "upload_attachment",
    "get_attachment",
    "download_attachment",
    "upload_video",
    "get_video",
    # classes: when adding a new class, also add a filter to "hide" it in docs/reference/toplevel.md
    "Dataset",
    "Run",
    "Attachment",
    "NominalClient",
    "Video",
]
