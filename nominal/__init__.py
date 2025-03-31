import importlib.metadata

from nominal import ts
from nominal.core import (
    Asset,
    Attachment,
    Channel,
    Checklist,
    CheckViolation,
    Connection,
    DataReview,
    DataReviewBuilder,
    Dataset,
    EventType,
    Log,
    LogSet,
    NominalClient,
    Run,
    User,
    Video,
    Workbook,
    WriteStream,
)
from nominal.nominal import (
    create_asset,
    create_event,
    create_log_set,
    create_run,
    create_run_csv,
    create_streaming_connection,
    create_workbook_from_template,
    data_review_builder,
    download_attachment,
    get_asset,
    get_attachment,
    get_checklist,
    get_connection,
    get_data_review,
    get_dataset,
    get_default_client,
    get_events,
    get_log_set,
    get_run,
    get_video,
    list_streaming_checklists,
    search_assets,
    search_runs,
    set_base_url,
    set_token,
    upload_attachment,
    upload_csv,
    upload_mcap_video,
    upload_pandas,
    upload_polars,
    upload_tdms,
    upload_video,
)

__all__ = [
    "ts",
    "create_asset",
    "create_run",
    "create_run_csv",
    "create_event",
    "get_events",
    "download_attachment",
    "get_attachment",
    "create_streaming_connection",
    "get_connection",
    "get_checklist",
    "get_dataset",
    "get_default_client",
    "get_log_set",
    "get_run",
    "get_video",
    "search_runs",
    "set_base_url",
    "set_token",
    "upload_attachment",
    "upload_csv",
    "upload_pandas",
    "upload_polars",
    "upload_tdms",
    "upload_mcap_video",
    "upload_video",
    "create_log_set",
    "get_asset",
    "create_workbook_from_template",
    "data_review_builder",
    "get_data_review",
    "list_streaming_checklists",
    "search_assets",
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
    "EventType",
    "__version__",
]


try:
    __version__ = importlib.metadata.version("nominal")
except importlib.metadata.PackageNotFoundError:
    __version__ = "unknown"
del importlib
