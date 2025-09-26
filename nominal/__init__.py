import importlib.metadata
import warnings
from typing import Any

import nominal.core
from nominal.nominal import (  # noqa: F401
    create_asset,
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


def __getattr__(name: str) -> Any:
    """Handle deprecated core class imports with warnings"""
    deprecated_names = {
        "Asset",
        "Attachment",
        "Channel",
        "Checklist",
        "CheckViolation",
        "Connection",
        "DataReview",
        "DataReviewBuilder",
        "Dataset",
        "DatasetFile",
        "NominalClient",
        "Run",
        "User",
        "Video",
        "Workbook",
        "Workspace",
        "WorkspaceSearchType",
        "WriteStream",
    }
    if name in deprecated_names:
        warnings.warn(
            f"Importing {name} from 'nominal' is deprecated. "
            f"Please import from 'nominal.core' instead: 'from nominal.core import {name}'",
            UserWarning,
            stacklevel=2,
        )
        return getattr(nominal.core, name)
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")


try:
    __version__ = importlib.metadata.version("nominal")
except importlib.metadata.PackageNotFoundError:
    __version__ = "unknown"
del importlib
