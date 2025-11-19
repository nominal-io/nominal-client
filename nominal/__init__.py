from __future__ import annotations

from typing import Any

AUTHENTICATION_DOCS_LINK = "https://docs.nominal.io/core/sdk/python-client/authentication"


def __getattr__(name: str) -> Any:  # noqa: PLR0912, PLR0915
    """Handle deprecated core class imports with warnings"""
    deleted_names = {
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
    if name in deleted_names:
        raise ImportError(
            f"Importing {name} from 'nominal' has been removed. "
            f"Please import from 'nominal.core' instead: 'from nominal.core import {name}'",
        )

    match name:
        case "__version__":
            raise ImportError(
                "nominal.__version__ has been removed. Use importlib.metadata.version('nominal') instead.",
            )
        case "set_base_url":
            raise ImportError(
                f"nominal.set_base_url has been removed. "
                f"Use `nominal.core.NominalClient.from_profile` instead, see {AUTHENTICATION_DOCS_LINK}"
            )
        case "set_token":
            raise ImportError(
                f"nominal.set_token has been removed. "
                f"Use `nominal.core.NominalClient.from_profile` instead, see {AUTHENTICATION_DOCS_LINK}"
            )
        case "get_default_client":
            raise ImportError(
                f"nominal.get_default_client has been removed. "
                f"Use `nominal.core.NominalClient.from_profile` instead, see {AUTHENTICATION_DOCS_LINK}"
            )
        case "get_user":
            raise ImportError(
                f"nominal.get_user has been removed. "
                f"Use `nominal.core.NominalClient.get_user` instead, see {AUTHENTICATION_DOCS_LINK}"
            )
        case "upload_tdms":
            raise ImportError(
                "`nominal.upload_tdms` has been removed. Use `nominal.thirdparty.tdms.upload_tdms` instead."
            )
        case "upload_pandas":
            raise ImportError(
                "`nominal.upload_pandas` has been removed. Use `nominal.thirdparty.pandas.upload_dataframe` instead."
            )
        case "upload_polars":
            raise ImportError(
                "`nominal.upload_polars` has been removed. "
                "Use `nominal.thirdparty.pandas.upload_dataframe(df.to_pandas(), ...)` instead."
            )
        case "create_dataset":
            raise ImportError(
                "nominal.create_dataset has been removed. Use `nominal.core.NominalClient.create_dataset` instead."
            )
        case "upload_csv":
            raise ImportError(
                "`nominal.upload_csv` has been removed. "
                "Use `nominal.core.NominalClient.create_dataset` or `nominal.core.NominalClient.get_dataset`, "
                "add data to an existing dataset instead."
            )
        case "get_dataset":
            raise ImportError(
                "nominal.get_dataset has been removed. Use `nominal.core.NominalClient.get_dataset` instead."
            )
        case "create_run":
            raise ImportError(
                "nominal.create_run has been removed. Use `nominal.core.NominalClient.create_run` instead."
            )
        case "create_run_csv":
            raise ImportError(
                "nominal.create_run_csv has been removed. "
                "Use `nominal.core.NominalClient.create_dataset` and `nominal.core.NominalClient.create_run` instead."
            )
        case "get_run":
            raise ImportError("nominal.get_run has been removed. Use `nominal.core.NominalClient.get_run` instead.")
        case "search_runs":
            raise ImportError(
                "nominal.search_runs has been removed. Use `nominal.core.NominalClient.search_runs` instead."
            )
        case "upload_attachment":
            raise ImportError(
                "nominal.upload_attachment has been removed. "
                "Use `nominal.core.NominalClient.create_attachment` instead."
            )
        case "get_attachment":
            raise ImportError(
                "nominal.get_attachment has been removed. Use `nominal.core.NominalClient.get_attachment` instead."
            )
        case "download_attachment":
            raise ImportError(
                "nominal.download_attachment has been removed. "
                "Use `nominal.core.NominalClient.get_attachment` and `nominal.core.Attachment.write` instead."
            )
        case "upload_video":
            raise ImportError(
                "`nominal.upload_video has been removed. Use `nominal.core.NominalClient.create_video` instead."
            )
        case "get_video":
            raise ImportError("nominal.get_video has been removed. Use `nominal.core.NominalClient.get_video` instead.")
        case "create_asset":
            raise ImportError(
                "nominal.create_asset has been removed. Use `nominal.core.NominalClient.create_asset` instead."
            )
        case "get_asset":
            raise ImportError("nominal.get_asset has been removed. Use `nominal.core.NominalClient.get_asset` instead.")
        case "search_assets":
            raise ImportError(
                "nominal.search_assets has been removed. Use `nominal.core.NominalClient.search_assets` instead."
            )
        case "list_streaming_checklists":
            raise ImportError(
                "nominal.list_streaming_checklists has been removed. "
                "Use `nominal.core.NominalClient.list_streaming_checklists` instead."
            )
        case "wait_until_ingestions_complete":
            raise ImportError(
                "nominal.wait_until_ingestions_complete has been removed. "
                "Use `nominal.core.DatasetFile.poll_until_ingestion_complete()` after ingesting"
            )
        case "get_checklist":
            raise ImportError(
                "nominal.get_checklist has been removed. Use `nominal.core.NominalClient.get_checklist` instead."
            )
        case "upload_mcap_video":
            raise ImportError(
                "nominal.upload_mcap_video has been removed. "
                "Use `nominal.core.NominalClient.create_mcap_video` instead."
            )
        case "create_streaming_connection":
            raise ImportError(
                "nominal.create_streaming_connection has been removed. "
                "Use `nominal.core.NominalClient.create_streaming_connection` instead."
            )
        case "get_connection":
            raise ImportError(
                "nominal.get_connection has been removed. Use `nominal.core.NominalClient.get_connection` instead."
            )
        case "create_workbook_from_template":
            raise ImportError(
                "nominal.create_workbook_from_template has been removed. "
                "Use `nominal.core.NominalClient.create_workbook_from_template` instead."
            )
        case "data_review_builder":
            raise ImportError(
                "nominal.data_review_builder has been removed. "
                "Use `nominal.core.NominalClient.data_review_builder` instead."
            )
        case "get_data_review":
            raise ImportError(
                "nominal.get_data_review has been removed. Use `nominal.core.NominalClient.get_data_review` instead."
            )

    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")
