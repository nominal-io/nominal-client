from __future__ import annotations

from datetime import datetime
from functools import cache
from pathlib import Path
from typing import TYPE_CHECKING, Iterable, Mapping, Sequence

import typing_extensions

from nominal import Connection, _config, ts
from nominal.core import (
    Asset,
    Attachment,
    Checklist,
    Dataset,
    FileType,
    Log,
    LogSet,
    NominalClient,
    Run,
    User,
    Video,
    Workbook,
    poll_until_ingestion_completed,
)
from nominal.core._constants import DEFAULT_API_BASE_URL
from nominal.core.connection import StreamingConnection
from nominal.core.data_review import DataReview, DataReviewBuilder

if TYPE_CHECKING:
    import pandas as pd
    import polars as pl


# global variable which `set_base_url()` modifies
_global_base_url = DEFAULT_API_BASE_URL


@cache
def _get_or_create_connection(base_url: str, token: str) -> NominalClient:
    return NominalClient.create(base_url, token)


@typing_extensions.deprecated(
    "nominal.set_base_url is deprecated and will be removed in a future version. "
    "Use `nominal.NominalClient.from_profile` instead, see https://docs.nominal.io/python/profile-migration"
)
def set_base_url(base_url: str) -> None:
    """Set the default Nominal platform base url.

    For production environments: "https://api.gov.nominal.io/api".
    For staging environments: "https://api-staging.gov.nominal.io/api".
    For local development: "https://api.nominal.test".
    """
    _config.get_token(base_url)

    global _global_base_url
    _global_base_url = base_url


@typing_extensions.deprecated(
    "nominal.set_token is deprecated and will be removed in a future version. "
    "Use `nominal.NominalClient.from_profile` instead, see https://docs.nominal.io/python/profile-migration"
)
def set_token(base_url: str, token: str) -> None:
    """Set the default token to be used in association with a given base url.

    Use in conjunction with `set_base_url()`.
    """
    _config.set_token(base_url, token)


def _get_default_client() -> NominalClient:
    token = _config.get_token(_global_base_url)
    return _get_or_create_connection(_global_base_url, token)


@typing_extensions.deprecated(
    "nominal.get_default_client is deprecated and will be removed in a future version. "
    "Use `nominal.NominalClient.from_profile` instead, see https://docs.nominal.io/python/profile-migration"
)
def get_default_client() -> NominalClient:
    """Retrieve the default client to the Nominal platform."""
    return _get_default_client()


@typing_extensions.deprecated(
    "nominal.get_user is deprecated and will be removed in a future version. "
    "Use `nominal.NominalClient.get_user` instead, see https://docs.nominal.io/python/profile-migration"
)
def get_user() -> User:
    """Retrieve current user."""
    client = _get_default_client()
    return client.get_user()


@typing_extensions.deprecated(
    "`nominal.upload_tdms` is deprecated and will be removed in a future version. "
    "Use `nominal.thirdparty.tdms.upload_tdms` instead."
)
def upload_tdms(
    file: Path | str,
    name: str | None = None,
    description: str | None = None,
    timestamp_column: str | None = None,
    timestamp_type: ts._AnyTimestampType | None = None,
    *,
    wait_until_complete: bool = True,
) -> Dataset:
    """Create a dataset in the Nominal platform from a tdms file."""
    from nominal.thirdparty.tdms import upload_tdms

    client = _get_default_client()
    return upload_tdms(
        client, file, name, description, timestamp_column, timestamp_type, wait_until_complete=wait_until_complete
    )


@typing_extensions.deprecated(
    "`nominal.upload_pandas` is deprecated and will be removed in a future version. "
    "Use `nominal.thirdparty.pandas.upload_dataframe` instead."
)
def upload_pandas(
    df: pd.DataFrame,
    name: str,
    timestamp_column: str,
    timestamp_type: ts._AnyTimestampType,
    description: str | None = None,
    channel_name_delimiter: str | None = None,
    *,
    wait_until_complete: bool = True,
) -> Dataset:
    """Create a dataset in the Nominal platform from a pandas.DataFrame."""
    from nominal.thirdparty.pandas import upload_dataframe

    client = _get_default_client()
    return upload_dataframe(
        client,
        df,
        name,
        timestamp_column,
        timestamp_type,
        description,
        channel_name_delimiter,
        wait_until_complete=wait_until_complete,
    )


@typing_extensions.deprecated(
    "`nominal.upload_polars` is deprecated and will be removed in a future version. "
    "Use `nominal.thirdparty.pandas.upload_dataframe(df.to_pandas(), ...)` instead."
)
def upload_polars(
    df: pl.DataFrame,
    name: str,
    timestamp_column: str,
    timestamp_type: ts._AnyTimestampType,
    description: str | None = None,
    channel_name_delimiter: str | None = None,
    *,
    wait_until_complete: bool = True,
) -> Dataset:
    """Create a dataset in the Nominal platform from a polars.DataFrame."""
    from nominal.thirdparty.pandas import upload_dataframe

    client = _get_default_client()
    return upload_dataframe(
        client,
        df.to_pandas(),
        name,
        timestamp_column,
        timestamp_type,
        description,
        channel_name_delimiter,
        wait_until_complete=wait_until_complete,
    )


@typing_extensions.deprecated(
    "nominal.create_dataset is deprecated and will be removed in a future version. "
    "Use `nominal.NominalClient.create_dataset` instead, see https://docs.nominal.io/python/profile-migration"
)
def create_dataset(
    name: str,
    description: str | None = None,
    labels: Sequence[str] = (),
    properties: Mapping[str, str] | None = None,
    prefix_tree_delimiter: str | None = None,
) -> Dataset:
    """Create an empty dataset.

    Args:
        name: Name of the dataset to create in Nominal.
        description: Human readable description of the dataset.
        labels: Text labels to apply to the created dataset
        properties: Key-value properties to apply to the cleated dataset
        prefix_tree_delimiter: If present, the delimiter to represent tiers when viewing channels hierarchically.

    Returns:
        Reference to the created dataset in Nominal.
    """
    client = _get_default_client()
    return client.create_dataset(
        name, description=description, labels=labels, properties=properties, prefix_tree_delimiter=prefix_tree_delimiter
    )


@typing_extensions.deprecated(
    "`nominal.upload_csv` is deprecated and will be removed in a future version. "
    "Use `nominal.create_dataset` or `nominal.get_dataset`, add data to an existing dataset instead."
)
def upload_csv(
    file: Path | str,
    name: str | None,
    timestamp_column: str,
    timestamp_type: ts._AnyTimestampType,
    description: str | None = None,
    channel_name_delimiter: str | None = None,
    *,
    wait_until_complete: bool = True,
) -> Dataset:
    """Create a dataset in the Nominal platform from a .csv or .csv.gz file.

    If `name` is None, the dataset is created with the name of the file.

    If `wait_until_complete=True` (the default), this function waits until the dataset has completed ingestion before
        returning. If you are uploading many datasets, set `wait_until_complete=False` instead and call
        `wait_until_ingestions_complete()` after uploading all datasets to allow for parallel ingestion.
    """
    client = _get_default_client()
    return _upload_csv(
        client,
        file,
        name,
        timestamp_column,
        timestamp_type,
        description,
        channel_name_delimiter,
        wait_until_complete=wait_until_complete,
    )


def _upload_csv(
    client: NominalClient,
    file: Path | str,
    name: str | None,
    timestamp_column: str,
    timestamp_type: ts._AnyTimestampType,
    description: str | None = None,
    channel_name_delimiter: str | None = None,
    *,
    wait_until_complete: bool = True,
) -> Dataset:
    dataset = client.create_dataset(
        name if name else Path(file).name,
        description=description,
        prefix_tree_delimiter=channel_name_delimiter,
    )
    dataset.add_tabular_data(
        file,
        timestamp_column=timestamp_column,
        timestamp_type=timestamp_type,
    )
    if wait_until_complete:
        dataset.poll_until_ingestion_completed()
    return dataset


@typing_extensions.deprecated(
    "nominal.get_dataset is deprecated and will be removed in a future version. "
    "Use `nominal.NominalClient.get_dataset` instead, see https://docs.nominal.io/python/profile-migration"
)
def get_dataset(rid: str) -> Dataset:
    """Retrieve a dataset from the Nominal platform by its RID."""
    client = _get_default_client()
    return client.get_dataset(rid)


@typing_extensions.deprecated(
    "nominal.create_run is deprecated and will be removed in a future version. "
    "Use `nominal.NominalClient.create_run` instead, see https://docs.nominal.io/python/profile-migration"
)
def create_run(
    name: str,
    start: datetime | str | ts.IntegralNanosecondsUTC,
    end: datetime | str | ts.IntegralNanosecondsUTC | None,
    description: str | None = None,
    *,
    properties: Mapping[str, str] | None = None,
    labels: Sequence[str] = (),
    attachments: Iterable[Attachment] | Iterable[str] = (),
) -> Run:
    """Create a run in the Nominal platform.

    If the run has no end (for example, if it is ongoing), use `end=None`.

    To add a dataset to the run, use `run.add_dataset()`.
    """
    client = _get_default_client()
    return client.create_run(
        name,
        start=ts._SecondsNanos.from_flexible(start).to_nanoseconds(),
        end=None if end is None else ts._SecondsNanos.from_flexible(end).to_nanoseconds(),
        description=description,
        properties=properties,
        labels=labels,
        attachments=attachments,
    )


@typing_extensions.deprecated(
    "nominal.create_run_csv is deprecated and will be removed in a future version. "
    "Use `nominal.NominalClient.create_dataset` and `nominal.NominalClient.create_run` instead, "
    "see https://docs.nominal.io/python/profile-migration"
)
def create_run_csv(
    file: Path | str,
    name: str,
    timestamp_column: str,
    timestamp_type: ts._LiteralAbsolute | ts.Iso8601 | ts.Epoch,
    description: str | None = None,
) -> Run:
    """Create a dataset from a CSV file, and create a run based on it.

    This is a convenience function that combines `upload_csv()` and `create_run()`.
    """
    dataset = upload_csv(file, f"Dataset for Run: {name}", timestamp_column, timestamp_type)
    dataset.refresh()
    assert dataset.bounds is not None
    run = create_run(name, start=dataset.bounds.start, end=dataset.bounds.end, description=description)
    run.add_dataset("dataset", dataset)
    return run


@typing_extensions.deprecated(
    "nominal.get_run is deprecated and will be removed in a future version. "
    "Use `nominal.NominalClient.get_run` instead, see https://docs.nominal.io/python/profile-migration"
)
def get_run(rid: str) -> Run:
    """Retrieve a run from the Nominal platform by its RID."""
    client = _get_default_client()
    return client.get_run(rid)


@typing_extensions.deprecated(
    "nominal.search_runs is deprecated and will be removed in a future version. "
    "Use `nominal.NominalClient.search_runs` instead, see https://docs.nominal.io/python/profile-migration"
)
def search_runs(
    *,
    start: str | datetime | ts.IntegralNanosecondsUTC | None = None,
    end: str | datetime | ts.IntegralNanosecondsUTC | None = None,
    name_substring: str | None = None,
    labels: Sequence[str] | None = None,
    properties: Mapping[str, str] | None = None,
) -> Sequence[Run]:
    """Search for runs meeting the specified filters.
    Filters are ANDed together, e.g. `(labels in run.labels) AND (run.end <= end)`

    Args:
        start: Inclusive start time for filtering runs.
        end: Inclusive end time for filtering runs.
        name_substring: Searches for a (case-insensitive) substring in the name
        labels: A sequence of labels that must ALL be present on a run to be included.
        properties: A mapping of key-value pairs that must ALL be present on a run to be included.

    Returns:
        All runs which match all of the provided conditions
    """
    client = _get_default_client()
    return client.search_runs(start=start, end=end, name_substring=name_substring, labels=labels, properties=properties)


@typing_extensions.deprecated(
    "nominal.upload_attachment is deprecated and will be removed in a future version. "
    "Use `nominal.NominalClient.create_attachment` instead, see https://docs.nominal.io/python/profile-migration"
)
def upload_attachment(
    file: Path | str,
    name: str,
    description: str | None = None,
) -> Attachment:
    """Upload an attachment to the Nominal platform."""
    path = Path(file)
    client = _get_default_client()
    file_type = FileType.from_path(path)
    with open(path, "rb") as f:
        return client.create_attachment_from_io(f, name, file_type, description)


@typing_extensions.deprecated(
    "nominal.get_attachment is deprecated and will be removed in a future version. "
    "Use `nominal.NominalClient.get_attachment` instead, see https://docs.nominal.io/python/profile-migration"
)
def get_attachment(rid: str) -> Attachment:
    """Retrieve an attachment from the Nominal platform by its RID."""
    client = _get_default_client()
    return client.get_attachment(rid)


@typing_extensions.deprecated(
    "LogSets are deprecated and will be removed in a future version. "
    "Add logs to an existing dataset with dataset.write_logs instead."
)
def get_log_set(rid: str) -> LogSet:
    """Retrieve a log set from the Nominal platform by its RID."""
    client = _get_default_client()
    return client.get_log_set(rid)


@typing_extensions.deprecated(
    "nominal.download_attachment is deprecated and will be removed in a future version. "
    "Use `nominal.NominalClient.get_attachment` and `nominal.core.Attachment.write` instead, "
    "see https://docs.nominal.io/python/profile-migration"
)
def download_attachment(rid: str, file: Path | str) -> None:
    """Retrieve an attachment from the Nominal platform and save it to `file`."""
    client = _get_default_client()
    attachment = client.get_attachment(rid)
    attachment.write(Path(file))


@typing_extensions.deprecated(
    "nominal.upload_video is deprecated and will be removed in a future version. "
    "Use `nominal.NominalClient.create_video` instead, see https://docs.nominal.io/python/profile-migration"
)
def upload_video(
    file: Path | str, name: str, start: datetime | str | ts.IntegralNanosecondsUTC, description: str | None = None
) -> Video:
    """Upload a video to Nominal from a file."""
    client = _get_default_client()
    path = Path(file)
    file_type = FileType.from_path(path)
    video = client.create_empty_video(
        name=name,
        description=description,
    )
    with open(file, "rb") as f:
        video.add_from_io(
            f,
            name,
            start=ts._SecondsNanos.from_flexible(start).to_nanoseconds(),
            description=description,
            file_type=file_type,
        )
    return video


@typing_extensions.deprecated(
    "nominal.get_video is deprecated and will be removed in a future version. "
    "Use `nominal.NominalClient.get_video` instead, see https://docs.nominal.io/python/profile-migration"
)
def get_video(rid: str) -> Video:
    """Retrieve a video from the Nominal platform by its RID."""
    client = _get_default_client()
    return client.get_video(rid)


@typing_extensions.deprecated(
    "nominal.create_asset is deprecated and will be removed in a future version. "
    "Use `nominal.NominalClient.create_asset` instead, see https://docs.nominal.io/python/profile-migration"
)
def create_asset(
    name: str,
    description: str | None = None,
    *,
    properties: Mapping[str, str] | None = None,
    labels: Sequence[str] = (),
) -> Asset:
    """Create an asset."""
    client = _get_default_client()
    return client.create_asset(name, description, properties=properties, labels=labels)


@typing_extensions.deprecated(
    "nominal.get_asset is deprecated and will be removed in a future version. "
    "Use `nominal.NominalClient.get_asset` instead, see https://docs.nominal.io/python/profile-migration"
)
def get_asset(rid: str) -> Asset:
    """Retrieve an asset by its RID."""
    client = _get_default_client()
    return client.get_asset(rid)


@typing_extensions.deprecated(
    "nominal.search_assets is deprecated and will be removed in a future version. "
    "Use `nominal.NominalClient.search_assets` instead, see https://docs.nominal.io/python/profile-migration"
)
def search_assets(
    *,
    search_text: str | None = None,
    labels: Sequence[str] | None = None,
    properties: Mapping[str, str] | None = None,
) -> Sequence[Asset]:
    """Search for assets meeting the specified filters.
    Filters are ANDed together, e.g. `(asset.label == label) AND (asset.search_text =~ field)`

    Args:
        search_text: case-insensitive search for any of the keywords in all string fields
        labels: A sequence of labels that must ALL be present on a asset to be included.
        properties: A mapping of key-value pairs that must ALL be present on a asset to be included.

    Returns:
        All assets which match all of the provided conditions
    """
    client = _get_default_client()
    return client.search_assets(search_text=search_text, labels=labels, properties=properties)


@typing_extensions.deprecated(
    "nominal.list_streaming_checklists is deprecated and will be removed in a future version. "
    "Use `nominal.NominalClient.list_streaming_checklists` instead, see https://docs.nominal.io/python/profile-migration"
)
def list_streaming_checklists(asset: Asset | str | None = None) -> Iterable[str]:
    """List all Streaming Checklists.

    Args:
        asset: if provided, only return checklists associated with the given asset.
    """
    client = _get_default_client()
    return client.list_streaming_checklists(asset)


@typing_extensions.deprecated(
    "nominal.wait_until_ingestions_complete is deprecated and will be removed in a future version. "
    "Use `nominal.NominalClient.get_dataset` and `nominal.core.Dataset.poll_until_ingestion_complete` "
    "instead, see https://docs.nominal.io/python/profile-migration"
)
def wait_until_ingestions_complete(datasets: list[Dataset]) -> None:
    """Wait until all datasets have completed ingestion.

    If you are uploading multiple datasets, consider setting wait_until_complete=False in the upload functions and call
    this function after uploading all datasets to wait until ingestion completes. This allows for parallel ingestion.
    """
    poll_until_ingestion_completed(datasets)


@typing_extensions.deprecated(
    "nominal.get_checklist is deprecated and will be removed in a future version. "
    "Use `nominal.NominalClient.get_checklist` instead, see https://docs.nominal.io/python/profile-migration"
)
def get_checklist(checklist_rid: str) -> Checklist:
    client = _get_default_client()
    return client.get_checklist(checklist_rid)


@typing_extensions.deprecated(
    "nominal.upload_mcap_video is deprecated and will be removed in a future version. "
    "Use `nominal.NominalClient.create_mcap_video` instead, see https://docs.nominal.io/python/profile-migration"
)
def upload_mcap_video(
    file: Path | str,
    topic: str,
    name: str | None = None,
    description: str | None = None,
    *,
    wait_until_complete: bool = True,
) -> Video:
    """Create a video in the Nominal platform from a topic in a mcap file.

    If `name` is None, the video is created with the name of the file.

    If `wait_until_complete=True` (the default), this function waits until the video has completed ingestion before
        returning. If you are uploading many videos, set `wait_until_complete=False` instead and call
        `wait_until_ingestion_complete()` after uploading all videos to allow for parallel ingestion.
    """
    client = _get_default_client()

    path = Path(file)
    file_type = FileType.from_path(path)
    if name is None:
        name = path.name

    with open(file, "rb") as f:
        video = client.create_video_from_mcap_io(
            f,
            topic,
            name,
            description,
            file_type,
        )
    if wait_until_complete:
        video.poll_until_ingestion_completed()
    return video


@typing_extensions.deprecated(
    "nominal.create_streaming_connection is deprecated and will be removed in a future version. "
    "Use `nominal.NominalClient.create_streaming_connection` instead, see https://docs.nominal.io/python/profile-migration"
)
def create_streaming_connection(
    datasource_id: str,
    connection_name: str,
    datasource_description: str | None = None,
    *,
    required_tag_names: list[str] | None = None,
) -> StreamingConnection:
    """Creates a new datasource and a new connection.

    datasource_id: A human readable identifier. Must be unique within an organization.
    """
    client = _get_default_client()
    return client.create_streaming_connection(
        datasource_id, connection_name, datasource_description, required_tag_names=required_tag_names
    )


@typing_extensions.deprecated(
    "nominal.get_connection is deprecated and will be removed in a future version. "
    "Use `nominal.NominalClient.get_connection` instead, see https://docs.nominal.io/python/profile-migration"
)
def get_connection(rid: str) -> Connection:
    """Retrieve a connection from the Nominal platform by its RID."""
    client = _get_default_client()
    return client.get_connection(rid)


@typing_extensions.deprecated(
    "nominal.create_workbook_from_template is deprecated and will be removed in a future version. "
    "Use `nominal.NominalClient.create_workbook_from_template` instead, see https://docs.nominal.io/python/profile-migration"
)
def create_workbook_from_template(
    template_rid: str, run_rid: str, *, title: str | None = None, description: str | None = None, is_draft: bool = False
) -> Workbook:
    """Creates a new workbook from a template.
    template_rid: The template to use for the workbook.
    run_rid: The run to associate the workbook with.
    """
    client = _get_default_client()
    return client.create_workbook_from_template(template_rid, run_rid, title, description, is_draft)


@typing_extensions.deprecated(
    "LogSets are deprecated and will be removed in a future version. "
    "Add logs to an existing dataset with dataset.write_logs instead."
)
def create_log_set(
    name: str,
    logs: Iterable[Log] | Iterable[tuple[datetime | ts.IntegralNanosecondsUTC, str]],
    timestamp_type: ts.LogTimestampType = "absolute",
    description: str | None = None,
) -> LogSet:
    """Create an immutable log set with the given logs.

    The logs are attached during creation and cannot be modified afterwards. Logs can either be of type `Log`
    or a tuple of a timestamp and a string. Timestamp type must be either 'absolute' or 'relative'.
    """
    client = _get_default_client()
    return client.create_log_set(name, logs, timestamp_type, description)


@typing_extensions.deprecated(
    "nominal.data_review_builder is deprecated and will be removed in a future version. "
    "Use `nominal.NominalClient.data_review_builder` instead, see https://docs.nominal.io/python/profile-migration"
)
def data_review_builder() -> DataReviewBuilder:
    """Create a batch of data reviews to be initiated together.

    Example:
    -------
    ```python
    builder = nm.data_review_builder()
    builder.add_integration("integration_rid")
    builder.add_request("run_rid_1", "checklist_rid_1", "commit_1")
    builder.add_request("run_rid_2", "checklist_rid_2", "commit_2")
    reviews = builder.initiate()

    for review in reviews:
        print(review.get_violations())
    ```
    """
    client = _get_default_client()
    return client.data_review_builder()


@typing_extensions.deprecated(
    "nominal.get_data_review is deprecated and will be removed in a future version. "
    "Use `nominal.NominalClient.get_data_review` instead, see https://docs.nominal.io/python/profile-migration"
)
def get_data_review(rid: str) -> DataReview:
    """Retrieve a data review from the Nominal platform by its RID."""
    client = _get_default_client()
    return client.get_data_review(rid)
