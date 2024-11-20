from __future__ import annotations

from datetime import datetime
from functools import cache
from pathlib import Path
from threading import Thread
from typing import TYPE_CHECKING, BinaryIO, Iterable, Mapping, Sequence

from nominal import Connection, _config, ts
from nominal._utils import FileType, FileTypes, deprecate_keyword_argument, reader_writer
from nominal.core import (
    Asset,
    Attachment,
    Checklist,
    ChecklistBuilder,
    Dataset,
    Log,
    LogSet,
    NominalClient,
    Run,
    User,
    Video,
    Workbook,
    poll_until_ingestion_completed,
)

if TYPE_CHECKING:
    import pandas as pd
    import polars as pl


_DEFAULT_BASE_URL = "https://api.gov.nominal.io/api"

# global variable which `set_base_url()` modifies
_global_base_url = _DEFAULT_BASE_URL


@cache
def _get_or_create_connection(base_url: str, token: str) -> NominalClient:
    return NominalClient.create(base_url, token)


def set_base_url(base_url: str) -> None:
    """Set the default Nominal platform base url.

    For production environments: "https://api.gov.nominal.io/api".
    For staging environments: "https://api-staging.gov.nominal.io/api".
    For local development: "https://api.nominal.test".
    """
    _config.get_token(base_url)

    global _global_base_url
    _global_base_url = base_url


def set_token(base_url: str, token: str) -> None:
    """Set the default token to be used in association with a given base url.

    Use in conjunction with `set_base_url()`.
    """
    _config.set_token(base_url, token)


def get_default_client() -> NominalClient:
    """Retrieve the default client to the Nominal platform."""
    token = _config.get_token(_global_base_url)
    return _get_or_create_connection(_global_base_url, token)


def get_user() -> User:
    """Retrieve the user associated with the default client."""
    conn = get_default_client()
    return conn.get_user()


def upload_tdms(
    file: Path | str, name: str | None = None, description: str | None = None, *, wait_until_complete: bool = True
) -> Dataset:
    """Create a dataset in the Nominal platform from a tdms file.

    TDMS channel properties must have both a `wf_increment` and `wf_start_time` property to be included in the dataset.

    Channels will be named as f"{group_name}.{channel_name}" with spaces replaced with underscores.

    If `name` is None, the dataset is created with the name of the file with a .csv suffix.

    If `wait_until_complete=True` (the default), this function waits until the dataset has completed ingestion before
        returning. If you are uploading many datasets, set `wait_until_complete=False` instead and call
        `wait_until_ingestions_complete()` after uploading all datasets to allow for parallel ingestion.
    """
    import numpy as np
    import pandas as pd
    from nptdms import TdmsChannel, TdmsFile, TdmsGroup

    path = Path(file)
    with TdmsFile.open(path) as tdms_file:
        # identify channels to extract
        channels_to_export: dict[str, TdmsChannel] = {}
        group: TdmsGroup
        for group in tdms_file.groups():
            channel: TdmsChannel
            for channel in group.channels():
                # some channels may not have the required properties to construct a time track
                if ("wf_increment" in channel.properties) and ("wf_start_time" in channel.properties):
                    channel_name = f"{channel.group_name.replace(' ', '_')}.{channel.name.replace(' ', '_')}"
                    channels_to_export[channel_name] = channel

        df = pd.DataFrame.from_dict(
            {
                channel_name: pd.Series(
                    data=channel.read_data(), index=channel.time_track(absolute_time=True, accuracy="ns")
                )
                for channel_name, channel in channels_to_export.items()
            }
        )

        # format for nominal upload
        time_column = "time_ns"
        df.index = df.index.set_names(time_column, level=None)
        df = df.reset_index()
        df[time_column] = df[time_column].astype(np.int64)

        return upload_pandas(
            df=df,
            name=name if name is not None else path.with_suffix(".csv").name,
            description=description,
            timestamp_column=time_column,
            timestamp_type=ts.EPOCH_NANOSECONDS,
            wait_until_complete=wait_until_complete,
        )


def upload_pandas(
    df: pd.DataFrame,
    name: str,
    timestamp_column: str,
    timestamp_type: ts._AnyTimestampType,
    description: str | None = None,
    *,
    wait_until_complete: bool = True,
) -> Dataset:
    """Create a dataset in the Nominal platform from a pandas.DataFrame.

    If `wait_until_complete=True` (the default), this function waits until the dataset has completed ingestion before
        returning. If you are uploading many datasets, set `wait_until_complete=False` instead and call
        `wait_until_ingestions_complete()` after uploading all datasets to allow for parallel ingestion.
    """
    conn = get_default_client()

    # TODO(alkasm): use parquet instead of CSV as an intermediary

    def write_and_close(df: pd.DataFrame, w: BinaryIO) -> None:
        df.to_csv(w)
        w.close()

    with reader_writer() as (reader, writer):
        # write the dataframe to CSV in another thread
        t = Thread(target=write_and_close, args=(df, writer))
        t.start()
        dataset = conn.create_dataset_from_io(
            reader,
            name,
            timestamp_column=timestamp_column,
            timestamp_type=timestamp_type,
            file_type=FileTypes.CSV,
            description=description,
        )
        t.join()
    if wait_until_complete:
        dataset.poll_until_ingestion_completed()
    return dataset


def upload_polars(
    df: pl.DataFrame,
    name: str,
    timestamp_column: str,
    timestamp_type: ts._AnyTimestampType,
    description: str | None = None,
    *,
    wait_until_complete: bool = True,
) -> Dataset:
    """Create a dataset in the Nominal platform from a polars.DataFrame.

    If `wait_until_complete=True` (the default), this function waits until the dataset has completed ingestion before
        returning. If you are uploading many datasets, set `wait_until_complete=False` instead and call
        `wait_until_ingestions_complete()` after uploading all datasets to allow for parallel ingestion.
    """
    conn = get_default_client()

    def write_and_close(df: pl.DataFrame, w: BinaryIO) -> None:
        df.write_csv(w)
        w.close()

    with reader_writer() as (reader, writer):
        # write the dataframe to CSV in another thread
        t = Thread(target=write_and_close, args=(df, writer))
        t.start()
        dataset = conn.create_dataset_from_io(
            reader,
            name,
            timestamp_column=timestamp_column,
            timestamp_type=timestamp_type,
            file_type=FileTypes.CSV,
            description=description,
        )
        t.join()
    if wait_until_complete:
        dataset.poll_until_ingestion_completed()
    return dataset


def upload_csv(
    file: Path | str,
    name: str | None,
    timestamp_column: str,
    timestamp_type: ts._AnyTimestampType,
    description: str | None = None,
    *,
    wait_until_complete: bool = True,
) -> Dataset:
    """Create a dataset in the Nominal platform from a .csv or .csv.gz file.

    If `name` is None, the dataset is created with the name of the file.

    If `wait_until_complete=True` (the default), this function waits until the dataset has completed ingestion before
        returning. If you are uploading many datasets, set `wait_until_complete=False` instead and call
        `wait_until_ingestions_complete()` after uploading all datasets to allow for parallel ingestion.
    """
    conn = get_default_client()
    return _upload_csv(
        conn, file, name, timestamp_column, timestamp_type, description, wait_until_complete=wait_until_complete
    )


def _upload_csv(
    conn: NominalClient,
    file: Path | str,
    name: str | None,
    timestamp_column: str,
    timestamp_type: ts._AnyTimestampType,
    description: str | None = None,
    *,
    wait_until_complete: bool = True,
) -> Dataset:
    dataset = conn.create_csv_dataset(
        file,
        name,
        timestamp_column=timestamp_column,
        timestamp_type=timestamp_type,
        description=description,
    )
    if wait_until_complete:
        dataset.poll_until_ingestion_completed()
    return dataset


def get_dataset(rid: str) -> Dataset:
    """Retrieve a dataset from the Nominal platform by its RID."""
    conn = get_default_client()
    return conn.get_dataset(rid)


def create_run(
    name: str,
    start: datetime | str | ts.IntegralNanosecondsUTC,
    end: datetime | str | ts.IntegralNanosecondsUTC | None,
    description: str | None = None,
) -> Run:
    """Create a run in the Nominal platform.

    If the run has no end (for example, if it is ongoing), use `end=None`.

    To add a dataset to the run, use `run.add_dataset()`.
    """
    conn = get_default_client()
    return conn.create_run(
        name,
        start=ts._SecondsNanos.from_flexible(start).to_nanoseconds(),
        end=None if end is None else ts._SecondsNanos.from_flexible(end).to_nanoseconds(),
        description=description,
    )


def create_run_csv(
    file: Path | str,
    name: str,
    timestamp_column: str,
    timestamp_type: ts._LiteralAbsolute | ts.Iso8601 | ts.Epoch,
    description: str | None = None,
) -> Run:
    """Create a dataset from a CSV file, and create a run based on it.

    This is a convenience function that combines `upload_csv()` and `create_run()` and can only be used with absolute
    timestamps. For relative timestamps or custom formats, use `upload_dataset()` and `create_run()` separately.

    The name and description are added to the run. The dataset is created with the name "Dataset for Run: {name}".
    The reference name for the dataset in the run is "dataset".

    The run start and end times are created from the minimum and maximum timestamps in the CSV file in the timestamp
    column.
    """
    ts_type = ts._to_typed_timestamp_type(timestamp_type)
    if not isinstance(ts_type, (ts.Iso8601, ts.Epoch)):
        raise ValueError(
            "`create_run_csv()` only supports iso8601 or epoch timestamps: use "
            "`upload_dataset()` and `create_run()` instead"
        )
    start, end = _get_start_end_timestamp_csv_file(file, timestamp_column, ts_type)
    dataset = upload_csv(file, f"Dataset for Run: {name}", timestamp_column, ts_type)
    run = create_run(name, start=start, end=end, description=description)
    run.add_dataset("dataset", dataset)
    return run


def get_run(rid: str) -> Run:
    """Retrieve a run from the Nominal platform by its RID."""
    conn = get_default_client()
    return conn.get_run(rid)


@deprecate_keyword_argument("name_substring", "exact_name")
def search_runs(
    *,
    start: str | datetime | ts.IntegralNanosecondsUTC | None = None,
    end: str | datetime | ts.IntegralNanosecondsUTC | None = None,
    name_substring: str | None = None,
    label: str | None = None,
    property: tuple[str, str] | None = None,
) -> list[Run]:
    """Search for runs meeting the specified filters.

    Filters are ANDed together, e.g. `(run.label == label) AND (run.end <= end)`
    - `start` and `end` times are both inclusive
    - `name_substring`: search for a (case-insensitive) substring in the name
    - `property` is a key-value pair, e.g. ("name", "value")
    """
    if all([v is None for v in (start, end, name_substring, label, property)]):
        raise ValueError("must provide one of: start, end, name_substring, label, or property")
    conn = get_default_client()
    runs = conn.search_runs(
        start=None if start is None else ts._SecondsNanos.from_flexible(start).to_nanoseconds(),
        end=None if end is None else ts._SecondsNanos.from_flexible(end).to_nanoseconds(),
        name_substring=name_substring,
        label=label,
        property=property,
    )
    return list(runs)


def upload_attachment(
    file: Path | str,
    name: str,
    description: str | None = None,
) -> Attachment:
    """Upload an attachment to the Nominal platform."""
    path = Path(file)
    conn = get_default_client()
    file_type = FileType.from_path(path)
    with open(path, "rb") as f:
        return conn.create_attachment_from_io(f, name, file_type, description)


def get_attachment(rid: str) -> Attachment:
    """Retrieve an attachment from the Nominal platform by its RID."""
    conn = get_default_client()
    return conn.get_attachment(rid)


def get_log_set(rid: str) -> LogSet:
    """Retrieve a log set from the Nominal platform by its RID."""
    conn = get_default_client()
    return conn.get_log_set(rid)


def download_attachment(rid: str, file: Path | str) -> None:
    """Retrieve an attachment from the Nominal platform and save it to `file`."""
    conn = get_default_client()
    attachment = conn.get_attachment(rid)
    attachment.write(Path(file))


def upload_video(
    file: Path | str, name: str, start: datetime | str | ts.IntegralNanosecondsUTC, description: str | None = None
) -> Video:
    """Upload a video to Nominal from a file."""
    conn = get_default_client()
    path = Path(file)
    file_type = FileType.from_path(path)
    with open(file, "rb") as f:
        return conn.create_video_from_io(
            f, name, ts._SecondsNanos.from_flexible(start).to_nanoseconds(), description, file_type
        )


def get_video(rid: str) -> Video:
    """Retrieve a video from the Nominal platform by its RID."""
    conn = get_default_client()
    return conn.get_video(rid)


def create_asset(
    name: str,
    description: str | None = None,
    *,
    properties: Mapping[str, str] | None = None,
    labels: Sequence[str] = (),
) -> Asset:
    """Create an asset."""
    conn = get_default_client()
    return conn.create_asset(name, description, properties=properties, labels=labels)


def get_asset(rid: str) -> Asset:
    """Retrieve an asset by its RID."""
    conn = get_default_client()
    return conn.get_asset(rid)


def search_assets(
    *,
    search_text: str | None = None,
    label: str | None = None,
    property: tuple[str, str] | None = None,
) -> list[Asset]:
    """Search for assets meeting the specified filters.

    Filters are ANDed together, e.g. `(asset.label == label) AND (asset.property == property)`
    - `search_text`: search case-insensitive for any of the keywords in all string fields.
    - `property` is a key-value pair, e.g. ("name", "value")
    """
    if all([v is None for v in (search_text, label, property)]):
        raise ValueError("must provide one of: start, end, search_text, label, or property")
    conn = get_default_client()
    assets = conn.search_assets(
        search_text=search_text,
        label=label,
        property=property,
    )
    return list(assets)


def wait_until_ingestions_complete(datasets: list[Dataset]) -> None:
    """Wait until all datasets have completed ingestion.

    If you are uploading multiple datasets, consider setting wait_until_complete=False in the upload functions and call
    this function after uploading all datasets to wait until ingestion completes. This allows for parallel ingestion.
    """
    poll_until_ingestion_completed(datasets)


def _get_start_end_timestamp_csv_file(
    file: Path | str,
    timestamp_column: str,
    timestamp_type: ts.Iso8601 | ts.Epoch,
) -> tuple[ts.IntegralNanosecondsUTC, ts.IntegralNanosecondsUTC]:
    import pandas as pd

    df = pd.read_csv(file)
    ts_col = df[timestamp_column]

    if isinstance(timestamp_type, ts.Iso8601):
        ts_col = pd.to_datetime(ts_col)
    elif isinstance(timestamp_type, ts.Epoch):
        pd_units: dict[ts._LiteralTimeUnit, str] = {
            "hours": "s",  # hours are not supported by pandas
            "minutes": "s",  # minutes are not supported by pandas
            "seconds": "s",
            "milliseconds": "ms",
            "microseconds": "us",
            "nanoseconds": "ns",
        }
        if timestamp_type.unit == "hours":
            ts_col *= 60 * 60
        elif timestamp_type.unit == "minutes":
            ts_col *= 60
        ts_col = pd.to_datetime(ts_col, unit=pd_units[timestamp_type.unit])
    else:
        raise ValueError(f"unhandled timestamp type {timestamp_type}")

    start, end = ts_col.min(), ts_col.max()
    return (
        ts.IntegralNanosecondsUTC(start.to_datetime64().astype(int)),
        ts.IntegralNanosecondsUTC(end.to_datetime64().astype(int)),
    )


def checklist_builder(
    name: str,
    description: str = "",
    assignee_email: str | None = None,
    default_ref_name: str | None = None,
) -> ChecklistBuilder:
    """Create a checklist builder to add checks and variables, and publish the checklist to Nominal.

    If assignee_email is None, the checklist is assigned to the user executing the code.

    Example:
    -------
    ```python
    builder = nm.checklist_builder("Programmatically created checklist")
    builder.add_check(
        name="derivative of cycle time is too high",
        priority=2,
        expression="derivative(numericChannel(channelName = 'Cycle_Time', refName = 'manufacturing')) > 0.05",
    )
    checklist = builder.publish()
    ```

    """
    conn = get_default_client()
    return conn.checklist_builder(
        name=name,
        description=description,
        assignee_email=assignee_email,
        default_ref_name=default_ref_name,
    )


def get_checklist(checklist_rid: str) -> Checklist:
    conn = get_default_client()
    return conn.get_checklist(checklist_rid)


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
    conn = get_default_client()

    path = Path(file)
    file_type = FileType.from_path(path)
    if name is None:
        name = path.name

    with open(file, "rb") as f:
        video = conn.create_video_from_mcap_io(
            f,
            topic,
            name,
            description,
            file_type,
        )
    if wait_until_complete:
        video.poll_until_ingestion_completed()
    return video


def create_streaming_connection(
    datasource_id: str, connection_name: str, datasource_description: str | None = None
) -> Connection:
    """Creates a new datasource and a new connection.

    datasource_id: A human readable identifier. Must be unique within an organization.
    """
    conn = get_default_client()
    return conn.create_streaming_connection(datasource_id, connection_name, datasource_description)


def get_connection(rid: str) -> Connection:
    """Retrieve a connection from the Nominal platform by its RID."""
    conn = get_default_client()
    return conn.get_connection(rid)


def create_workbook_from_template(
    template_rid: str, run_rid: str, *, title: str | None = None, description: str | None = None, is_draft: bool = False
) -> Workbook:
    """Creates a new workbook from a template.
    template_rid: The template to use for the workbook.
    run_rid: The run to associate the workbook with.
    """
    conn = get_default_client()
    return conn.create_workbook_from_template(template_rid, run_rid, title, description, is_draft)


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
    conn = get_default_client()
    return conn.create_log_set(name, logs, timestamp_type, description)
