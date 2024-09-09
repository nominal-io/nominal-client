from __future__ import annotations

import shutil
from datetime import datetime
from pathlib import Path
from threading import Thread
from typing import TYPE_CHECKING, BinaryIO, Mapping, Sequence

import dateutil.parser

from .exceptions import NominalError
from .sdk import Attachment, Dataset, NominalClient, Run
from ._utils import (
    FileType,
    FileTypes,
    IntegralNanosecondsUTC,
    TimestampColumnType,
    _datetime_to_integral_nanoseconds,
    reader_writer,
)

if TYPE_CHECKING:
    import pandas as pd
    import polars as pl


_default_connection: NominalClient | None = None


def set_default_connection(base_url: str, token: str) -> None:
    """Set the default global connection to the Nominal platform.

    base_url: The URL of the Nominal API platform, e.g. "https://api.gov.nominal.io/api".
    token: An API token to authenticate with. You can grab a client token from the Nominal sandbox, e.g.
        at https://app.gov.nominal.io/sandbox.
    """
    global _default_connection
    _default_connection = NominalClient.create(base_url, token)


def get_default_connection() -> NominalClient:
    """Retrieve the default global connection to the Nominal platform.

    Raises nominal.exceptions.NominalError if no global connection has been set.
    """
    global _default_connection
    if _default_connection is None:
        raise NominalError("No default connection set: initialize with `set_default_connection(base_url, token)`")
    return _default_connection


def upload_pandas(
    df: pd.DataFrame,
    name: str,
    timestamp_column: str,
    timestamp_type: TimestampColumnType,
    description: str | None = None,
    *,
    wait_until_complete: bool = True,
) -> Dataset:
    """Create a dataset in the Nominal platform from a pandas.DataFrame."""
    conn = get_default_connection()

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
    timestamp_type: TimestampColumnType,
    description: str | None = None,
    *,
    wait_until_complete: bool = True,
) -> Dataset:
    """Create a dataset in the Nominal platform from a polars.DataFrame."""
    conn = get_default_connection()

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


def upload_dataset(
    path: Path | str,
    name: str,
    timestamp_column: str,
    timestamp_type: TimestampColumnType,
    description: str | None = None,
    *,
    wait_until_complete: bool = True,
) -> Dataset:
    """Create a dataset in the Nominal platform from a .csv, .csv.gz, or .parquet file."""
    path = Path(path)
    file_type = FileType.from_path_dataset(path)
    conn = get_default_connection()
    with open(path, "rb") as f:
        dataset = conn.create_dataset_from_io(
            f,
            name,
            timestamp_column=timestamp_column,
            timestamp_type=timestamp_type,
            file_type=file_type,
            description=description,
        )
    if wait_until_complete:
        dataset.poll_until_ingestion_completed()
    return dataset


def get_dataset_by_rid(rid: str) -> Dataset:
    """Retrieve a dataset from the Nominal platform by its RID."""
    conn = get_default_connection()
    return conn.get_dataset(rid)


def update_dataset(
    dataset: Dataset,
    *,
    name: str | None = None,
    description: str | None = None,
    properties: Mapping[str, str] | None = None,
    labels: Sequence[str] | None = None,
) -> Dataset:
    """Update dataset metadata.
    Updates the current instance and returns it.

    Note: This replaces the metadata (rather than adding to it).
    """
    return dataset.update(name=name, description=description, properties=properties, labels=labels)


def create_run(
    title: str,
    description: str,
    start: datetime | str | IntegralNanosecondsUTC,
    end: datetime | str | IntegralNanosecondsUTC,
) -> Run:
    conn = get_default_connection()
    return conn.create_run(
        title,
        description,
        start=_parse_timestamp(start),
        end=_parse_timestamp(end),
    )


def get_run_by_rid(rid: str) -> Run:
    """Retrieve a run from the Nominal platform by its RID."""
    conn = get_default_connection()
    return conn.get_run(rid)


def search_runs(
    *,
    start: str | datetime | IntegralNanosecondsUTC | None = None,
    end: str | datetime | IntegralNanosecondsUTC | None = None,
    exact_title: str | None = None,
    label: str | None = None,
    property: tuple[str, str] | None = None,
) -> list[Run]:
    """Search for runs meeting the specified filters.

    Filters are ANDed together, e.g. `(run.label == label) AND (run.end <= end)`
    - `start` and `end` times are both inclusive
    - `exact_title` is case-insensitive
    - `property` is a key-value pair, e.g. ("name", "value")
    """
    conn = get_default_connection()
    runs = conn.search_runs(
        start=None if start is None else _parse_timestamp(start),
        end=None if end is None else _parse_timestamp(end),
        exact_title=exact_title,
        label=label,
        property=property,
    )
    return list(runs)


def update_run(
    run: Run,
    *,
    title: str | None = None,
    description: str | None = None,
    properties: Mapping[str, str] | None = None,
    labels: Sequence[str] | None = None,
) -> Run:
    """Update run metadata.
    Updates the current instance and returns it.

    Note: This replaces the metadata (rather than adding to it).
    """
    return run.update(title=title, description=description, properties=properties, labels=labels)


def add_dataset_to_run(ref_name: str, dataset: Dataset, run: Run) -> None:
    """Add a dataset to this run.

    Datasets map "ref names" (their name within the run) to a Dataset (or dataset rid). The same type of datasets
    should use the same ref name across runs, since checklists and templates use ref names to reference datasets.
    """
    _ensure_same_clients(dataset, run)
    run.add_dataset(ref_name, dataset)


def list_datasets_for_run(run: Run) -> list[tuple[str, Dataset]]:
    """List the datasets associated with this run.
    Returns (ref_name, dataset) pairs.
    """
    return list(run.list_datasets())


def add_attachment_to_run(attachment: Attachment, run: Run) -> None:
    """Add attachments that have already been uploaded to the run."""
    run.add_attachments([attachment])


def list_attachments_for_run(run: Run) -> list[Attachment]:
    """List attachments that have been associated to the run."""
    return list(run.list_attachments())


def upload_attachment(
    path: Path | str,
    title: str,
    description: str,
) -> Attachment:
    """Upload an attachment to the Nominal platform."""
    conn = get_default_connection()
    file_type = FileType.from_path(Path(path))
    with open(path, "rb") as f:
        return conn.create_attachment_from_io(f, title, description, file_type)


def get_attachment_by_rid(rid: str) -> Attachment:
    """Retrieve an attachment from the Nominal platform by its RID."""
    conn = get_default_connection()
    return conn.get_attachment(rid)


def update_attachment(
    attachment: Attachment,
    *,
    title: str | None = None,
    description: str | None = None,
    properties: Mapping[str, str] | None = None,
    labels: Sequence[str] | None = None,
) -> Attachment:
    """Update attachment metadata.
    Updates the current instance and returns it.

    Note: This replaces the metadata (rather than adding to it).
    """
    return attachment.update(title=title, description=description, properties=properties, labels=labels)


def save_attachment(attachment: Attachment, path: Path | str, mkdir: bool = True) -> None:
    """Save an attachment to the local filesystem.

    `path` should be the path you want to save to, i.e. a file, not a directory.
    """
    if mkdir:
        path.mkdir(exist_ok=True, parents=True)
    with open(path, "wb") as wf:
        shutil.copyfileobj(attachment.get_contents(), wf)


def _parse_timestamp(ts: str | datetime | IntegralNanosecondsUTC) -> IntegralNanosecondsUTC:
    if isinstance(ts, int):
        return ts
    if isinstance(ts, str):
        ts = dateutil.parser.parse(ts)
    return _datetime_to_integral_nanoseconds(ts)


def _ensure_same_clients(*objs: Dataset | Run | Attachment) -> None:
    if len(set(obj._client for obj in objs)) != 1:
        raise NominalError("All objects must be created with the same NominalClient")
