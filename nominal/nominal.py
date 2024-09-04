from __future__ import annotations

from io import BytesIO
import shutil
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Iterable, Mapping, Sequence, cast

import dateutil.parser

from .exceptions import NominalError
from .sdk import Attachment, Dataset, NominalClient, Run, _AllowedFileExtensions
from ._utils import IntegralNanosecondsUTC, TimestampColumnType, _datetime_to_seconds_nanos

import dateutil

if TYPE_CHECKING:
    import pandas as pd
    import polars as pl


_default_connection: NominalClient | None


def set_default_connection(base_url: str, token: str) -> None:
    global _default_connection
    _default_connection = NominalClient.create(base_url, token)


# TODO(alkasm): assert operations with N > 1 objects are using the same connection
def get_default_connection() -> NominalClient:
    if _default_connection is None:
        raise NominalError("No default connection set: initialize with `set_default_connection(base_url, token)`")
    return _default_connection


def upload_dataset_from_pandas(
    df: pd.DataFrame,
    name: str,
    description: str,
    timestamp_column: str,
    timestamp_type: TimestampColumnType,
    properties: Mapping[str, str] | None = None,
    labels: Sequence[str] = (),
) -> Dataset:
    conn = get_default_connection()
    # copy the dataframe as a CSV to an in-memory file-like object
    # TODO(alkasm): do something more efficient like a reader/writer over a FIFO
    # TODO(alkasm): use parquet instead of CSV as an intermediary
    f = BytesIO()
    df.to_csv(f)
    f.seek(0)
    return conn.create_dataset_from_io(
        f,
        name,
        timestamp_column_name=timestamp_column,
        timestamp_column_type=timestamp_type,
        file_extension=".csv",
        mimetype="text/csv",
        description=description,
        properties=properties,
        labels=labels,
    )


def upload_dataset_from_polars(
    df: pl.DataFrame,
    name: str,
    description: str,
    timestamp_column: str,
    timestamp_type: TimestampColumnType,
    properties: Mapping[str, str] | None = None,
    labels: Sequence[str] = (),
) -> Dataset:
    conn = get_default_connection()
    # copy the dataframe as a CSV to an in-memory file-like object
    f = BytesIO()
    df.write_csv(f)
    f.seek(0)
    return conn.create_dataset_from_io(
        f,
        name,
        timestamp_column_name=timestamp_column,
        timestamp_column_type=timestamp_type,
        file_extension=".csv",
        mimetype="text/csv",
        description=description,
        properties=properties,
        labels=labels,
    )


def upload_dataset(
    path: Path | str,
    name: str,
    description: str,
    timestamp_column: str,
    timestamp_type: TimestampColumnType,
    *,
    properties: Mapping[str, str] | None = None,
    labels: Sequence[str] = (),
) -> Dataset:
    path = Path(path)
    ext = ".".join(path.suffixes)
    if ext not in [".csv", ".csv.gz", ".parquet"]:
        raise ValueError("dataset files must be .csv, .csv.gz, or .parquet files")
    ext = cast(_AllowedFileExtensions, ext)
    conn = get_default_connection()
    with open(path, "rb") as f:
        return conn.create_dataset_from_io(
            f,
            name,
            timestamp_column_name=timestamp_column,
            timestamp_column_type=timestamp_type,
            file_extension=ext,
            description=description,
            properties=properties,
            labels=labels,
        )


def get_dataset_by_rid(rid: str) -> Dataset:
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
    return dataset.update(name=name, description=description, properties=properties, labels=labels)


def create_run(
    title: str,
    description: str,
    start: datetime | str | IntegralNanosecondsUTC,
    end: datetime | str | IntegralNanosecondsUTC | None = None,
    datasets: Mapping[str, Dataset] | None = None,
    *,
    properties: Mapping[str, str] | None = None,
    labels: Sequence[str] = (),
    attachments: Iterable[Attachment] = (),
) -> Run:
    conn = get_default_connection()
    return conn.create_run(
        title,
        description,
        start_time=_parse_timestamp(start),
        end_time=None if end is None else _parse_timestamp(end),
        datasets=datasets or {},
        properties=properties,
        labels=labels,
        attachments=attachments,
    )


def get_run_by_rid(rid: str) -> Run:
    conn = get_default_connection()
    return conn.get_run(rid)


def update_run(
    run: Run,
    *,
    title: str | None = None,
    description: str | None = None,
    properties: Mapping[str, str] | None = None,
    labels: Sequence[str] | None = None,
) -> Run:
    return run.update(title=title, description=description, properties=properties, labels=labels)


def add_dataset_to_run(ref_name: str, dataset: Dataset, run: Run) -> None:
    run.add_datasets({ref_name: dataset})


def list_datasets_for_run(run: Run) -> list[tuple[str, Dataset]]:
    return list(run.list_datasets())


def add_attachment_to_run(attachment: Attachment, run: Run) -> None:
    run.add_attachments([attachment])


def list_attachments_for_run(run: Run) -> list[Attachment]:
    return list(run.list_attachments())


def upload_attachment(
    path: Path | str,
    title: str,
    filename: str,
    description: str,
    *,
    properties: Mapping[str, str] | None = None,
    labels: Sequence[str] = (),
) -> Attachment:
    conn = get_default_connection()
    with open(path, "rb") as f:
        return conn.create_attachment_from_io(f, title, filename, description, properties=properties, labels=labels)


def get_attachment_by_rid(rid: str) -> Attachment:
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
    return attachment.update(title=title, description=description, properties=properties, labels=labels)


def save_attachment(attachment: Attachment, path: Path | str) -> None:
    with open(path, "wb") as wf:
        shutil.copyfileobj(attachment.get_contents(), wf)


def _parse_timestamp(ts: str | datetime | IntegralNanosecondsUTC) -> IntegralNanosecondsUTC:
    if isinstance(ts, int):
        return ts
    if isinstance(ts, str):
        ts = dateutil.parser.parse(ts)
    seconds, nanos = _datetime_to_seconds_nanos(ts)
    return seconds * 1_000_000_000 + nanos
