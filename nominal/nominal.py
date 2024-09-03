from __future__ import annotations
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Mapping, Sequence

from .exceptions import NominalError
from .sdk import Attachment, Dataset, NominalClient, Run
from ._utils import IntegralNanosecondsUTC, TimestampColumnType

if TYPE_CHECKING:
    import pandas as pd
    import polars as pl


"""Client"""

_default_connection: NominalClient | None


def set_default_connection(base_url: str, token: str) -> None:
    global _default_connection
    _default_connection = NominalClient.create(base_url, token)


# TODO(alkasm): assert operations with N > 1 objects are using the same connection
def get_default_connection() -> NominalClient:
    if _default_connection is None:
        raise NominalError("No default connection set: initialize with `set_default_connection(base_url, token)`")
    return _default_connection


"""Datasets"""


# TODO(alkasm): use parquet instead of CSV as an intermediary
def upload_dataset_from_pandas(
    df: pd.DataFrame,
    name: str,
    description: str,
    timestamp_column: str,
    timestamp_type: TimestampColumnType,
    labels: Sequence[str] | None = None,
    properties: Mapping[str, str] | None = None,
) -> Dataset: ...


def upload_dataset_from_polars(
    df: pl.DataFrame,
    name: str,
    description: str,
    timestamp_column: str,
    timestamp_type: TimestampColumnType,
    labels: Sequence[str] | None = None,
    properties: Mapping[str, str] | None = None,
) -> Dataset: ...


def upload_dataset(
    path: Path | str,
    name: str,
    description: str,
    timestamp_column: str,
    timestamp_type: TimestampColumnType,
    *,
    labels: Sequence[str] | None = None,
    properties: Mapping[str, str] | None = None,
) -> Dataset: ...


def get_dataset_by_rid(rid: str) -> Dataset: ...


def update_dataset(
    dataset: Dataset,
    *,
    name: str | None = None,
    description: str | None = None,
    properties: Mapping[str, str] | None = None,
    labels: Sequence[str] | None = None,
) -> Dataset: ...


"""Runs"""


def create_run(
    title: str,
    description: str,
    start: datetime | str | IntegralNanosecondsUTC,
    end: datetime | str | IntegralNanosecondsUTC | None = None,
    datasets: Mapping[str, Dataset] | None = None,
    *,
    labels: Sequence[str] | None = None,
    properties: Mapping[str, str] | None = None,
) -> Run: ...


def get_run_by_rid(rid: str) -> Run: ...


def update_run(
    run: Run,
    *,
    title: str | None = None,
    description: str | None = None,
    properties: Mapping[str, str] | None = None,
    labels: Sequence[str] | None = None,
) -> Run: ...


def add_dataset_to_run(dataset: Dataset, run: Run) -> Run: ...


def list_datasets_for_run(run: Run) -> list[Dataset]: ...


def add_attachment_to_run(attachment: Attachment, run: Run) -> Run: ...


def list_attachments_for_run(run: Run) -> list[Attachment]: ...


"""Attachments"""


def upload_attachment(
    path: Path | str,
    name: str,
    description: str,
    *,
    labels: Sequence[str] | None = None,
    properties: Mapping[str, str] | None = None,
) -> Attachment: ...


def get_attachment_by_rid(rid: str) -> Attachment: ...


def update_attachment(
    attachment: Attachment,
    *,
    name: str | None = None,
    description: str | None = None,
    properties: Mapping[str, str] | None = None,
    labels: Sequence[str] | None = None,
) -> Attachment: ...


def save_attachment(attachment: Attachment, path: Path | str) -> None: ...
