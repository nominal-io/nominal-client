from __future__ import annotations

from datetime import datetime
from pathlib import Path
from threading import Thread
from typing import TYPE_CHECKING, BinaryIO

import dateutil.parser
import keyring

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


_DEFAULT_BASE_URL = "https://api.gov.nominal.io/api"
_KEYRING_SERVICE = "nominal-python-client"
_KEYRING_USER = "nominal-python-user"

# global variable which `set_base_url()` is intended to modify
_global_base_url = _DEFAULT_BASE_URL


def set_base_url(base_url: str = _DEFAULT_BASE_URL) -> None:
    """Set the default Nominal platform base url.

    For typical production environments, the default is "https://api.gov.nominal.io/api".
    For staging environments: "https://api-staging.gov.nominal.io/api".
    For local development: "https://api.nominal.test".
    """
    global _global_base_url
    _global_base_url = base_url


def set_token(token: str) -> None:
    """Set the default Nominal platform auth token.

    Note that this sets the token on your system, and you don't need to set it every time you run a script.

    token: An API token to authenticate with. You can grab a client token from the Nominal sandbox, e.g.
        at https://app.gov.nominal.io/sandbox.
    """
    keyring.set_password(_KEYRING_SERVICE, _KEYRING_USER, token)


def get_default_connection() -> NominalClient:
    """Retrieve the default connection to the Nominal platform.

    Raises nominal.exceptions.NominalError if no global connection has been set.
    """
    token = keyring.get_password(_KEYRING_SERVICE, _KEYRING_USER)
    if token is None:
        raise NominalError("No token set: initialize with `nm.set_token('eyJ...')`")
    return NominalClient.create(_global_base_url, token)


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


def upload_csv(
    file: Path | str,
    name: str,
    timestamp_column: str,
    timestamp_type: TimestampColumnType,
    description: str | None = None,
    *,
    wait_until_complete: bool = True,
) -> Dataset:
    """Create a dataset in the Nominal platform from a .csv or .csv.gz file"""
    path = Path(file)
    conn = get_default_connection()
    with open(path, "rb") as f:
        dataset = conn.create_dataset_from_io(
            f,
            name,
            timestamp_column=timestamp_column,
            timestamp_type=timestamp_type,
            file_type=FileTypes.CSV,
            description=description,
        )
    if wait_until_complete:
        dataset.poll_until_ingestion_completed()
    return dataset


def get_dataset(rid: str) -> Dataset:
    """Retrieve a dataset from the Nominal platform by its RID."""
    conn = get_default_connection()
    return conn.get_dataset(rid)


def create_run(
    name: str,
    start: datetime | str | IntegralNanosecondsUTC,
    end: datetime | str | IntegralNanosecondsUTC,
    description: str | None = None,
) -> Run:
    conn = get_default_connection()
    return conn.create_run(
        name,
        start=_parse_timestamp(start),
        end=_parse_timestamp(end),
        description=description,
    )


def get_run(rid: str) -> Run:
    """Retrieve a run from the Nominal platform by its RID."""
    conn = get_default_connection()
    return conn.get_run(rid)


def search_runs(
    *,
    start: str | datetime | IntegralNanosecondsUTC | None = None,
    end: str | datetime | IntegralNanosecondsUTC | None = None,
    exact_name: str | None = None,
    label: str | None = None,
    property: tuple[str, str] | None = None,
) -> list[Run]:
    """Search for runs meeting the specified filters.

    Filters are ANDed together, e.g. `(run.label == label) AND (run.end <= end)`
    - `start` and `end` times are both inclusive
    - `exact_name` is case-insensitive
    - `property` is a key-value pair, e.g. ("name", "value")
    """
    conn = get_default_connection()
    runs = conn.search_runs(
        start=None if start is None else _parse_timestamp(start),
        end=None if end is None else _parse_timestamp(end),
        exact_name=exact_name,
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
    conn = get_default_connection()
    file_type = FileType.from_path(path)
    with open(path, "rb") as f:
        return conn.create_attachment_from_io(f, name, file_type, description)


def get_attachment(rid: str) -> Attachment:
    """Retrieve an attachment from the Nominal platform by its RID."""
    conn = get_default_connection()
    return conn.get_attachment(rid)


def download_attachment(rid: str, file: Path | str) -> None:
    """Retrieve an attachment from the Nominal platform and save it to `file`."""
    conn = get_default_connection()
    attachment = conn.get_attachment(rid)
    attachment.write(Path(file))


def _parse_timestamp(ts: str | datetime | IntegralNanosecondsUTC) -> IntegralNanosecondsUTC:
    if isinstance(ts, int):
        return ts
    if isinstance(ts, str):
        ts = dateutil.parser.parse(ts)
    return _datetime_to_integral_nanoseconds(ts)
