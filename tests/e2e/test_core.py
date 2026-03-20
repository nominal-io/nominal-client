"""End-to-end tests for mutation operations and data retrieval on datasets, runs, and attachments.

Covers:
  - Updating metadata (name, description, properties, labels) on datasets, runs, and attachments
  - Updating a run's time window (start/end)
  - Uploading multiple CSV files to a single dataset
  - Linking datasets and attachments to a run, then listing them back
  - Reading channel data via the pandas integration (single-channel and full-dataset retrieval)
  - Downloading a dataset file to disk via MultipartFileDownloader and verifying byte-identical content
  - Creating channels on a datasource via add_channel and batch_add_channels

The `ingested_dataset` fixture is session-scoped (defined in conftest.py): one shared dataset is
created at the start of the session and reused by all read-only channel/pandas tests, avoiding
redundant ingest round-trips for each test.
"""

from __future__ import annotations

from datetime import timedelta
from io import BytesIO
from pathlib import Path
from typing import Callable
from uuid import uuid4

import pandas as pd
import pytest

from nominal.core import NominalClient
from nominal.core.channel import ChannelDataType
from nominal.core.dataset import Dataset
from nominal.core.dataset_file import wait_for_files_to_ingest
from nominal.core.datasource import CreateChannelRequest
from nominal.thirdparty.pandas import channel_to_series, datasource_to_dataframe
from nominal.ts import ISO_8601, _SecondsNanos
from tests.e2e import POLL_INTERVAL, _create_random_start_end

ArchiveFn = Callable[[object], None]


def test_update_dataset(client: NominalClient, csv_data, archive: ArchiveFn):
    """Calling `dataset.update()` mutates name, description, properties, and labels in-place."""
    name = f"dataset-{uuid4()}"
    desc = f"core test to update a dataset {uuid4()}"

    ds = client.create_dataset(name, description=desc)
    archive(ds)
    ds.add_from_io(BytesIO(csv_data), "timestamp", "iso_8601").poll_until_ingestion_completed(interval=POLL_INTERVAL)

    new_name = name + "-updated"
    new_desc = desc + "-updated"
    new_props = {"key": "value"}
    new_labels = ["label"]
    ds.update(name=new_name, description=new_desc, properties=new_props, labels=new_labels)

    assert ds.name == new_name
    assert ds.description == new_desc
    assert ds.properties == new_props
    assert ds.labels == tuple(new_labels)


def test_update_run(client: NominalClient, archive: ArchiveFn):
    """Calling `run.update()` mutates all mutable fields, including the start/end time window."""
    title = f"run-{uuid4()}"
    desc = f"core test to update a run {uuid4()}"
    start, end = _create_random_start_end()
    run = client.create_run(title, start, end, description=desc)
    archive(run)

    # Verify initial state before updating
    assert run.name == title
    assert run.description == desc
    assert len(run.properties) == 0
    assert len(run.labels) == 0
    assert run.start == _SecondsNanos.from_datetime(start).to_nanoseconds()
    assert run.end == _SecondsNanos.from_datetime(end).to_nanoseconds()

    new_name = title + "-updated"
    new_desc = desc + "-updated"
    new_props = {"key": "value"}
    new_labels = ["label"]
    # Shrink the time window by 1 second on each side to confirm timestamps are updated
    new_start = start + timedelta(seconds=1)
    new_end = end - timedelta(seconds=1)

    run.update(
        name=new_name,
        description=new_desc,
        properties=new_props,
        labels=new_labels,
        start=new_start,
        end=new_end,
    )

    assert run.name == new_name
    assert run.description == new_desc
    assert run.properties == new_props
    assert run.labels == tuple(new_labels)
    assert run.start == _SecondsNanos.from_datetime(new_start).to_nanoseconds()
    assert run.end == _SecondsNanos.from_datetime(new_end).to_nanoseconds()


def test_add_dataset_to_run_and_list_datasets(client: NominalClient, csv_data, archive: ArchiveFn):
    """Linking a dataset to a run with a custom ref-name is reflected in `run.list_datasets()`."""
    ds = client.create_dataset(f"dataset-{uuid4()}")
    archive(ds)
    ds.add_from_io(BytesIO(csv_data), "timestamp", "iso_8601").poll_until_ingestion_completed(interval=POLL_INTERVAL)

    run = client.create_run(f"run-{uuid4()}", *_create_random_start_end())
    archive(run)

    ref_name = f"ref-name-{uuid4()}"
    run.add_dataset(ref_name, ds)

    ds_list = run.list_datasets()
    assert len(ds_list) == 1
    ref_name2, ds2 = ds_list[0]
    assert ref_name2 == ref_name
    assert ds2.rid == ds.rid


def test_add_csv_to_dataset(client: NominalClient, csv_data, csv_data2, archive: ArchiveFn):
    """Uploading two separate CSV files to the same dataset both ingest successfully."""
    name = f"dataset-{uuid4()}"
    desc = f"core test to add more data to a dataset {uuid4()}"

    ds = client.create_dataset(name, description=desc)
    archive(ds)
    # Upload both CSVs first, then batch-wait for both to ingest
    file1 = ds.add_from_io(BytesIO(csv_data), "timestamp", ISO_8601)
    file2 = ds.add_from_io(BytesIO(csv_data2), "timestamp", ISO_8601)
    wait_for_files_to_ingest([file1, file2], poll_interval=POLL_INTERVAL)

    assert ds.rid != ""
    assert ds.name == name
    assert ds.description == desc
    assert len(ds.properties) == 0
    assert len(ds.labels) == 0


def test_update_attachment(client: NominalClient, csv_data, archive: ArchiveFn):
    """Calling `attachment.update()` mutates name, description, properties, and labels in-place."""
    at_name = f"attachment-{uuid4()}"
    at_desc = f"core test to update an attachment {uuid4()}"

    at = client.create_attachment_from_io(BytesIO(csv_data), at_name, description=at_desc)
    archive(at)

    new_name = at_name + "-updated"
    new_desc = at_desc + "-updated"
    new_props = {"key": "value"}
    new_labels = ["label"]
    at.update(name=new_name, description=new_desc, properties=new_props, labels=new_labels)

    assert at.name == new_name
    assert at.description == new_desc
    assert at.properties == new_props
    assert at.labels == tuple(new_labels)


def test_add_attachment_to_run_and_list_attachments(client: NominalClient, csv_data, archive: ArchiveFn):
    """Attaching a file to a run is reflected in `run.list_attachments()`; byte contents are preserved."""
    at = client.create_attachment_from_io(BytesIO(csv_data), f"attachment-{uuid4()}")
    archive(at)

    run = client.create_run(f"run-{uuid4()}", *_create_random_start_end())
    archive(run)

    run.add_attachments([at])

    at_list = run.list_attachments()
    assert len(at_list) == 1
    at2 = at_list[0]
    assert at2.rid == at.rid != ""
    assert at2.name == at.name
    assert at2.properties == at.properties == {}
    assert at2.labels == at.labels == ()
    assert at2.get_contents().read() == at.get_contents().read() == csv_data


def test_get_channel(ingested_dataset: Dataset):
    """Fetching a channel by name returns correct metadata: data type, unit, and description."""
    c = ingested_dataset.get_channel("temperature")
    assert c.name == "temperature"
    assert c.data_source == ingested_dataset.rid
    assert c.data_type == ChannelDataType.DOUBLE
    assert c.unit is None
    assert c.description is None


def test_get_channel_pandas(ingested_dataset: Dataset, csv_data):
    """Converting a channel to a pandas Series produces values identical to the original CSV."""
    c = ingested_dataset.get_channel("temperature")
    s = channel_to_series(c)
    assert s.name == c.name == "temperature"
    assert s.index.name == "timestamp"
    assert s.dtype == "float64"

    # Parse the reference CSV with matching dtype and index for a direct comparison
    df = pd.read_csv(
        BytesIO(csv_data), parse_dates=["timestamp"], index_col="timestamp", dtype={"temperature": "float64"}
    )
    assert s.equals(df["temperature"])


def test_get_dataset_pandas(ingested_dataset: Dataset, csv_data):
    """Converting a full dataset to a DataFrame matches the original CSV; channel_exact_match filters columns."""
    expected_data = pd.read_csv(BytesIO(csv_data), index_col="timestamp", parse_dates=["timestamp"])
    for col in expected_data.columns:
        expected_data[col] = expected_data[col].astype(float)

    df = datasource_to_dataframe(ingested_dataset)
    df_sorted = df.reindex(expected_data.columns, axis=1)
    pd.testing.assert_frame_equal(df_sorted, expected_data)

    # channel_exact_match filters to channels whose names contain ALL listed substrings;
    # "relative" AND "minutes" matches only "relative_minutes"
    df2 = datasource_to_dataframe(ingested_dataset, channel_exact_match=["relative", "minutes"])
    pd.testing.assert_frame_equal(df2, expected_data[["relative_minutes"]])


def test_get_or_create_dataset_creates_and_returns_idempotently(client: NominalClient, archive: Callable[..., None]):
    """get_or_create_dataset creates a dataset on first call and returns the same one on subsequent calls."""
    asset = client.create_asset(f"asset-{uuid4()}")
    archive(asset)

    scope_name = "primary"
    ds1 = asset.get_or_create_dataset(scope_name, name=f"dataset-{uuid4()}")
    archive(ds1)

    ds2 = asset.get_or_create_dataset(scope_name)
    assert ds1.rid == ds2.rid


def test_get_or_create_dataset_with_series_tags_creates_and_returns_idempotently(
    client: NominalClient,
    archive: Callable[..., None],
):
    """get_or_create_dataset with series_tags creates a scoped dataset and returns it on repeated calls
    with matching tags.
    """
    asset = client.create_asset(f"asset-{uuid4()}")
    archive(asset)

    scope_name = "tagged-scope"
    tags = {"vehicle": "test-car", "run_type": "nominal"}

    ds1 = asset.get_or_create_dataset(scope_name, name=f"dataset-{uuid4()}", series_tags=tags)
    archive(ds1)

    ds2 = asset.get_or_create_dataset(scope_name, series_tags=tags)
    assert ds1.rid == ds2.rid


def test_get_or_create_dataset_raises_on_tag_mismatch(client: NominalClient, archive: Callable[..., None]):
    """get_or_create_dataset raises ValueError if the existing datascope has different series_tags."""
    asset = client.create_asset(f"asset-{uuid4()}")
    archive(asset)

    scope_name = "tagged-scope"
    ds = asset.get_or_create_dataset(scope_name, name=f"dataset-{uuid4()}", series_tags={"env": "prod"})
    archive(ds)

    with pytest.raises(ValueError, match="datascope already exists"):
        asset.get_or_create_dataset(scope_name, series_tags={"env": "staging"})


def test_download_dataset_file_roundtrips_to_disk(
    client: NominalClient, csv_data: bytes, tmp_path: Path, archive: Callable[..., None]
) -> None:
    """Uploading a CSV and downloading it via MultipartFileDownloader produces byte-identical content on disk."""
    ds = client.create_dataset(f"dataset-{uuid4()}")
    archive(ds)

    dataset_file = ds.add_from_io(BytesIO(csv_data), "timestamp", ISO_8601)
    dataset_file.poll_until_ingestion_completed(interval=POLL_INTERVAL)

    output_path = dataset_file.download(tmp_path)

    assert output_path.exists()
    assert output_path.read_bytes() == csv_data


def test_add_channel(client: NominalClient, archive: ArchiveFn) -> None:
    """add_channel creates a channel on the datasource that is retrievable by name."""
    ds = client.create_dataset(f"dataset-{uuid4()}")
    archive(ds)

    channel = ds.add_channel(name="velocity", data_type=ChannelDataType.DOUBLE, description="speed", unit="m/s")

    assert channel.name == "velocity"
    assert channel.data_type == ChannelDataType.DOUBLE
    assert channel.description == "speed"
    assert channel.unit == "m/s"


def test_batch_add_channels(client: NominalClient, archive: ArchiveFn) -> None:
    """batch_add_channels creates all channels and returns them with no missing entries."""
    ds = client.create_dataset(f"dataset-{uuid4()}")
    archive(ds)

    requests = [
        CreateChannelRequest(name="velocity", data_type=ChannelDataType.DOUBLE, unit="m/s"),
        CreateChannelRequest(name="temperature", data_type=ChannelDataType.DOUBLE, unit="degC"),
        CreateChannelRequest(name="status", data_type=ChannelDataType.STRING, description="system status"),
    ]
    result = ds.batch_add_channels(requests)

    assert result.missing == []
    channels = {ch.name: ch for ch in result.channels}
    assert set(channels) == {"velocity", "temperature", "status"}
    assert channels["velocity"].unit == "m/s"
    assert channels["temperature"].unit == "degC"
    assert channels["status"].description == "system status"
