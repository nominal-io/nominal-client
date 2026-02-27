from datetime import datetime, timedelta
from io import BytesIO
from typing import Callable
from uuid import uuid4

import pandas as pd
import polars as pl

from nominal.core import NominalClient
from nominal.core.filetype import FileTypes
from nominal.thirdparty.pandas import upload_dataframe
from nominal.ts import ISO_8601, Relative, _SecondsNanos
from tests.e2e import _create_random_start_end


def test_upload_csv(client: NominalClient, csv_data, archive: Callable):
    name = f"dataset-{uuid4()}"
    desc = f"top-level test to create a dataset {uuid4()}"

    ds = client.create_dataset(name, description=desc)
    archive(ds)
    ds.add_from_io(BytesIO(csv_data), "timestamp", "iso_8601").poll_until_ingestion_completed(
        interval=timedelta(seconds=0.1)
    )

    assert ds.rid != ""
    assert ds.name == name
    assert ds.description == desc
    assert len(ds.properties) == 0
    assert len(ds.labels) == 0


def test_upload_csv_gz(client: NominalClient, csv_gz_data, archive: Callable):
    name = f"dataset-{uuid4()}"
    desc = f"top-level test to create a dataset from a gzipped csv {uuid4()}"

    ds = client.create_dataset(name, description=desc)
    archive(ds)
    ds.add_from_io(BytesIO(csv_gz_data), "timestamp", "iso_8601", FileTypes.CSV_GZ).poll_until_ingestion_completed(
        interval=timedelta(seconds=0.1)
    )

    assert ds.rid != ""
    assert ds.name == name
    assert ds.description == desc
    assert len(ds.properties) == 0
    assert len(ds.labels) == 0


def test_upload_csv_relative_timestamp(client: NominalClient, csv_data, archive: Callable):
    name = f"dataset-{uuid4()}"
    desc = f"top-level test to create a dataset with relative timestamps {uuid4()}"
    start, _ = _create_random_start_end()

    ds = client.create_dataset(name, description=desc)
    archive(ds)
    ds.add_from_io(BytesIO(csv_data), "relative_minutes", Relative("minutes", start)).poll_until_ingestion_completed(
        interval=timedelta(seconds=0.1)
    )

    assert ds.rid != ""
    assert ds.name == name
    assert ds.description == desc
    assert len(ds.properties) == 0
    assert len(ds.labels) == 0


def test_upload_pandas(client: NominalClient, csv_data, archive: Callable):
    name = f"dataset-{uuid4()}"
    desc = f"top-level test to create a dataset from pandas {uuid4()}"

    df = pd.read_csv(BytesIO(csv_data))
    ds = upload_dataframe(client, df, name, "timestamp", "iso_8601", desc)
    archive(ds)

    assert ds.rid != ""
    assert ds.name == name
    assert ds.description == desc
    assert len(ds.properties) == 0
    assert len(ds.labels) == 0


def test_upload_polars(client: NominalClient, csv_data, archive: Callable):
    name = f"dataset-{uuid4()}"
    desc = f"top-level test to create a dataset from polars {uuid4()}"

    # Write polars df back to CSV bytes to avoid a pyarrow dependency in to_pandas()
    df = pl.read_csv(csv_data)
    csv_bytes = df.write_csv().encode()
    ds = client.create_dataset(name, description=desc)
    archive(ds)
    ds.add_from_io(BytesIO(csv_bytes), "timestamp", "iso_8601").poll_until_ingestion_completed(
        interval=timedelta(seconds=0.1)
    )

    assert ds.rid != ""
    assert ds.name == name
    assert ds.description == desc
    assert len(ds.properties) == 0
    assert len(ds.labels) == 0


def test_get_dataset(client: NominalClient, csv_data, archive: Callable):
    name = f"dataset-{uuid4()}"
    desc = f"top-level test to create & get a dataset from csv {uuid4()}"

    ds = client.create_dataset(name, description=desc)
    archive(ds)
    ds.add_from_io(BytesIO(csv_data), "timestamp", "iso_8601")

    ds2 = client.get_dataset(ds.rid)
    assert ds2.rid == ds.rid != ""
    assert ds2.name == ds.name == name
    assert ds2.description == ds.description == desc
    assert ds2.properties == ds.properties == {}
    assert ds2.labels == ds.labels == ()


def test_create_run(client: NominalClient, archive: Callable):
    name = f"run-{uuid4()}"
    desc = f"top-level test to create a run {uuid4()}"
    start, end = _create_random_start_end()
    run = client.create_run(name, start, end, description=desc)
    archive(run)

    assert run.rid != ""
    assert run.name == name
    assert run.description == desc
    assert run.start == _SecondsNanos.from_datetime(start).to_nanoseconds()
    assert run.end == _SecondsNanos.from_datetime(end).to_nanoseconds()
    assert len(run.properties) == 0
    assert len(run.labels) == 0


def test_create_run_csv(client: NominalClient, csv_data, archive: Callable):
    name = f"run-{uuid4()}"
    desc = f"top-level test to create a run and dataset {uuid4()}"
    dataset_name = f"Dataset for Run: {name}"

    ds = client.create_dataset(dataset_name)
    archive(ds)
    ds.add_from_io(BytesIO(csv_data), "timestamp", ISO_8601).poll_until_ingestion_completed(
        interval=timedelta(seconds=0.1)
    )
    ds.refresh()
    assert ds.bounds is not None

    run = client.create_run(name, start=ds.bounds.start, end=ds.bounds.end, description=desc)
    archive(run)

    expected_start = datetime.fromisoformat("2024-09-05T18:00:00Z")
    expected_end = datetime.fromisoformat("2024-09-05T18:09:00Z")
    assert run.rid != ""
    assert run.name == name
    assert run.description == desc
    assert run.start == _SecondsNanos.from_datetime(expected_start).to_nanoseconds()
    assert run.end == _SecondsNanos.from_datetime(expected_end).to_nanoseconds()
    assert len(run.properties) == 0
    assert len(run.labels) == 0

    datasets = run.list_datasets()
    assert len(datasets) == 1
    ref_name, dataset = datasets[0]
    assert ref_name == "dataset"
    assert dataset.rid != ""
    assert dataset.name == dataset_name
    assert dataset.description is None
    assert len(dataset.properties) == 0
    assert len(dataset.labels) == 0


def test_get_run(client: NominalClient, archive: Callable):
    name = f"run-{uuid4()}"
    desc = f"top-level test to get a run {uuid4()}"
    start, end = _create_random_start_end()
    run = client.create_run(name, start, end, description=desc)
    archive(run)
    run2 = client.get_run(run.rid)

    assert run2.rid == run.rid != ""
    assert run2.name == run.name == name
    assert run2.description == run.description == desc
    assert run2.start == run.start == _SecondsNanos.from_flexible(start).to_nanoseconds()
    assert run2.end == run.end == _SecondsNanos.from_flexible(end).to_nanoseconds()
    assert run2.properties == run.properties == {}
    assert run2.labels == run.labels == ()


def test_upload_attachment(client: NominalClient, csv_data, archive: Callable):
    at_title = f"attachment-{uuid4()}"
    at_desc = f"top-level test to upload an attachment {uuid4()}"

    at = client.create_attachment_from_io(BytesIO(csv_data), at_title, description=at_desc)
    archive(at)

    assert at.rid != ""
    assert at.name == at_title
    assert at.description == at_desc
    assert len(at.properties) == 0
    assert len(at.labels) == 0


def test_get_attachment(client: NominalClient, csv_data, archive: Callable):
    at_title = f"attachment-{uuid4()}"
    at_desc = f"top-level test to get an attachment {uuid4()}"

    at = client.create_attachment_from_io(BytesIO(csv_data), at_title, description=at_desc)
    archive(at)
    a2 = client.get_attachment(at.rid)

    assert a2.rid == at.rid != ""
    assert a2.name == at.name == at_title
    assert a2.description == at.description == at_desc
    assert a2.properties == at.properties == {}
    assert a2.labels == at.labels == ()


def test_download_attachment(client: NominalClient, csv_data, archive: Callable):
    at_title = f"attachment-{uuid4()}"
    at_desc = f"top-level test to download an attachment {uuid4()}"

    at = client.create_attachment_from_io(BytesIO(csv_data), at_title, description=at_desc)
    archive(at)
    assert at.get_contents().read() == csv_data


def test_upload_video(client: NominalClient, mp4_data, archive: Callable):
    title = f"video-{uuid4()}"
    desc = f"top-level test to ingest a video {uuid4()}"
    start, _ = _create_random_start_end()

    v = client.create_video(title, description=desc)
    archive(v)
    v.add_from_io(BytesIO(mp4_data), f"{title}.mp4", start=start)
    v.poll_until_ingestion_completed(interval=timedelta(seconds=0.1))

    assert v.rid != ""
    assert v.name == title
    assert v.description == desc
    assert len(v.properties) == 0
    assert len(v.labels) == 0


def test_get_video(client: NominalClient, mp4_data, archive: Callable):
    title = f"video-{uuid4()}"
    desc = f"top-level test to get a video {uuid4()}"
    start, _ = _create_random_start_end()

    v = client.create_video(title, description=desc)
    archive(v)
    v.add_from_io(BytesIO(mp4_data), f"{title}.mp4", start=start)
    v2 = client.get_video(v.rid)

    assert v2.rid == v.rid != ""
    assert v2.name == v.name == title
    assert v2.description == v.description == desc
    assert v2.properties == v.properties == {}
    assert v2.labels == v.labels == ()
