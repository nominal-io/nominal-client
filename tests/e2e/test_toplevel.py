from datetime import datetime, timedelta
from io import BytesIO
from unittest import mock
from uuid import uuid4

import pandas as pd
import polars as pl

import nominal as nm
from nominal._utils import reader_writer
from tests.e2e import _create_random_start_end


def test_upload_csv(csv_data):
    name = f"dataset-{uuid4()}"
    desc = f"top-level test to create a dataset {uuid4()}"

    with mock.patch("builtins.open", mock.mock_open(read_data=csv_data)):
        ds = nm.upload_csv("fake_path.csv", name, "timestamp", "iso_8601", desc)
    ds.poll_until_ingestion_completed(interval=timedelta(seconds=0.1))

    assert ds.rid != ""
    assert ds.name == name
    assert ds.description == desc
    assert len(ds.properties) == 0
    assert len(ds.labels) == 0


def test_upload_csv_gz(csv_gz_data):
    name = f"dataset-{uuid4()}"
    desc = f"top-level test to create a dataset from a gzipped csv {uuid4()}"

    with mock.patch("builtins.open", mock.mock_open(read_data=csv_gz_data)):
        ds = nm.upload_csv("fake_path.csv.gz", name, "timestamp", "iso_8601", desc)
    ds.poll_until_ingestion_completed(interval=timedelta(seconds=0.1))

    assert ds.rid != ""
    assert ds.name == name
    assert ds.description == desc
    assert len(ds.properties) == 0
    assert len(ds.labels) == 0


def test_upload_csv_relative_timestamp(csv_data):
    name = f"dataset-{uuid4()}"
    desc = f"top-level test to create a dataset with relative timestamps {uuid4()}"
    start, _ = _create_random_start_end()

    with mock.patch("builtins.open", mock.mock_open(read_data=csv_data)):
        ds = nm.upload_csv("fake_path.csv", name, "relative_minutes", nm.ts.Relative("minutes", start), desc)
    ds.poll_until_ingestion_completed(interval=timedelta(seconds=0.1))

    assert ds.rid != ""
    assert ds.name == name
    assert ds.description == desc
    assert len(ds.properties) == 0
    assert len(ds.labels) == 0


def test_upload_pandas(csv_data):
    name = f"dataset-{uuid4()}"
    desc = f"top-level test to create a dataset from pandas {uuid4()}"

    csv_f = BytesIO(csv_data)
    df = pd.read_csv(csv_f)
    ds = nm.upload_pandas(df, name, "timestamp", "iso_8601", desc)
    ds.poll_until_ingestion_completed(interval=timedelta(seconds=0.1))

    assert ds.rid != ""
    assert ds.name == name
    assert ds.description == desc
    assert len(ds.properties) == 0
    assert len(ds.labels) == 0


def test_upload_polars(csv_data):
    name = f"dataset-{uuid4()}"
    desc = f"top-level test to create a dataset from polars {uuid4()}"

    df = pl.read_csv(csv_data)
    ds = nm.upload_polars(df, name, "timestamp", "iso_8601", desc)
    ds.poll_until_ingestion_completed(interval=timedelta(seconds=0.1))

    assert ds.rid != ""
    assert ds.name == name
    assert ds.description == desc
    assert len(ds.properties) == 0
    assert len(ds.labels) == 0


def test_get_dataset(csv_data):
    name = f"dataset-{uuid4()}"
    desc = f"top-level test to create & get a dataset from csv {uuid4()}"

    with mock.patch("builtins.open", mock.mock_open(read_data=csv_data)):
        ds = nm.upload_csv("fake_path.csv", name, "timestamp", "iso_8601", desc)

    ds2 = nm.get_dataset(ds.rid)
    assert ds2.rid == ds.rid != ""
    assert ds2.name == ds.name == name
    assert ds2.description == ds.description == desc
    assert ds2.properties == ds.properties == {}
    assert ds2.labels == ds.labels == ()


def test_create_run():
    name = f"run-{uuid4()}"
    desc = f"top-level test to create a run {uuid4()}"
    start, end = _create_random_start_end()
    run = nm.create_run(name, start, end, desc)

    assert run.rid != ""
    assert run.name == name
    assert run.description == desc
    assert run.start == nm.ts._SecondsNanos.from_datetime(start).to_nanoseconds()
    assert run.end == nm.ts._SecondsNanos.from_datetime(end).to_nanoseconds()
    assert len(run.properties) == 0
    assert len(run.labels) == 0


def test_create_run_csv(csv_data):
    name = f"run-{uuid4()}"
    desc = f"top-level test to create a run and dataset {uuid4()}"

    with mock.patch("builtins.open", mock.mock_open(read_data=csv_data)):
        run = nm.create_run_csv("fake_path.csv", name, "timestamp", "iso_8601", desc)

    start = datetime.fromisoformat("2024-09-05T18:00:00Z")
    end = datetime.fromisoformat("2024-09-05T18:09:00Z")
    assert run.rid != ""
    assert run.name == name
    assert run.description == desc
    assert run.start == nm.ts._SecondsNanos.from_datetime(start).to_nanoseconds()
    assert run.end == nm.ts._SecondsNanos.from_datetime(end).to_nanoseconds()
    assert len(run.properties) == 0
    assert len(run.labels) == 0

    datasets = run.list_datasets()
    assert len(datasets) == 1
    ref_name, dataset = datasets[0]
    assert ref_name == "dataset"
    assert dataset.rid != ""
    assert dataset.name == f"Dataset for Run: {name}"
    assert dataset.description is None
    assert len(dataset.properties) == 0
    assert len(dataset.labels) == 0


def test_get_run():
    name = f"run-{uuid4()}"
    desc = f"top-level test to get a run {uuid4()}"
    start, end = _create_random_start_end()
    run = nm.create_run(name, start, end, desc)
    run2 = nm.get_run(run.rid)

    assert run2.rid == run.rid != ""
    assert run2.name == run.name == name
    assert run2.description == run.description == desc
    assert run2.start == run.start == nm.ts._SecondsNanos.from_flexible(start).to_nanoseconds()
    assert run2.end == run.end == nm.ts._SecondsNanos.from_flexible(end).to_nanoseconds()
    assert run2.properties == run.properties == {}
    assert run2.labels == run.labels == ()


def test_search_runs():
    # TODO: Add more search criteria
    name = f"run-{uuid4()}"
    desc = f"top-level test to search for a run {uuid4()}"
    start, end = _create_random_start_end()
    run = nm.create_run(name, start, end, desc)
    runs = nm.search_runs(start=start, end=end)
    assert len(runs) == 1
    run2 = runs[0]

    assert run2.rid == run.rid != ""
    assert run2.name == run.name == name
    assert run2.description == run.description == desc
    assert run2.start == run.start == nm.ts._SecondsNanos.from_datetime(start).to_nanoseconds()
    assert run2.end == run.end == nm.ts._SecondsNanos.from_datetime(end).to_nanoseconds()
    assert run2.properties == run.properties == {}
    assert run2.labels == run.labels == ()


def test_search_runs_substring():
    name = f"run-{uuid4()}"
    desc = f"top-level test to search for a run by name {uuid4()}"
    start, end = _create_random_start_end()
    run = nm.create_run(name, start, end, desc)
    runs = nm.search_runs(name_substring=name[4:])
    assert len(runs) == 1
    run2 = runs[0]

    assert run2.rid == run.rid != ""
    assert run2.name == run.name == name


def test_upload_attachment(csv_data):
    at_title = f"attachment-{uuid4()}"
    at_desc = f"top-level test to upload an attachment {uuid4()}"

    with mock.patch("builtins.open", mock.mock_open(read_data=csv_data)):
        at = nm.upload_attachment("fake_path.csv", at_title, at_desc)

    assert at.rid != ""
    assert at.name == at_title
    assert at.description == at_desc
    assert len(at.properties) == 0
    assert len(at.labels) == 0


def test_get_attachment(csv_data):
    at_title = f"attachment-{uuid4()}"
    at_desc = f"top-level test to get an attachment {uuid4()}"

    with mock.patch("builtins.open", mock.mock_open(read_data=csv_data)):
        at = nm.upload_attachment("fake_path.csv", at_title, at_desc)

    a2 = nm.get_attachment(at.rid)
    assert a2.rid == at.rid != ""
    assert a2.name == at.name == at_title
    assert a2.description == at.description == at_desc
    assert a2.properties == at.properties == {}
    assert a2.labels == at.labels == ()


def test_download_attachment(csv_data):
    at_title = f"attachment-{uuid4()}"
    at_desc = f"top-level test to download an attachment {uuid4()}"

    with mock.patch("builtins.open", mock.mock_open(read_data=csv_data)):
        at = nm.upload_attachment("fake_path.csv", at_title, at_desc)

    with (
        reader_writer() as (r, w),
        mock.patch("builtins.open", return_value=w),
    ):
        nm.download_attachment(at.rid, "fake_path.csv")
        assert r.read() == csv_data


def test_upload_video(mp4_data):
    title = f"video-{uuid4()}"
    desc = f"top-level test to ingest a video {uuid4()}"
    start, _ = _create_random_start_end()

    with mock.patch("builtins.open", mock.mock_open(read_data=mp4_data)):
        v = nm.upload_video("fake_path.mp4", title, start, desc)
    v.poll_until_ingestion_completed(interval=timedelta(seconds=0.1))

    assert v.rid != ""
    assert v.name == title
    assert v.description == desc
    assert len(v.properties) == 0
    assert len(v.labels) == 0


def test_get_video(mp4_data):
    title = f"video-{uuid4()}"
    desc = f"top-level test to get a video {uuid4()}"
    start, _ = _create_random_start_end()

    with mock.patch("builtins.open", mock.mock_open(read_data=mp4_data)):
        v = nm.upload_video("fake_path.mp4", title, start, desc)
    v2 = nm.get_video(v.rid)

    assert v2.rid == v.rid != ""
    assert v2.name == v.name == title
    assert v2.description == v.description == desc
    assert v2.properties == v.properties == {}
    assert v2.labels == v.labels == ()
