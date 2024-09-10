from datetime import datetime, timedelta
from io import BytesIO
from unittest import mock
from uuid import uuid4

import pandas as pd
import polars as pl

import nominal as nm
from nominal import _utils
from nominal.nominal import _parse_timestamp

from . import _create_random_start_end


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
    assert run.start == _utils._datetime_to_integral_nanoseconds(start)
    assert run.end == _utils._datetime_to_integral_nanoseconds(end)
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
    assert run.start == _utils._datetime_to_integral_nanoseconds(start)
    assert run.end == _utils._datetime_to_integral_nanoseconds(end)
    assert len(run.properties) == 0
    assert len(run.labels) == 0

    datasets = run.list_datasets()
    assert len(datasets) == 1
    ref_name, dataset = datasets[0]
    assert ref_name == "dataset"
    assert dataset.rid != ""
    assert dataset.name == f"Dataset for Run: {name}"
    assert dataset.description == None
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
    assert run2.start == run.start == _parse_timestamp(start)
    assert run2.end == run.end == _parse_timestamp(end)
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
    assert run2.start == run.start == _parse_timestamp(start)
    assert run2.end == run.end == _parse_timestamp(end)
    assert run2.properties == run.properties == {}
    assert run2.labels == run.labels == ()


def test_upload_attachment(csv_data):
    at_title = f"attachment-{uuid4()}"
    at_desc = f"top-level test to add a attachment to a run {uuid4()}"

    with mock.patch("builtins.open", mock.mock_open(read_data=csv_data)):
        at = nm.upload_attachment("fake_path.csv", at_title, at_desc)

    assert at.rid != ""
    assert at.name == at_title
    assert at.description == at_desc
    assert len(at.properties) == 0
    assert len(at.labels) == 0


def test_get_attachment(csv_data):
    at_title = f"attachment-{uuid4()}"
    at_desc = f"top-level test to add a attachment to a run {uuid4()}"

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
    at_desc = f"top-level test to add a attachment to a run {uuid4()}"

    with mock.patch("builtins.open", mock.mock_open(read_data=csv_data)):
        at = nm.upload_attachment("fake_path.csv", at_title, at_desc)

    with _utils.reader_writer() as (r, w):
        with mock.patch("builtins.open", return_value=w):
            nm.download_attachment(at.rid, "fake_path.csv")
            assert r.read() == csv_data
