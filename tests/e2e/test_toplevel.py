from datetime import timedelta
from io import BytesIO
from uuid import uuid4
from unittest import mock

import pandas as pd
import polars as pl
import pytest
import nominal.nominal as nm
from nominal import _utils

from . import _create_random_start_end


@pytest.fixture(scope="session", autouse=True)
def set_conn(base_url, auth_token):
    nm.set_default_connection(base_url, auth_token)


@pytest.fixture(scope="session")
def csv_data():
    return b"""\
timestamp,temperature,humidity
2024-09-05T18:00:00Z,20,50
2024-09-05T18:01:00Z,21,49
2024-09-05T18:02:00Z,22,48
2024-09-05T18:03:00Z,23,47
2024-09-05T18:04:00Z,24,46
2024-09-05T18:05:00Z,25,45
2024-09-05T18:06:00Z,26,44
2024-09-05T18:07:00Z,27,43
2024-09-05T18:08:00Z,28,42
2024-09-05T18:09:00Z,29,41
"""


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
    title = f"run-{uuid4()}"
    desc = f"top-level test to create a run {uuid4()}"
    start, end = _create_random_start_end()
    run = nm.create_run(title, start, end, desc)

    assert run.rid != ""
    assert run.title == title
    assert run.description == desc
    assert run.start == _utils._datetime_to_integral_nanoseconds(start)
    assert run.end == _utils._datetime_to_integral_nanoseconds(end)
    assert len(run.properties) == 0
    assert len(run.labels) == 0


def test_get_run():
    title = f"run-{uuid4()}"
    desc = f"top-level test to get a run {uuid4()}"
    start, end = _create_random_start_end()
    run = nm.create_run(title, start, end, desc)
    run2 = nm.get_run(run.rid)

    assert run2.rid == run.rid != ""
    assert run2.title == run.title == title
    assert run2.description == run.description == desc
    assert run2.start == run.start == nm._parse_timestamp(start)
    assert run2.end == run.end == nm._parse_timestamp(end)
    assert run2.properties == run.properties == {}
    assert run2.labels == run.labels == ()


def test_search_runs():
    # TODO: Add more search criteria
    title = f"run-{uuid4()}"
    desc = f"top-level test to search for a run {uuid4()}"
    start, end = _create_random_start_end()
    run = nm.create_run(title, start, end, desc)
    runs = nm.search_runs(start=start, end=end)
    assert len(runs) == 1
    run2 = runs[0]

    assert run2.rid == run.rid != ""
    assert run2.title == run.title == title
    assert run2.description == run.description == desc
    assert run2.start == run.start == nm._parse_timestamp(start)
    assert run2.end == run.end == nm._parse_timestamp(end)
    assert run2.properties == run.properties == {}
    assert run2.labels == run.labels == ()


def test_upload_attachment(csv_data):
    at_title = f"attachment-{uuid4()}"
    at_desc = f"top-level test to add a attachment to a run {uuid4()}"

    with mock.patch("builtins.open", mock.mock_open(read_data=csv_data)):
        at = nm.upload_attachment("fake_path.csv", at_title, at_desc)

    assert at.rid != ""
    assert at.title == at_title
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
    assert a2.title == at.title == at_title
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
