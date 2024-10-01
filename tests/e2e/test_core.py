from datetime import timedelta
from unittest import mock
from uuid import uuid4

import nominal as nm
from nominal.ts import _SecondsNanos

from . import _create_random_start_end


def test_update_dataset(csv_data):
    name = f"dataset-{uuid4()}"
    desc = f"core test to update a dataset {uuid4()}"

    with mock.patch("builtins.open", mock.mock_open(read_data=csv_data)):
        ds = nm.upload_csv("fake_path.csv", name, "timestamp", "iso_8601", desc)
    new_name = name + "-updated"
    new_desc = desc + "-updated"
    new_props = {"key": "value"}
    new_labels = ["label"]
    ds.update(name=new_name, description=new_desc, properties=new_props, labels=new_labels)

    assert ds.name == new_name
    assert ds.description == new_desc
    assert ds.properties == new_props
    assert ds.labels == tuple(new_labels)


def test_update_run():
    title = f"run-{uuid4()}"
    desc = f"core test to update a run {uuid4()}"
    start, end = _create_random_start_end()
    run = nm.create_run(title, start, end, desc)

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


def test_add_dataset_to_run_and_list_datasets(csv_data):
    ds_name = f"dataset-{uuid4()}"
    ds_desc = f"core test to add a dataset to a run {uuid4()}"

    with mock.patch("builtins.open", mock.mock_open(read_data=csv_data)):
        ds = nm.upload_csv("fake_path.csv", ds_name, "timestamp", "iso_8601", ds_desc)

    title = f"run-{uuid4()}"
    desc = f"core test to add a dataset to a run {uuid4()}"
    start, end = _create_random_start_end()
    run = nm.create_run(title, start, end, desc)

    ref_name = f"ref-name-{uuid4()}"
    run.add_dataset(ref_name, ds)

    ds_list = run.list_datasets()
    assert len(ds_list) == 1
    ref_name2, ds2 = ds_list[0]
    assert ref_name2 == ref_name
    assert ds2.rid == ds.rid


def test_add_csv_to_dataset(csv_data, csv_data2):
    name = f"dataset-{uuid4()}"
    desc = f"TESTING core test to add more data to a dataset {uuid4()}"

    with mock.patch("builtins.open", mock.mock_open(read_data=csv_data)):
        ds = nm.upload_csv("fake_path.csv", name, "timestamp", nm.ts.ISO_8601, desc)
    ds.poll_until_ingestion_completed(interval=timedelta(seconds=0.1))

    with mock.patch("builtins.open", mock.mock_open(read_data=csv_data2)):
        ds.add_csv_to_dataset("fake_path.csv", "timestamp", nm.ts.ISO_8601)
    ds.poll_until_ingestion_completed(interval=timedelta(seconds=0.1))

    assert ds.rid != ""
    assert ds.name == name
    assert ds.description == desc
    assert len(ds.properties) == 0
    assert len(ds.labels) == 0


def test_update_attachment(csv_data):
    at_name = f"attachment-{uuid4()}"
    at_desc = f"core test to add a attachment to a run {uuid4()}"

    with mock.patch("builtins.open", mock.mock_open(read_data=csv_data)):
        at = nm.upload_attachment("fake_path.csv", at_name, at_desc)

    new_name = at_name + "-updated"
    new_desc = at_desc + "-updated"
    new_props = {"key": "value"}
    new_labels = ["label"]
    at.update(name=new_name, description=new_desc, properties=new_props, labels=new_labels)

    assert at.name == new_name
    assert at.description == new_desc
    assert at.properties == new_props
    assert at.labels == tuple(new_labels)


def test_add_attachment_to_run_and_list_attachments(csv_data):
    at_name = f"attachment-{uuid4()}"
    at_desc = f"core test to add a attachment to a run {uuid4()}"

    with mock.patch("builtins.open", mock.mock_open(read_data=csv_data)):
        at = nm.upload_attachment("fake_path.csv", at_name, at_desc)

    title = f"run-{uuid4()}"
    desc = f"core test to add a attachment to a run {uuid4()}"
    start, end = _create_random_start_end()
    run = nm.create_run(title, start, end, desc)

    run.add_attachments([at])

    at_list = run.list_attachments()

    assert len(at_list) == 1
    at2 = at_list[0]
    assert at2.rid == at.rid != ""
    assert at2.name == at.name == at_name
    assert at2.description == at.description == at_desc
    assert at2.properties == at.properties == {}
    assert at2.labels == at.labels == ()
    assert at2.get_contents().read() == at.get_contents().read() == csv_data


def test_create_get_log_set(client: nm.NominalClient):
    name = f"logset-{uuid4()}"
    desc = f"core test to create & get a log set {uuid4()}"
    start, _ = _create_random_start_end()
    logs = [
        (nm.ts._SecondsNanos.from_datetime(start + timedelta(seconds=i)).to_nanoseconds(), f"Log message {i}")
        for i in range(5)
    ]

    logset = client.create_log_set(name, logs, "absolute", desc)
    logset2 = nm.get_log_set(logset.rid)
    assert logset2.rid == logset.rid != ""
    assert logset2.name == logset.name == name
    assert logset2.description == logset.description == desc
    assert logset2.timestamp_type == logset.timestamp_type == "absolute"

    retrieved_logs = [(log.timestamp, log.body) for log in logset2.stream_logs()]
    assert len(retrieved_logs) == 5
    assert retrieved_logs == logs
