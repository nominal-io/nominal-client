from unittest import mock
from uuid import uuid4

import nominal as nm

from . import _create_random_start_end


def test_update_dataset(csv_data):
    name = f"dataset-{uuid4()}"
    desc = f"sdk to update a dataset {uuid4()}"

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
    desc = f"sdk to update a run {uuid4()}"
    start, end = _create_random_start_end()
    run = nm.create_run(title, start, end, desc)
    new_name = title + "-updated"
    new_desc = desc + "-updated"
    new_props = {"key": "value"}
    new_labels = ["label"]
    run.update(name=new_name, description=new_desc, properties=new_props, labels=new_labels)

    assert run.name == new_name
    assert run.description == new_desc
    assert run.properties == new_props
    assert run.labels == tuple(new_labels)


def test_add_dataset_to_run_and_list_datasets(csv_data):
    ds_name = f"dataset-{uuid4()}"
    ds_desc = f"sdk to add a dataset to a run {uuid4()}"

    with mock.patch("builtins.open", mock.mock_open(read_data=csv_data)):
        ds = nm.upload_csv("fake_path.csv", ds_name, "timestamp", "iso_8601", ds_desc)

    title = f"run-{uuid4()}"
    desc = f"sdk to add a dataset to a run {uuid4()}"
    start, end = _create_random_start_end()
    run = nm.create_run(title, start, end, desc)

    ref_name = f"ref-name-{uuid4()}"
    run.add_dataset(ref_name, ds)

    ds_list = run.list_datasets()
    assert len(ds_list) == 1
    ref_name2, ds2 = ds_list[0]
    assert ref_name2 == ref_name
    assert ds2.rid == ds.rid


def test_update_attachment(csv_data):
    at_name = f"attachment-{uuid4()}"
    at_desc = f"sdk to add a attachment to a run {uuid4()}"

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
    at_desc = f"sdk to add a attachment to a run {uuid4()}"

    with mock.patch("builtins.open", mock.mock_open(read_data=csv_data)):
        at = nm.upload_attachment("fake_path.csv", at_name, at_desc)

    title = f"run-{uuid4()}"
    desc = f"sdk to add a attachment to a run {uuid4()}"
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
