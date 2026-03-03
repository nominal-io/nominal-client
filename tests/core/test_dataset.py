from io import BytesIO
from unittest.mock import MagicMock, Mock

import pytest
from nominal_api import api, ingest_api

import nominal.core.dataset as dataset_module
from nominal.core.dataset import Dataset, DatasetBounds
from nominal.core.log import LogPoint
from nominal.core.unit import Unit

UNITS = [
    Unit(name="coulomb", symbol="C"),
    Unit(name="kilograms", symbol="kg"),
    Unit(name="mole", symbol="mol"),
]


@pytest.fixture
def mock_clients():
    clients = MagicMock()
    clients.logical_series = MagicMock()
    return clients


@pytest.fixture
def mock_dataset(mock_clients):
    ds = Dataset(
        rid="test-rid",
        name="Test Dataset",
        description="A dataset for testing",
        bounds=DatasetBounds(start=123455, end=123456),
        properties={},
        labels=[],
        _clients=mock_clients,
    )

    spy = MagicMock(wraps=ds.refresh)
    object.__setattr__(ds, "refresh", spy)
    ds.refresh.return_value = ds

    return ds


def test_write_logs_more_than_batch(mock_dataset: Dataset):
    endpoint = Mock()
    mock_dataset._clients.storage_writer.write_logs = endpoint

    log_0 = LogPoint(0, "a", {})
    log_1 = LogPoint(1, "b", {})
    log_2 = LogPoint(2, "c", {})

    def log_generator():
        yield log_0
        yield log_1
        yield log_2

    mock_dataset.write_logs(log_generator(), batch_size=2)

    assert len(endpoint.call_args_list) == 2

    _auth, _rid, first_req = endpoint.call_args_list[0][0]
    assert len(first_req.logs) == 2

    _auth, _rid, second_req = endpoint.call_args_list[1][0]
    assert len(second_req.logs) == 1


def test_write_logs_less_than_batch(mock_dataset: Dataset):
    endpoint = Mock()
    mock_dataset._clients.storage_writer.write_logs = endpoint

    log_0 = LogPoint(0, "a", {})
    log_1 = LogPoint(1, "b", {})
    log_2 = LogPoint(2, "c", {})

    def log_generator():
        yield log_0
        yield log_1
        yield log_2

    mock_dataset.write_logs(log_generator(), batch_size=1000)

    assert len(endpoint.call_args_list) == 1
    _auth, _rid, req = endpoint.call_args_list[0][0]
    assert len(req.logs) == 3


def test_create_mcap_ingest_request_supports_additional_file_tags():
    target = ingest_api.DatasetIngestTarget(existing=ingest_api.ExistingDatasetIngestDestination(dataset_rid="ds-rid"))
    channels = ingest_api.McapChannels(all=api.Empty())

    with_tags = dataset_module._create_mcap_ingest_request(
        "s3://bucket/with-tags.mcap", channels, target, tags={"vehicle": "A"}
    )
    without_tags = dataset_module._create_mcap_ingest_request("s3://bucket/no-tags.mcap", channels, target)

    assert with_tags.options.mcap_protobuf_timeseries.additional_file_tags == {"vehicle": "A"}
    assert without_tags.options.mcap_protobuf_timeseries.additional_file_tags is None


def test_add_mcap_from_io_forwards_tags_to_mcap_ingest_request(mock_dataset: Dataset, monkeypatch: pytest.MonkeyPatch):
    s3_path = "s3://bucket/sample.mcap"
    channels = Mock()
    request = Mock()
    response = Mock()
    dataset_file = Mock()

    monkeypatch.setattr(dataset_module, "upload_multipart_io", Mock(return_value=s3_path))
    monkeypatch.setattr(dataset_module, "_create_mcap_channels", Mock(return_value=channels))
    create_request = Mock(return_value=request)
    monkeypatch.setattr(dataset_module, "_create_mcap_ingest_request", create_request)

    mock_dataset._clients.ingest.ingest.return_value = response
    object.__setattr__(mock_dataset, "_handle_ingest_response", Mock(return_value=dataset_file))

    result = mock_dataset.add_mcap_from_io(BytesIO(b"test-mcap"), tags={"vehicle": "A"})

    called_s3_path, called_channels, called_target, called_tags = create_request.call_args.args
    assert called_s3_path == s3_path
    assert called_channels is channels
    assert called_target.existing.dataset_rid == mock_dataset.rid
    assert called_tags == {"vehicle": "A"}
    mock_dataset._clients.ingest.ingest.assert_called_once_with(mock_dataset._clients.auth_header, request)
    assert result is dataset_file


def test_dataset_wrapper_add_mcap_merges_scope_tags_and_user_tags():
    class TestDatasetWrapper(dataset_module._DatasetWrapper):
        def _list_dataset_scopes(self):
            return []

    wrapper = TestDatasetWrapper()
    scoped_dataset = MagicMock()
    scoped_dataset.add_mcap.return_value = "ok"
    wrapper._get_dataset_scope = Mock(return_value=(scoped_dataset, {"scope_only": "1", "override": "scope"}))

    result = wrapper.add_mcap(
        "scope-name",
        "/tmp/file.mcap",
        include_topics=["topic-a"],
        exclude_topics=["topic-b"],
        tags={"override": "user", "extra": "2"},
    )

    scoped_dataset.add_mcap.assert_called_once_with(
        "/tmp/file.mcap",
        include_topics=["topic-a"],
        exclude_topics=["topic-b"],
        tags={"scope_only": "1", "override": "user", "extra": "2"},
    )
    assert result == "ok"
