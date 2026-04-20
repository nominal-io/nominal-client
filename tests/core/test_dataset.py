from __future__ import annotations

from collections.abc import Iterator
from typing import Any, cast
from unittest.mock import MagicMock, Mock

import pytest

from nominal.core.dataset import Dataset, DatasetBounds, _api_base_url_to_grpc_target
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

    spy: MagicMock = MagicMock(wraps=ds.refresh)
    object.__setattr__(ds, "refresh", spy)
    spy.return_value = ds

    return ds


def test_write_logs_more_than_batch(mock_dataset: Dataset):
    endpoint = Mock()
    cast(Any, mock_dataset._clients.storage_writer).write_logs = endpoint

    log_0 = LogPoint(0, "a", {})
    log_1 = LogPoint(1, "b", {})
    log_2 = LogPoint(2, "c", {})

    def log_generator() -> Iterator[LogPoint]:
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
    cast(Any, mock_dataset._clients.storage_writer).write_logs = endpoint

    log_0 = LogPoint(0, "a", {})
    log_1 = LogPoint(1, "b", {})
    log_2 = LogPoint(2, "c", {})

    def log_generator() -> Iterator[LogPoint]:
        yield log_0
        yield log_1
        yield log_2

    mock_dataset.write_logs(log_generator(), batch_size=1000)

    assert len(endpoint.call_args_list) == 1
    _auth, _rid, req = endpoint.call_args_list[0][0]
    assert len(req.logs) == 3


def test_get_owner_rid_uses_role_service_lookup(monkeypatch: pytest.MonkeyPatch, mock_dataset: Dataset):
    def fake_lookup(*, auth_header: str, api_base_url: str, dataset_rid: str) -> str | None:
        assert auth_header == mock_dataset._clients.auth_header
        assert api_base_url == mock_dataset._clients._api_base_url
        assert dataset_rid == mock_dataset.rid
        return "ri.authn.user.owner"

    mock_dataset._clients.auth_header = "Bearer test-token"
    mock_dataset._clients._api_base_url = "https://api.gov.nominal.io/api"
    monkeypatch.setattr("nominal.core.dataset._get_dataset_owner_rid", fake_lookup)

    assert mock_dataset.get_owner_rid() == "ri.authn.user.owner"


def test_get_owner_rid_raises_when_no_owner_found(monkeypatch: pytest.MonkeyPatch, mock_dataset: Dataset):
    monkeypatch.setattr("nominal.core.dataset._get_dataset_owner_rid", lambda **_: None)

    with pytest.raises(ValueError, match="Could not resolve an owner for dataset"):
        mock_dataset.get_owner_rid()


def test_api_base_url_to_grpc_target_strips_scheme_and_api_suffix() -> None:
    assert _api_base_url_to_grpc_target("https://api.gov.nominal.io/api") == "api.gov.nominal.io"
    assert _api_base_url_to_grpc_target("https://api.gov.nominal.io") == "api.gov.nominal.io"
    assert _api_base_url_to_grpc_target("http://localhost:8080/api") == "localhost:8080"
