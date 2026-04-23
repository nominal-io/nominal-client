from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from nominal.core.dataset import Dataset, DatasetBounds
from nominal.core.user import User
from nominal.experimental.dataset_utils import get_dataset_owner, get_dataset_owner_rid
from nominal.experimental.dataset_utils._dataset_utils import _api_base_url_to_grpc_target


@pytest.fixture
def mock_dataset():
    clients = MagicMock()
    dataset = Dataset(
        rid="test-rid",
        name="Test Dataset",
        description="A dataset for testing",
        bounds=DatasetBounds(start=123455, end=123456),
        properties={},
        labels=[],
        _clients=clients,
    )
    clients.auth_header = "Bearer test-token"
    clients._api_base_url = "https://api.gov.nominal.io/api"
    return dataset


def test_get_dataset_owner_rid_uses_role_service_lookup(monkeypatch: pytest.MonkeyPatch, mock_dataset: Dataset):
    def fake_lookup(*, auth_header: str, api_base_url: str, dataset_rid: str) -> str | None:
        assert auth_header is mock_dataset._clients.auth_header
        assert api_base_url is mock_dataset._clients._api_base_url  # type: ignore[attr-defined]
        assert dataset_rid == mock_dataset.rid
        return "ri.authn.user.owner"

    monkeypatch.setattr("nominal.experimental.dataset_utils._dataset_utils._lookup_dataset_owner_rid", fake_lookup)

    assert get_dataset_owner_rid(mock_dataset) == "ri.authn.user.owner"


def test_get_dataset_owner_returns_user_from_owner_rid(monkeypatch: pytest.MonkeyPatch, mock_dataset: Dataset):
    mock_dataset._clients.authentication.get_user.return_value = User(  # type: ignore[attr-defined]
        rid="ri.authn.user.owner",
        display_name="Owner User",
        email="owner@nominal.io",
    )
    monkeypatch.setattr(
        "nominal.experimental.dataset_utils._dataset_utils.get_dataset_owner_rid",
        lambda dataset: "ri.authn.user.owner",
    )

    owner = get_dataset_owner(mock_dataset)

    assert owner.rid == "ri.authn.user.owner"
    mock_dataset._clients.authentication.get_user.assert_called_once_with(  # type: ignore[attr-defined]
        mock_dataset._clients.auth_header, "ri.authn.user.owner"
    )


def test_get_dataset_owner_rid_raises_when_no_owner_found(monkeypatch: pytest.MonkeyPatch, mock_dataset: Dataset):
    monkeypatch.setattr("nominal.experimental.dataset_utils._dataset_utils._lookup_dataset_owner_rid", lambda **_: None)

    with pytest.raises(ValueError, match="Could not resolve an owner for dataset"):
        get_dataset_owner_rid(mock_dataset)


def test_api_base_url_to_grpc_target_strips_scheme_and_api_suffix() -> None:
    assert _api_base_url_to_grpc_target("https://api.gov.nominal.io/api") == "api.gov.nominal.io"
    assert _api_base_url_to_grpc_target("https://api.gov.nominal.io") == "api.gov.nominal.io"
    assert _api_base_url_to_grpc_target("http://localhost:8080/api") == "localhost:8080"
