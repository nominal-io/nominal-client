from unittest.mock import MagicMock, patch

import pytest

from nominal.core.asset import Asset
from nominal.core.dataset import Dataset, DatasetBounds

SCOPE_NAME = "test-scope"


@pytest.fixture
def mock_clients():
    return MagicMock()


@pytest.fixture
def mock_asset(mock_clients):
    return Asset(
        rid="asset-rid-1",
        name="Test Asset",
        description=None,
        properties={},
        labels=[],
        created_at=0,
        _clients=mock_clients,
    )


@pytest.fixture
def mock_dataset(mock_clients):
    return Dataset(
        rid="dataset-rid-1",
        name="Test Dataset",
        description=None,
        bounds=DatasetBounds(start=0, end=1),
        properties={},
        labels=[],
        _clients=mock_clients,
    )


def test_get_or_create_dataset_returns_existing_when_no_tags(mock_asset, mock_dataset):
    """When a dataset exists with no tags and no tags are requested, it is returned as-is."""
    with patch.object(Asset, "_get_dataset_scope", return_value=(mock_dataset, {})):
        result = mock_asset.get_or_create_dataset(SCOPE_NAME)
    assert result == mock_dataset


def test_get_or_create_dataset_returns_existing_when_tags_match(mock_asset, mock_dataset):
    """When a dataset exists with tags and the same tags are requested, it is returned as-is."""
    tags = {"env": "prod", "robot": "r2"}
    with patch.object(Asset, "_get_dataset_scope", return_value=(mock_dataset, tags)):
        result = mock_asset.get_or_create_dataset(SCOPE_NAME, series_tags=tags)
    assert result == mock_dataset


def test_get_or_create_dataset_raises_when_tags_mismatch(mock_asset, mock_dataset):
    """When a dataset exists with different tags than requested, a ValueError is raised."""
    with (
        patch.object(Asset, "_get_dataset_scope", return_value=(mock_dataset, {"env": "prod"})),
        pytest.raises(ValueError, match="datascope already exists"),
    ):
        mock_asset.get_or_create_dataset(SCOPE_NAME, series_tags={"env": "staging"})


def test_get_or_create_dataset_raises_when_existing_has_tags_but_none_requested(mock_asset, mock_dataset):
    """When a dataset exists with tags but the caller requests no tags, a ValueError is raised."""
    with (
        patch.object(Asset, "_get_dataset_scope", return_value=(mock_dataset, {"env": "prod"})),
        pytest.raises(ValueError, match="datascope already exists"),
    ):
        mock_asset.get_or_create_dataset(SCOPE_NAME)


def test_get_or_create_dataset_raises_when_caller_has_tags_but_existing_has_none(mock_asset, mock_dataset):
    """When a dataset exists with no tags but the caller requests tags, a ValueError is raised."""
    with (
        patch.object(Asset, "_get_dataset_scope", return_value=(mock_dataset, {})),
        pytest.raises(ValueError, match="datascope already exists"),
    ):
        mock_asset.get_or_create_dataset(SCOPE_NAME, series_tags={"env": "prod"})


def test_get_or_create_dataset_creates_when_not_found(mock_asset, mock_dataset, mock_clients):
    """When no dataset scope exists, a new dataset is created and added to the asset."""
    mock_clients.resolve_default_workspace_rid.return_value = "workspace-rid"
    series_tags = {"env": "prod"}

    with (
        patch.object(Asset, "_get_dataset_scope", side_effect=ValueError("not found")),
        patch("nominal.core.asset._create_dataset", return_value=MagicMock()) as mock_create,
        patch.object(Dataset, "_from_conjure", return_value=mock_dataset),
        patch.object(Asset, "add_dataset") as mock_add,
    ):
        result = mock_asset.get_or_create_dataset(SCOPE_NAME, series_tags=series_tags)

    assert result == mock_dataset
    mock_create.assert_called_once()
    mock_add.assert_called_once_with(SCOPE_NAME, mock_dataset, series_tags=series_tags)


def test_get_or_create_dataset_creates_without_tags(mock_asset, mock_dataset, mock_clients):
    """When no dataset scope exists and no tags are given, add_dataset is called with series_tags=None."""
    mock_clients.resolve_default_workspace_rid.return_value = "workspace-rid"

    with (
        patch.object(Asset, "_get_dataset_scope", side_effect=ValueError("not found")),
        patch("nominal.core.asset._create_dataset", return_value=MagicMock()),
        patch.object(Dataset, "_from_conjure", return_value=mock_dataset),
        patch.object(Asset, "add_dataset") as mock_add,
    ):
        result = mock_asset.get_or_create_dataset(SCOPE_NAME)

    assert result == mock_dataset
    mock_add.assert_called_once_with(SCOPE_NAME, mock_dataset, series_tags=None)
