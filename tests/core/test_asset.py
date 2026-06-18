from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from nominal.core._utils.query_tools import ArchiveStatusFilter
from nominal.core.asset import Asset
from nominal.core.connection import Connection
from nominal.core.dataset import Dataset, DatasetBounds
from nominal.core.video import Video

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


@pytest.fixture
def mock_connection(mock_clients):
    return Connection(
        rid="connection-rid-1",
        name="Test Connection",
        description=None,
        _clients=mock_clients,
    )


@pytest.fixture
def mock_video(mock_clients):
    return Video(
        rid="video-rid-1",
        name="Test Video",
        description=None,
        properties={},
        labels=[],
        created_at=0,
        _clients=mock_clients,
    )


def _asset_api_with_scopes(*scopes):
    return SimpleNamespace(data_scopes=list(scopes))


def _scope(scope_name: str, scope_type: str, rid: str):
    data_source = SimpleNamespace(type=scope_type)
    setattr(data_source, scope_type, rid)
    return SimpleNamespace(data_scope_name=scope_name, data_source=data_source)


def test_get_dataset_fetches_scoped_dataset(mock_asset, mock_clients, mock_dataset):
    """A dataset scope name is resolved to one backing dataset fetch."""
    raw_dataset = MagicMock()

    with (
        patch.object(
            Asset,
            "_get_latest_api",
            return_value=_asset_api_with_scopes(_scope(SCOPE_NAME, "dataset", "dataset-rid-1")),
        ),
        patch("nominal.core.asset._get_dataset", return_value=raw_dataset) as get_dataset,
        patch.object(Dataset, "_from_conjure", return_value=mock_dataset) as from_conjure,
    ):
        result = mock_asset.get_dataset(SCOPE_NAME)

    assert result == mock_dataset
    get_dataset.assert_called_once_with(mock_clients.auth_header, mock_clients.catalog, "dataset-rid-1")
    from_conjure.assert_called_once_with(mock_clients, raw_dataset)


def test_get_connection_fetches_scoped_connection(mock_asset, mock_clients, mock_connection):
    """A connection scope name is resolved to one backing connection fetch."""
    raw_connection = MagicMock()
    mock_clients.connection.get_connection.return_value = raw_connection

    with (
        patch.object(
            Asset,
            "_get_latest_api",
            return_value=_asset_api_with_scopes(_scope(SCOPE_NAME, "connection", "connection-rid-1")),
        ),
        patch.object(Connection, "_from_conjure", return_value=mock_connection) as from_conjure,
    ):
        result = mock_asset.get_connection(SCOPE_NAME)

    assert result == mock_connection
    mock_clients.connection.get_connection.assert_called_once_with(mock_clients.auth_header, "connection-rid-1")
    from_conjure.assert_called_once_with(mock_clients, raw_connection)


def test_get_video_fetches_scoped_video(mock_asset, mock_clients, mock_video):
    """A video scope name is resolved to one backing video fetch."""
    raw_video = MagicMock()

    with (
        patch.object(
            Asset,
            "_get_latest_api",
            return_value=_asset_api_with_scopes(_scope(SCOPE_NAME, "video", "video-rid-1")),
        ),
        patch("nominal.core.asset._get_video", return_value=raw_video) as get_video,
        patch.object(Video, "_from_conjure", return_value=mock_video) as from_conjure,
    ):
        result = mock_asset.get_video(SCOPE_NAME)

    assert result == mock_video
    get_video.assert_called_once_with(mock_clients, "video-rid-1")
    from_conjure.assert_called_once_with(mock_clients, raw_video)


def test_get_connection_raises_key_error_when_connection_scope_is_missing(mock_asset, mock_clients):
    """A same-named scope of another type does not satisfy a typed connection lookup."""
    with (
        patch.object(
            Asset,
            "_get_latest_api",
            return_value=_asset_api_with_scopes(_scope(SCOPE_NAME, "dataset", "dataset-rid-1")),
        ),
        pytest.raises(KeyError, match=f"No connection with data scope name '{SCOPE_NAME}' found for this asset"),
    ):
        mock_asset.get_connection(SCOPE_NAME)

    mock_clients.connection.get_connection.assert_not_called()


def test_get_dataset_raises_key_error_when_dataset_scope_is_missing(mock_asset):
    """A same-named scope of another type does not satisfy a typed dataset lookup."""
    with (
        patch.object(
            Asset,
            "_get_latest_api",
            return_value=_asset_api_with_scopes(_scope(SCOPE_NAME, "video", "video-rid-1")),
        ),
        patch("nominal.core.asset._get_dataset") as get_dataset,
        pytest.raises(KeyError, match=f"No dataset with data scope name '{SCOPE_NAME}' found for this asset"),
    ):
        mock_asset.get_dataset(SCOPE_NAME)

    get_dataset.assert_not_called()


def test_get_video_raises_key_error_when_video_scope_is_missing(mock_asset):
    """A same-named scope of another type does not satisfy a typed video lookup."""
    with (
        patch.object(
            Asset,
            "_get_latest_api",
            return_value=_asset_api_with_scopes(_scope(SCOPE_NAME, "dataset", "dataset-rid-1")),
        ),
        patch("nominal.core.asset._get_video") as get_video,
        pytest.raises(KeyError, match=f"No video with data scope name '{SCOPE_NAME}' found for this asset"),
    ):
        mock_asset.get_video(SCOPE_NAME)

    get_video.assert_not_called()


def test_get_dataset_propagates_backing_dataset_error(mock_asset):
    """If the scope exists but the dataset fetch fails, the fetch error is surfaced."""
    error = ValueError("dataset 'stale-dataset-rid' not found")

    with (
        patch.object(
            Asset,
            "_get_latest_api",
            return_value=_asset_api_with_scopes(_scope(SCOPE_NAME, "dataset", "stale-dataset-rid")),
        ),
        patch("nominal.core.asset._get_dataset", side_effect=error),
        patch.object(Dataset, "_from_conjure") as from_conjure,
        pytest.raises(ValueError) as exc_info,
    ):
        mock_asset.get_dataset(SCOPE_NAME)

    assert exc_info.value is error
    from_conjure.assert_not_called()


def test_get_connection_propagates_backing_connection_error(mock_asset, mock_clients):
    """If the scope exists but the connection fetch fails, the fetch error is surfaced."""
    error = RuntimeError("connection 'stale-connection-rid' not found")
    mock_clients.connection.get_connection.side_effect = error

    with (
        patch.object(
            Asset,
            "_get_latest_api",
            return_value=_asset_api_with_scopes(_scope(SCOPE_NAME, "connection", "stale-connection-rid")),
        ),
        patch.object(Connection, "_from_conjure") as from_conjure,
        pytest.raises(RuntimeError) as exc_info,
    ):
        mock_asset.get_connection(SCOPE_NAME)

    assert exc_info.value is error
    from_conjure.assert_not_called()


def test_get_video_propagates_backing_video_error(mock_asset):
    """If the scope exists but the video fetch fails, the fetch error is surfaced."""
    error = RuntimeError("video 'stale-video-rid' not found")

    with (
        patch.object(
            Asset,
            "_get_latest_api",
            return_value=_asset_api_with_scopes(_scope(SCOPE_NAME, "video", "stale-video-rid")),
        ),
        patch("nominal.core.asset._get_video", side_effect=error),
        patch.object(Video, "_from_conjure") as from_conjure,
        pytest.raises(RuntimeError) as exc_info,
    ):
        mock_asset.get_video(SCOPE_NAME)

    assert exc_info.value is error
    from_conjure.assert_not_called()


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


def test_search_events_passes_archive_status(mock_asset):
    """Asset.search_events forwards archive_status to the shared event search helper."""
    with patch("nominal.core.asset._search_events", return_value=[]) as mock_search_events:
        result = mock_asset.search_events(archive_status=ArchiveStatusFilter.ANY)

    assert result == []
    mock_search_events.assert_called_once()
    assert mock_search_events.call_args.kwargs["asset_rids"] == [mock_asset.rid]
    assert mock_search_events.call_args.kwargs["archive_status"] == ArchiveStatusFilter.ANY


def test_search_data_reviews_passes_archive_status(mock_asset):
    """Asset.search_data_reviews forwards archive_status to the shared data-review iterator."""
    with patch("nominal.core.asset.data_review._iter_search_data_reviews", return_value=iter(())) as mock_reviews:
        result = mock_asset.search_data_reviews(archive_status=ArchiveStatusFilter.ARCHIVED)

    assert result == []
    mock_reviews.assert_called_once()
    assert mock_reviews.call_args.kwargs["assets"] == [mock_asset.rid]
    assert mock_reviews.call_args.kwargs["archive_status"] == ArchiveStatusFilter.ARCHIVED
