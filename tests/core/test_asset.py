from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from nominal.core._utils.query_tools import ArchiveStatusFilter
from nominal.core.asset import Asset
from nominal.core.connection import Connection
from nominal.core.dataset import Dataset
from nominal.core.video import Video

SCOPE_NAME = "test-scope"


def _asset_api_with_scopes(*scopes):
    return SimpleNamespace(data_scopes=list(scopes))


def _scope(scope_name: str, scope_type: str, rid: str):
    data_source = SimpleNamespace(type=scope_type)
    setattr(data_source, scope_type, rid)
    return SimpleNamespace(data_scope_name=scope_name, data_source=data_source)


def _set_asset_scopes(mock_clients, asset_rid: str, *scopes):
    mock_clients.assets.get_assets.return_value = {asset_rid: _asset_api_with_scopes(*scopes)}


def _assert_asset_fetched(mock_clients, asset_rid: str):
    mock_clients.assets.get_assets.assert_called_once_with(mock_clients.auth_header, [asset_rid])


def _assert_dataset_fetched(mock_clients, dataset_rid: str):
    mock_clients.catalog.get_enriched_datasets.assert_called_once()
    args = mock_clients.catalog.get_enriched_datasets.call_args.args
    assert args[0] == mock_clients.auth_header
    assert args[1].dataset_rids == [dataset_rid]


def test_get_dataset_fetches_scoped_dataset(mock_asset, mock_clients, mock_dataset):
    """A dataset scope name is resolved to one backing dataset fetch."""
    raw_dataset = MagicMock()
    mock_clients.catalog.get_enriched_datasets.return_value = [raw_dataset]
    _set_asset_scopes(mock_clients, mock_asset.rid, _scope(SCOPE_NAME, "dataset", "dataset-rid-1"))

    with patch.object(Dataset, "_from_conjure", return_value=mock_dataset) as from_conjure:
        result = mock_asset.get_dataset(SCOPE_NAME)

    assert result == mock_dataset
    _assert_asset_fetched(mock_clients, mock_asset.rid)
    _assert_dataset_fetched(mock_clients, "dataset-rid-1")
    from_conjure.assert_called_once_with(mock_clients, raw_dataset)


def test_get_connection_fetches_scoped_connection(mock_asset, mock_clients, mock_connection):
    """A connection scope name is resolved to one backing connection fetch."""
    raw_connection = MagicMock()
    mock_clients.connection.get_connection.return_value = raw_connection
    _set_asset_scopes(mock_clients, mock_asset.rid, _scope(SCOPE_NAME, "connection", "connection-rid-1"))

    with patch.object(Connection, "_from_conjure", return_value=mock_connection) as from_conjure:
        result = mock_asset.get_connection(SCOPE_NAME)

    assert result == mock_connection
    _assert_asset_fetched(mock_clients, mock_asset.rid)
    mock_clients.connection.get_connection.assert_called_once_with(mock_clients.auth_header, "connection-rid-1")
    from_conjure.assert_called_once_with(mock_clients, raw_connection)


def test_get_video_fetches_scoped_video(mock_asset, mock_clients, mock_video):
    """A video scope name is resolved to one backing video fetch."""
    raw_video = MagicMock()
    mock_clients.video.get.return_value = raw_video
    _set_asset_scopes(mock_clients, mock_asset.rid, _scope(SCOPE_NAME, "video", "video-rid-1"))

    with patch.object(Video, "_from_conjure", return_value=mock_video) as from_conjure:
        result = mock_asset.get_video(SCOPE_NAME)

    assert result == mock_video
    _assert_asset_fetched(mock_clients, mock_asset.rid)
    mock_clients.video.get.assert_called_once_with(mock_clients.auth_header, "video-rid-1")
    from_conjure.assert_called_once_with(mock_clients, raw_video)


def test_get_connection_raises_value_error_when_connection_scope_is_missing(mock_asset, mock_clients):
    """A same-named scope of another type does not satisfy a typed connection lookup."""
    _set_asset_scopes(mock_clients, mock_asset.rid, _scope(SCOPE_NAME, "dataset", "dataset-rid-1"))

    with pytest.raises(ValueError, match=f"No connection with data scope name '{SCOPE_NAME}' found for this asset"):
        mock_asset.get_connection(SCOPE_NAME)

    _assert_asset_fetched(mock_clients, mock_asset.rid)
    mock_clients.connection.get_connection.assert_not_called()


def test_get_dataset_raises_value_error_when_dataset_scope_is_missing(mock_asset, mock_clients):
    """A same-named scope of another type does not satisfy a typed dataset lookup."""
    _set_asset_scopes(mock_clients, mock_asset.rid, _scope(SCOPE_NAME, "video", "video-rid-1"))

    with pytest.raises(ValueError, match=f"No dataset with data scope name '{SCOPE_NAME}' found for this asset"):
        mock_asset.get_dataset(SCOPE_NAME)

    _assert_asset_fetched(mock_clients, mock_asset.rid)
    mock_clients.catalog.get_enriched_datasets.assert_not_called()


def test_get_video_raises_value_error_when_video_scope_is_missing(mock_asset, mock_clients):
    """A same-named scope of another type does not satisfy a typed video lookup."""
    _set_asset_scopes(mock_clients, mock_asset.rid, _scope(SCOPE_NAME, "dataset", "dataset-rid-1"))

    with pytest.raises(ValueError, match=f"No video with data scope name '{SCOPE_NAME}' found for this asset"):
        mock_asset.get_video(SCOPE_NAME)

    _assert_asset_fetched(mock_clients, mock_asset.rid)
    mock_clients.video.get.assert_not_called()


def test_get_dataset_propagates_backing_dataset_error(mock_asset, mock_clients):
    """If the scope exists but the dataset fetch fails, the fetch error is surfaced."""
    error = ValueError("dataset 'stale-dataset-rid' not found")
    mock_clients.catalog.get_enriched_datasets.side_effect = error
    _set_asset_scopes(mock_clients, mock_asset.rid, _scope(SCOPE_NAME, "dataset", "stale-dataset-rid"))

    with pytest.raises(ValueError) as exc_info:
        mock_asset.get_dataset(SCOPE_NAME)

    assert exc_info.value is error
    _assert_asset_fetched(mock_clients, mock_asset.rid)
    _assert_dataset_fetched(mock_clients, "stale-dataset-rid")


def test_get_connection_propagates_backing_connection_error(mock_asset, mock_clients):
    """If the scope exists but the connection fetch fails, the fetch error is surfaced."""
    error = RuntimeError("connection 'stale-connection-rid' not found")
    mock_clients.connection.get_connection.side_effect = error
    _set_asset_scopes(mock_clients, mock_asset.rid, _scope(SCOPE_NAME, "connection", "stale-connection-rid"))

    with pytest.raises(RuntimeError) as exc_info:
        mock_asset.get_connection(SCOPE_NAME)

    assert exc_info.value is error
    _assert_asset_fetched(mock_clients, mock_asset.rid)
    mock_clients.connection.get_connection.assert_called_once_with(mock_clients.auth_header, "stale-connection-rid")


def test_get_video_propagates_backing_video_error(mock_asset, mock_clients):
    """If the scope exists but the video fetch fails, the fetch error is surfaced."""
    error = RuntimeError("video 'stale-video-rid' not found")
    mock_clients.video.get.side_effect = error
    _set_asset_scopes(mock_clients, mock_asset.rid, _scope(SCOPE_NAME, "video", "stale-video-rid"))

    with pytest.raises(RuntimeError) as exc_info:
        mock_asset.get_video(SCOPE_NAME)

    assert exc_info.value is error
    _assert_asset_fetched(mock_clients, mock_asset.rid)
    mock_clients.video.get.assert_called_once_with(mock_clients.auth_header, "stale-video-rid")


def test_get_or_create_video_creates_when_scope_is_missing(mock_asset, mock_clients, mock_video):
    """A missing video scope still enters the get-or-create create path."""
    raw_video = MagicMock()
    mock_clients.resolve_default_workspace_rid.return_value = "workspace-rid"
    _set_asset_scopes(mock_clients, mock_asset.rid, _scope(SCOPE_NAME, "dataset", "dataset-rid-1"))

    with (
        patch("nominal.core.asset._create_video", return_value=raw_video) as create_video,
        patch.object(Video, "_from_conjure", return_value=mock_video) as from_conjure,
        patch.object(Asset, "add_video") as add_video,
    ):
        result = mock_asset.get_or_create_video(
            SCOPE_NAME,
            name="created-video",
            description="description",
            labels=["label"],
            properties={"key": "value"},
        )

    assert result == mock_video
    _assert_asset_fetched(mock_clients, mock_asset.rid)
    create_video.assert_called_once_with(
        mock_clients.auth_header,
        mock_clients.video,
        "created-video",
        description="description",
        properties={"key": "value"},
        labels=["label"],
        workspace_rid="workspace-rid",
    )
    from_conjure.assert_called_once_with(mock_clients, raw_video)
    add_video.assert_called_once_with(SCOPE_NAME, mock_video)


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
