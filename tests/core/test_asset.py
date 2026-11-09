from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from nominal.core._utils.query_tools import ArchiveStatusFilter
from nominal.core.asset import Asset
from nominal.core.dataset import Dataset, DatasetBounds
from nominal.exceptions import NominalNotFoundError
from nominal.protos.asset.v2 import asset_pb2

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
        is_archived=False,
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
        is_archived=False,
        _clients=mock_clients,
    )


def _stub_scopes(mock_clients, *scopes: asset_pb2.DataScope, readable_rids: tuple[str, ...] = ()) -> None:
    """Serve the asset with these data scopes, and the catalog with only the readable datasets."""
    asset = asset_pb2.Asset(rid="asset-rid-1", data_scopes=list(scopes))
    mock_clients.assets.GetAssets.return_value = asset_pb2.GetAssetsResponse(responses={asset.rid: asset})
    mock_clients.catalog.get_enriched_datasets.return_value = [MagicMock(rid=rid) for rid in readable_rids]


def _dataset_scope(series_tags: dict[str, str]) -> asset_pb2.DataScope:
    return asset_pb2.DataScope(
        data_scope_name=SCOPE_NAME, data_source=asset_pb2.DataSource(dataset="dataset-rid-1"), series_tags=series_tags
    )


@pytest.mark.parametrize("tags", [{}, {"env": "prod", "robot": "r2"}])
def test_get_or_create_dataset_returns_existing_when_tags_match(mock_asset, mock_dataset, mock_clients, tags):
    """An existing dataset scope with the requested tags is returned as-is."""
    _stub_scopes(mock_clients, _dataset_scope(tags), readable_rids=("dataset-rid-1",))
    with patch.object(Dataset, "_from_conjure", return_value=mock_dataset):
        result = mock_asset.get_or_create_dataset(SCOPE_NAME, series_tags=tags or None)
    assert result == mock_dataset


@pytest.mark.parametrize(
    ("existing", "requested"),
    [({"env": "prod"}, {"env": "staging"}), ({"env": "prod"}, None), ({}, {"env": "prod"})],
)
def test_get_or_create_dataset_raises_when_tags_mismatch(mock_asset, mock_dataset, mock_clients, existing, requested):
    """An existing dataset scope with different tags than requested is an error, not a silent reuse."""
    _stub_scopes(mock_clients, _dataset_scope(existing), readable_rids=("dataset-rid-1",))
    with (
        patch.object(Dataset, "_from_conjure", return_value=mock_dataset),
        pytest.raises(ValueError, match="datascope already exists"),
    ):
        mock_asset.get_or_create_dataset(SCOPE_NAME, series_tags=requested)


def test_get_or_create_dataset_refuses_a_taken_name_whose_dataset_is_unreadable(mock_asset, mock_clients):
    """A scope name that exists but does not resolve is taken: raise rather than create an orphaned dataset."""
    _stub_scopes(mock_clients, _dataset_scope({}))
    with (
        patch("nominal.core.asset._create_dataset") as mock_create,
        pytest.raises(NominalNotFoundError, match=f"No dataset with data scope name '{SCOPE_NAME}' found"),
    ):
        mock_asset.get_or_create_dataset(SCOPE_NAME)
    mock_create.assert_not_called()


@pytest.mark.parametrize(
    ("get_or_create", "create", "held_by"),
    [
        (Asset.get_or_create_dataset, "_create_dataset", asset_pb2.DataSource(connection="connection-rid-1")),
        (Asset.get_or_create_video, "_create_video", asset_pb2.DataSource(dataset="dataset-rid-1")),
    ],
)
def test_get_or_create_refuses_a_name_held_by_another_scope_type(
    mock_asset, mock_clients, get_or_create, create, held_by
):
    """Scope names are unique across types, so raise before creating a resource the asset would then reject."""
    _stub_scopes(mock_clients, asset_pb2.DataScope(data_scope_name=SCOPE_NAME, data_source=held_by))
    with (
        patch(f"nominal.core.asset.{create}") as mock_create,
        pytest.raises(ValueError, match="a data scope of another type on asset asset-rid-1 has that name"),
    ):
        get_or_create(mock_asset, SCOPE_NAME)
    mock_create.assert_not_called()


@pytest.mark.parametrize("series_tags", [{"env": "prod"}, None])
def test_get_or_create_dataset_creates_when_the_name_is_free(mock_asset, mock_dataset, mock_clients, series_tags):
    """When no dataset scope has the name, a new dataset is created and added to the asset with the given tags."""
    _stub_scopes(mock_clients)
    mock_clients.resolve_default_workspace_rid.return_value = "workspace-rid"

    with (
        patch("nominal.core.asset._create_dataset", return_value=MagicMock()) as mock_create,
        patch.object(Dataset, "_from_conjure", return_value=mock_dataset),
        patch.object(Asset, "add_dataset") as mock_add,
    ):
        result = mock_asset.get_or_create_dataset(SCOPE_NAME, series_tags=series_tags)

    assert result == mock_dataset
    mock_create.assert_called_once()
    mock_add.assert_called_once_with(SCOPE_NAME, mock_dataset, series_tags=series_tags)


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


@pytest.fixture
def asset_with_scopes(mock_asset, mock_clients):
    """An asset with one dataset-backed and one video-backed scope, neither carrying an offset."""
    api_asset = asset_pb2.Asset(
        rid=mock_asset.rid,
        title=mock_asset.name,
        data_scopes=[
            asset_pb2.DataScope(
                data_scope_name="ds",
                data_source=asset_pb2.DataSource(dataset="ri.dataset.1"),
                series_tags={"vehicle": "a"},
            ),
            asset_pb2.DataScope(data_scope_name="vid", data_source=asset_pb2.DataSource(video="ri.video.1")),
        ],
    )
    mock_clients.assets.GetAssets.return_value = asset_pb2.GetAssetsResponse(responses={mock_asset.rid: api_asset})
    mock_clients.assets.UpdateAsset.return_value = asset_pb2.UpdateAssetResponse(asset=api_asset)
    return mock_asset


def test_remove_data_scopes_resends_survivors_unchanged(asset_with_scopes, mock_clients):
    """Surviving scopes are re-sent as they were; an unset proto offset must not become an explicit zero."""
    asset_with_scopes.remove_data_scopes(scopes=["ri.video.1"])

    kept = mock_clients.assets.UpdateAsset.call_args.args[0].data_scopes.data_scopes
    assert list(kept) == [
        asset_pb2.CreateAssetDataScope(
            data_scope_name="ds",
            data_source=asset_pb2.DataSource(dataset="ri.dataset.1"),
            series_tags={"vehicle": "a"},
        )
    ]


def test_remove_data_scopes_ignores_unset_source_fields(asset_with_scopes, mock_clients):
    """Unset oneof fields read as "", so an empty rid must not match, and remove, every scope."""
    asset_with_scopes.remove_data_scopes(scopes=[""])

    kept = mock_clients.assets.UpdateAsset.call_args.args[0].data_scopes.data_scopes
    assert [scope.data_scope_name for scope in kept] == ["ds", "vid"]
