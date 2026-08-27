from __future__ import annotations

from typing import Sequence
from unittest.mock import MagicMock, patch

import pytest
from google.protobuf import timestamp_pb2

from nominal.core._utils.query_tools import ArchiveStatusFilter
from nominal.core.asset import Asset
from nominal.core.client import NominalClient
from nominal.core.dataset import Dataset, DatasetBounds
from nominal.core.exceptions import NominalNotFoundError
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
        updated_at=0,
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


def _proto_asset(
    rid: str = "ri.asset.test",
    *,
    title: str = "original",
    description: str | None = None,
    created_by: str | None = None,
    data_scopes: Sequence[asset_pb2.DataScope] = (),
) -> asset_pb2.Asset:
    return asset_pb2.Asset(
        rid=rid,
        title=title,
        description=description,
        created_by=created_by,
        created_at=timestamp_pb2.Timestamp(seconds=1),
        updated_at=timestamp_pb2.Timestamp(seconds=2),
        data_scopes=list(data_scopes),
    )


@pytest.fixture
def update_asset(mock_clients, mock_asset):
    """The stubbed UpdateAsset, echoing the same asset back for `update()` to refresh from."""
    mock_clients.assets.UpdateAsset.return_value = asset_pb2.UpdateAssetResponse(asset=_proto_asset(mock_asset.rid))
    return mock_clients.assets.UpdateAsset


@pytest.fixture
def asset_with_scopes(mock_asset, mock_clients):
    """An asset resolving to one dataset-backed scope and one video-backed scope."""
    mock_clients.assets.GetAssets.return_value = asset_pb2.GetAssetsResponse(
        responses={
            mock_asset.rid: _proto_asset(
                mock_asset.rid,
                data_scopes=[
                    asset_pb2.DataScope(
                        data_scope_name="ds",
                        data_source=asset_pb2.DataSource(dataset="ri.dataset.1"),
                        series_tags={"vehicle": "a"},
                    ),
                    asset_pb2.DataScope(data_scope_name="vid", data_source=asset_pb2.DataSource(video="ri.video.1")),
                ],
            )
        }
    )
    return mock_asset


def test_update_omits_absent_fields_so_the_backend_leaves_them_unchanged(mock_asset, update_asset) -> None:
    """None must not reach the wire: an omitted field is how a caller says "leave this alone"."""
    mock_asset.update(description="only the description")

    request = update_asset.call_args.args[0]
    assert request.asset_rid == mock_asset.rid
    assert request.description == "only the description"
    for field in ("title", "labels", "properties", "links", "data_scopes"):
        assert not request.HasField(field), f"{field} should be absent when omitted"


def test_update_sends_empty_collections_as_explicit_clears(mock_asset, update_asset) -> None:
    """An empty collection is a clear, which the update wrappers make distinguishable from omission."""
    mock_asset.update(labels=[], properties={}, links=[])

    request = update_asset.call_args.args[0]
    assert (request.HasField("labels"), list(request.labels.labels)) == (True, [])
    assert (request.HasField("properties"), dict(request.properties.properties)) == (True, {})
    assert (request.HasField("links"), list(request.links.links)) == (True, [])


def test_remove_data_scopes_drops_by_rid_and_keeps_survivors_intact(asset_with_scopes, update_asset) -> None:
    """Survivors must round-trip without gaining an offset they never had."""
    asset_with_scopes.remove_data_scopes(scopes=["ri.video.1"])

    kept = update_asset.call_args.args[0].data_scopes.data_scopes
    assert [scope.data_scope_name for scope in kept] == ["ds"]
    assert kept[0].data_source.dataset == "ri.dataset.1"
    assert dict(kept[0].series_tags) == {"vehicle": "a"}
    assert not kept[0].HasField("offset"), "an unset offset must not be re-sent as an explicit zero"


def test_scopes_are_selected_by_the_oneof_discriminator(asset_with_scopes) -> None:
    """The proto data_source is a oneof, so scope kind comes from which field is set."""
    assert asset_with_scopes._scope_rids("dataset") == {"ds": "ri.dataset.1"}
    assert asset_with_scopes._scope_rids("video") == {"vid": "ri.video.1"}
    assert [
        (scope.data_scope_name, scope.data_source.dataset, dict(scope.series_tags))
        for scope in asset_with_scopes._dataset_scopes()
    ] == [("ds", "ri.dataset.1", {"vehicle": "a"})]


def test_lookup_dataset_scope_resolves_a_name_to_its_dataset_and_tags(asset_with_scopes) -> None:
    """`_DatasetWrapper` resolves scope names through this, and only dataset-backed scopes can answer."""
    dataset_rid, series_tags = asset_with_scopes._lookup_dataset_scope("ds")
    assert (dataset_rid, dict(series_tags)) == ("ri.dataset.1", {"vehicle": "a"})

    assert asset_with_scopes._lookup_dataset_scope("vid") is None
    assert asset_with_scopes._lookup_dataset_scope("absent") is None


def test_from_proto_reads_optional_fields() -> None:
    """Description and created_by are optional on the wire; absent must read as None, not empty string."""
    absent = Asset._from_proto(MagicMock(), _proto_asset())
    assert (absent.description, absent.created_by_rid) == (None, None)

    present = Asset._from_proto(MagicMock(), _proto_asset(description="d", created_by="ri.user.1"))
    assert (present.description, present.created_by_rid) == ("d", "ri.user.1")


def test_get_asset_raises_not_found_when_the_rid_is_absent(mock_clients) -> None:
    """GetAssets answers a missing rid with an absent map entry, never a NOT_FOUND status."""
    mock_clients.assets.GetAssets.return_value = asset_pb2.GetAssetsResponse()

    with pytest.raises(NominalNotFoundError, match="no asset found with RID"):
        NominalClient(_clients=mock_clients).get_asset("ri.asset.missing")
