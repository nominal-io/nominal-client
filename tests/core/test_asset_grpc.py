from __future__ import annotations

from unittest.mock import MagicMock

import grpc
import pytest
from google.protobuf import timestamp_pb2

from nominal.core.asset import Asset, _create_proto_links
from nominal.core.client import NominalClient
from nominal.core.exceptions import NominalNotFoundError
from nominal.protos.asset.v2 import asset_pb2


def _proto_asset(rid: str = "ri.asset.test", **kwargs: object) -> asset_pb2.Asset:
    defaults: dict[str, object] = {"title": "original", "created_at": timestamp_pb2.Timestamp(seconds=1)}
    defaults.update(kwargs)
    return asset_pb2.Asset(rid=rid, **defaults)  # type: ignore[arg-type]


def _asset(clients: MagicMock, rid: str = "ri.asset.test") -> Asset:
    return Asset._from_proto(clients, _proto_asset(rid, labels=["keep"]))


def test_update_leaves_omitted_fields_absent_on_the_wire() -> None:
    """Fields not passed to update() are absent from the request, so the backend leaves them unchanged."""
    clients = MagicMock()
    asset = _asset(clients)
    clients.assets.UpdateAsset.return_value = asset_pb2.UpdateAssetResponse(
        asset=_proto_asset(asset.rid, title="renamed")
    )

    asset.update(name="renamed")

    request = clients.assets.UpdateAsset.call_args.args[0]
    assert request.asset_rid == asset.rid
    assert request.title == "renamed"
    assert not request.HasField("description")
    assert not request.HasField("labels")
    assert not request.HasField("properties")
    assert not request.HasField("links")
    assert not request.HasField("data_scopes")
    assert asset.name == "renamed"


def test_update_sends_empty_collections_as_explicit_clears() -> None:
    """Passing empty labels/properties sends present-but-empty wrappers (clear), distinct from omission."""
    clients = MagicMock()
    asset = _asset(clients)
    clients.assets.UpdateAsset.return_value = asset_pb2.UpdateAssetResponse(asset=_proto_asset(asset.rid))

    asset.update(labels=[], properties={}, links=[])

    request = clients.assets.UpdateAsset.call_args.args[0]
    assert request.HasField("labels")
    assert list(request.labels.labels) == []
    assert request.HasField("properties")
    assert dict(request.properties.properties) == {}
    assert request.HasField("links")
    assert list(request.links.links) == []


def test_update_omitting_name_leaves_title_absent() -> None:
    """Title has explicit presence, so an update that does not pass name cannot blank the asset's title."""
    clients = MagicMock()
    asset = _asset(clients)
    clients.assets.UpdateAsset.return_value = asset_pb2.UpdateAssetResponse(asset=_proto_asset(asset.rid))

    asset.update(description="only the description")

    request = clients.assets.UpdateAsset.call_args.args[0]
    assert not request.HasField("title")
    assert request.HasField("description")


def test_remove_data_scopes_keeps_survivors_intact() -> None:
    """Scopes are dropped by name and by rid; survivors round-trip without gaining fields they never had."""
    clients = MagicMock()
    asset = _asset(clients)
    clients.assets.GetAssets.return_value = asset_pb2.GetAssetsResponse(
        responses={
            asset.rid: _proto_asset(
                asset.rid,
                data_scopes=[
                    asset_pb2.DataScope(
                        data_scope_name="by-name", data_source=asset_pb2.DataSource(dataset="ri.dataset.1")
                    ),
                    asset_pb2.DataScope(
                        data_scope_name="by-rid", data_source=asset_pb2.DataSource(video="ri.video.doomed")
                    ),
                    asset_pb2.DataScope(
                        data_scope_name="survivor",
                        data_source=asset_pb2.DataSource(dataset="ri.dataset.2"),
                        series_tags={"vehicle": "a"},
                    ),
                ],
            )
        }
    )
    clients.assets.UpdateAsset.return_value = asset_pb2.UpdateAssetResponse(asset=_proto_asset(asset.rid))

    asset.remove_data_scopes(names=["by-name"], scopes=["ri.video.doomed"])

    kept = clients.assets.UpdateAsset.call_args.args[0].data_scopes.data_scopes
    assert [scope.data_scope_name for scope in kept] == ["survivor"]
    assert kept[0].data_source.dataset == "ri.dataset.2"
    assert dict(kept[0].series_tags) == {"vehicle": "a"}


def test_remove_data_scopes_does_not_invent_an_offset() -> None:
    """Reading an unset message field yields a default instance; re-sending it must not mark offset present."""
    clients = MagicMock()
    asset = _asset(clients)
    clients.assets.GetAssets.return_value = asset_pb2.GetAssetsResponse(
        responses={
            asset.rid: _proto_asset(
                asset.rid,
                data_scopes=[
                    asset_pb2.DataScope(
                        data_scope_name="keep", data_source=asset_pb2.DataSource(dataset="ri.dataset.1")
                    )
                ],
            )
        }
    )
    clients.assets.UpdateAsset.return_value = asset_pb2.UpdateAssetResponse(asset=_proto_asset(asset.rid))

    asset.remove_data_scopes(names=["nothing-matches"])

    kept = clients.assets.UpdateAsset.call_args.args[0].data_scopes.data_scopes
    assert not kept[0].HasField("offset")


def test_get_asset_translates_not_found(fake_rpc_error) -> None:
    """A NOT_FOUND status from the asset service surfaces as NominalNotFoundError, not grpc.RpcError."""
    clients = MagicMock()
    client = NominalClient(_clients=clients)
    clients.assets.GetAssets.side_effect = fake_rpc_error(grpc.StatusCode.NOT_FOUND)

    with pytest.raises(NominalNotFoundError):
        client.get_asset("ri.asset.missing")


def test_get_asset_raises_not_found_when_rid_is_absent_from_the_response() -> None:
    """The batch endpoint answers a missing rid with an absent map entry, not NOT_FOUND -- same error either way."""
    clients = MagicMock()
    client = NominalClient(_clients=clients)
    clients.assets.GetAssets.return_value = asset_pb2.GetAssetsResponse()

    with pytest.raises(NominalNotFoundError, match="no asset found with RID"):
        client.get_asset("ri.asset.missing")


def test_from_proto_maps_absent_optionals_to_none() -> None:
    """Description and created_by are optional on the wire; absent must read as None, not empty string."""
    asset = Asset._from_proto(MagicMock(), _proto_asset())

    assert asset.description is None
    assert asset.created_by_rid is None


def test_from_proto_reads_present_optionals() -> None:
    """A description and created_by that are set on the wire read through unchanged."""
    asset = Asset._from_proto(MagicMock(), _proto_asset(description="d", created_by="ri.user.1"))

    assert asset.description == "d"
    assert asset.created_by_rid == "ri.user.1"


def test_scope_rids_selects_by_oneof_discriminator() -> None:
    """The proto data_source is a oneof, so scope kind comes from which field is set."""
    clients = MagicMock()
    asset = _asset(clients)
    clients.assets.GetAssets.return_value = asset_pb2.GetAssetsResponse(
        responses={
            asset.rid: _proto_asset(
                asset.rid,
                data_scopes=[
                    asset_pb2.DataScope(data_scope_name="ds", data_source=asset_pb2.DataSource(dataset="ri.dataset.1")),
                    asset_pb2.DataScope(data_scope_name="vid", data_source=asset_pb2.DataSource(video="ri.video.1")),
                ],
            )
        }
    )

    assert asset._scope_rids("dataset") == {"ds": "ri.dataset.1"}
    assert asset._scope_rids("video") == {"vid": "ri.video.1"}


def test_list_dataset_scopes_normalizes_to_dataset_scope_records() -> None:
    """Asset hands the shared dataset wrapper transport-neutral records, not proto messages."""
    clients = MagicMock()
    asset = _asset(clients)
    clients.assets.GetAssets.return_value = asset_pb2.GetAssetsResponse(
        responses={
            asset.rid: _proto_asset(
                asset.rid,
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

    scopes = asset._list_dataset_scopes()

    assert [(s.data_scope_name, s.dataset_rid, dict(s.series_tags)) for s in scopes] == [
        ("ds", "ri.dataset.1", {"vehicle": "a"})
    ]


def test_create_proto_links_accepts_every_link_spelling() -> None:
    """The proto link builder mirrors create_links: bare url, (url, title) tuple, and dict forms."""
    links = _create_proto_links(
        ["https://a", ("https://b", "B"), {"url": "https://c", "title": "C"}, {"url": "https://d"}]
    )

    assert [(link.url, link.title) for link in links] == [
        ("https://a", ""),
        ("https://b", "B"),
        ("https://c", "C"),
        ("https://d", ""),
    ]


def test_archive_and_unarchive_address_this_asset() -> None:
    """archive()/unarchive() address exactly this asset."""
    clients = MagicMock()
    asset = _asset(clients)

    asset.archive()
    asset.unarchive()

    assert clients.assets.Archive.call_args.args[0].asset_rid == asset.rid
    assert clients.assets.Unarchive.call_args.args[0].asset_rid == asset.rid
