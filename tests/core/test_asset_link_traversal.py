from unittest.mock import MagicMock

import pytest

from nominal.core import Asset, AssetTypeLink, NominalClient, ResolvedAssetLink, UnresolvedAssetLink
from nominal.protos.asset.v2 import asset_pb2


def test_traverse_forward_across_pages_and_batches() -> None:
    """Preserve resolved metadata and blank unresolved values while paging each distinct batch."""
    clients = MagicMock()
    resolved = asset_pb2.LinkEdge(
        requested_asset_rid="asset-0000",
        linked_asset=asset_pb2.LinkedAsset(rid="target", title="Target asset", primary_key_value="S-1"),
    )
    unresolved = asset_pb2.LinkEdge(requested_asset_rid="asset-0001", unresolved_value="")
    clients.assets.TraverseLinks.side_effect = [
        asset_pb2.TraverseLinksResponse(edges=[resolved], next_page_token="cursor"),
        asset_pb2.TraverseLinksResponse(edges=[unresolved]),
        asset_pb2.TraverseLinksResponse(),
    ]
    assets = [f"asset-{i:04d}" for i in range(1001)]
    asset = Asset._from_proto(clients, asset_pb2.Asset(rid=assets[0]))
    edges = NominalClient(_clients=clients).traverse_asset_links([asset, *assets], "link")

    assert edges == [
        ResolvedAssetLink("asset-0000", "target", "Target asset", "S-1"),
        UnresolvedAssetLink("asset-0001", ""),
    ]
    first, second, third = [call.args[0] for call in clients.assets.TraverseLinks.call_args_list]
    assert list(first.asset_rids) == list(second.asset_rids) == assets[:1000]
    assert list(third.asset_rids) == assets[1000:]
    assert [r.page_token for r in (first, second, third)] == ["", "cursor", ""]
    assert first.link == asset_pb2.LinkRef(link_rid="link", direction=asset_pb2.LINK_DIRECTION_FORWARD)


def test_traverse_inverse_uses_existing_link_and_single_start() -> None:
    """Resolve a saved definition and traverse toward source assets from a single RID."""
    clients = MagicMock()
    clients.assets.TraverseLinks.return_value = asset_pb2.TraverseLinksResponse()
    client = NominalClient(_clients=clients)
    link = AssetTypeLink(target_type_rid="type", source_property="serial", rid="link")
    assert client.traverse_asset_links("target", link, direction="inverse") == []
    request = clients.assets.TraverseLinks.call_args.args[0]
    assert list(request.asset_rids) == ["target"]
    assert request.link == asset_pb2.LinkRef(link_rid="link", direction=asset_pb2.LINK_DIRECTION_INVERSE)


def test_traverse_unknown_edge() -> None:
    """An unsupported edge variant cannot be mistaken for an unresolved blank property."""
    clients = MagicMock()
    clients.assets.TraverseLinks.return_value = asset_pb2.TraverseLinksResponse(
        edges=[asset_pb2.LinkEdge(requested_asset_rid="asset")]
    )
    with pytest.raises(ValueError, match="asset"):
        NominalClient(_clients=clients).traverse_asset_links(["asset"], "link")
