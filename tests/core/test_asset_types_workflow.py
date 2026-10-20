from unittest.mock import MagicMock

import pytest

from nominal.core import Asset, AssetType, NominalClient, WorkspaceSearchType
from nominal.exceptions import NominalNotFoundError
from nominal.protos.asset.v2 import asset_pb2
from nominal.protos.types import types_pb2


def test_asset_memberships_survive_conversion_and_update() -> None:
    """Membership snapshots refresh in place, with omission distinct from clearing."""
    clients = MagicMock()
    raw = asset_pb2.Asset(rid="asset-1", types=["type-1"])
    asset = Asset._from_proto(clients, raw)
    assert asset.type_rids == ("type-1",)
    clients.assets.UpdateAsset.return_value = asset_pb2.UpdateAssetResponse(asset=raw)
    assert asset.update(name="renamed") is asset
    assert not clients.assets.UpdateAsset.call_args.args[0].HasField("types")
    clients.assets.UpdateAsset.return_value = asset_pb2.UpdateAssetResponse(asset=asset_pb2.Asset(rid="asset-1"))
    asset.update(types=[])
    assert clients.assets.UpdateAsset.call_args.args[0].HasField("types")
    assert not clients.assets.UpdateAsset.call_args.args[0].types.types
    assert asset.type_rids == ()


def test_create_asset_accepts_type_objects_and_numeric_properties() -> None:
    """Typed asset creation sends memberships and numeric values without string coercion."""
    clients = MagicMock()
    clients.resolve_default_workspace_rid.return_value = "workspace-1"
    kind = AssetType._from_proto(clients, asset_pb2.Type(rid="type-1"))
    clients.assets.CreateAsset.return_value = asset_pb2.CreateAssetResponse(
        asset=asset_pb2.Asset(rid="asset-1", types=["type-1"])
    )
    asset = NominalClient(_clients=clients).create_asset(
        "Pump", types=[kind], properties={"serial": "p-1", "power": 12.5}
    )
    request = clients.assets.CreateAsset.call_args.args[0]
    assert list(request.types) == ["type-1"]
    assert request.typed_properties["power"].numeric_value == 12.5
    assert asset.type_rids == ("type-1",)


@pytest.mark.parametrize("types", [None, [], ["a", "b"]])
def test_asset_search_type_filter(types) -> None:
    """An omitted type filter matches all; an explicit list selects any matching member type."""
    clients = MagicMock()
    clients.assets.SearchAssets.return_value = asset_pb2.SearchAssetsResponse()
    NominalClient(_clients=clients).search_assets(types=types, workspace=WorkspaceSearchType.ALL)
    query = clients.assets.SearchAssets.call_args.args[0].query
    filters = [q.asset_types for q in getattr(query, "and").queries if q.WhichOneof("query") == "asset_types"]
    assert len(filters) == (0 if types is None else 1)
    if types is not None:
        assert list(filters[0].asset_types) == types


@pytest.mark.parametrize(
    ("policy", "expected"), [("return", asset_pb2.RETURN_EXISTING), ("update", asset_pb2.UPDATE_EXISTING)]
)
def test_identity_upsert_policies_and_memberships(policy, expected) -> None:
    """Upsert retains the identity type alongside extra types and returns typed server metadata."""
    clients = MagicMock()
    clients.resolve_default_workspace_rid.return_value = "workspace-1"
    raw = asset_pb2.Asset(
        rid="asset-1",
        types=["identity", "other"],
        typed_properties={"power": types_pb2.TypedPropertyValue(numeric_value=12.5)},
    )
    clients.assets.CreateOrUpdateAssetByPrimaryKey.return_value = asset_pb2.CreateOrUpdateAssetByPrimaryKeyResponse(
        asset=raw
    )
    asset = NominalClient(_clients=clients).create_or_update_asset_by_primary_key(
        "identity",
        name="Pump",
        properties={"serial": "p-1", "power": 12.5},
        types=["other", "identity"],
        if_exists=policy,
    )
    request = clients.assets.CreateOrUpdateAssetByPrimaryKey.call_args.args[0]
    assert request.if_exists == expected
    assert list(request.asset.types) == ["identity", "other"]
    assert request.asset.workspace == "workspace-1"
    assert request.asset.typed_properties["power"].numeric_value == 12.5
    assert asset.properties["power"] == 12.5
    assert asset.type_rids == ("identity", "other")


def test_get_types_preserves_membership_order_and_detects_missing() -> None:
    """Batch retrieval orders definitions by membership and reports inaccessible types."""
    clients = MagicMock()
    asset = Asset._from_proto(clients, asset_pb2.Asset(rid="a", types=["b", "a"]))
    clients.assets.GetTypes.return_value = asset_pb2.GetTypesResponse(
        responses={
            "a": asset_pb2.Type(rid="a"),
            "b": asset_pb2.Type(rid="b"),
        }
    )
    assert [kind.rid for kind in asset.get_types()] == ["b", "a"]
    clients.assets.GetTypes.return_value = asset_pb2.GetTypesResponse(responses={"a": asset_pb2.Type(rid="a")})
    with pytest.raises(NominalNotFoundError, match="b"):
        asset.get_types()


def test_invalid_upsert_policy_does_not_issue_request() -> None:
    """Reject unsupported policies before any network calls or writes."""
    clients = MagicMock()
    with pytest.raises(ValueError, match="policy"):
        NominalClient(_clients=clients).create_or_update_asset_by_primary_key(
            "type-1",
            name="Pump",
            properties={"serial": "p-1"},
            if_exists="invalid",
        )
    clients.assets.CreateOrUpdateAssetByPrimaryKey.assert_not_called()
    clients.resolve_default_workspace_rid.assert_not_called()
