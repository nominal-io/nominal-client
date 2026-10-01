from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from nominal.core.client import NominalClient
from nominal.protos.asset.v2 import asset_pb2


@pytest.mark.parametrize(
    ("if_exists", "policy"),
    [("update", asset_pb2.UPDATE_EXISTING), ("return", asset_pb2.RETURN_EXISTING)],
)
def test_create_or_update_asset_by_primary_key(if_exists, policy) -> None:
    """The request carries the type RID, payload, and if_exists policy; the proto response becomes an Asset."""
    clients = MagicMock()
    clients.resolve_default_workspace_rid.return_value = "workspace-rid-1"
    clients.assets_v2.CreateOrUpdateAssetByPrimaryKey.return_value = asset_pb2.CreateOrUpdateAssetByPrimaryKeyResponse(
        asset=asset_pb2.Asset(rid="asset-rid-1", title="Robot 1"), created=True
    )

    asset = NominalClient(_clients=clients).create_or_update_asset_by_primary_key(
        "type-rid-1", name="Robot 1", properties={"serial": "r1"}, if_exists=if_exists
    )

    request = clients.assets_v2.CreateOrUpdateAssetByPrimaryKey.call_args.args[0]
    assert request.type_rid == "type-rid-1"
    assert request.if_exists == policy
    assert list(request.asset.types) == ["type-rid-1"]
    assert dict(request.asset.properties) == {"serial": "r1"}
    assert request.asset.workspace == "workspace-rid-1"
    assert (asset.rid, asset.name, asset.description) == ("asset-rid-1", "Robot 1", None)
