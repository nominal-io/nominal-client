from unittest.mock import MagicMock

import pytest

from nominal.core import AssetType, AssetTypeProperty, NominalClient
from nominal.core._utils.query_tools import ArchiveStatusFilter
from nominal.exceptions import NominalNotFoundError
from nominal.protos.asset.v2 import asset_pb2
from nominal.protos.types import types_pb2


def test_type_round_trip_and_update_presence() -> None:
    """Definition snapshots preserve property rules and update omission differs from clearing."""
    clients = MagicMock()
    raw = asset_pb2.Type(
        rid="type-1",
        name="Pump",
        primary_key="serial",
        property_configs={
            "power": asset_pb2.PropertyConfig(requirement=asset_pb2.REQUIRED, value_type=asset_pb2.NUMBER),
        },
    )
    kind = AssetType._from_proto(clients, raw)
    assert kind.properties == {"power": AssetTypeProperty(value_type="number", required=True)}
    clients.assets.UpdateType.return_value = asset_pb2.UpdateTypeResponse(type=raw)
    assert kind.update(name="New") is kind
    request = clients.assets.UpdateType.call_args.args[0]
    assert not request.HasField("property_configs")
    assert not request.HasField("primary_key")
    kind.update(properties={})
    request = clients.assets.UpdateType.call_args.args[0]
    assert request.HasField("property_configs")
    assert not request.property_configs.property_configs


def test_create_asset_type_uses_workspace_and_separate_identity_key() -> None:
    """Creation keeps the identity key separate from other property definitions."""
    clients = MagicMock()
    clients.resolve_default_workspace_rid.return_value = "workspace-1"
    clients.assets.CreateType.return_value = asset_pb2.CreateTypeResponse(type=asset_pb2.Type(rid="type-1"))
    NominalClient(_clients=clients).create_asset_type(
        "Pump",
        primary_key="serial",
        properties={
            "power": AssetTypeProperty(value_type="number", required=True),
        },
    )
    request = clients.assets.CreateType.call_args.args[0]
    assert request.workspace == "workspace-1"
    assert request.primary_key == "serial"
    assert set(request.property_configs) == {"power"}
    assert request.property_configs["power"].value_type == asset_pb2.NUMBER


def test_search_types_paginates_and_scopes_query() -> None:
    """Type searches retain workspace and archive filters across result pages."""
    clients = MagicMock()
    clients.resolve_workspace.return_value.rid = "workspace-1"
    clients.assets.SearchTypes.side_effect = [
        asset_pb2.SearchTypesResponse(results=[asset_pb2.Type(rid="a")], next_page_token="next"),
        asset_pb2.SearchTypesResponse(results=[asset_pb2.Type(rid="b")]),
    ]
    result = NominalClient(_clients=clients).search_asset_types(
        "Pump", workspace="workspace-1", archive_status=ArchiveStatusFilter.ANY
    )
    assert [t.rid for t in result] == ["a", "b"]
    first, second = [call.args[0] for call in clients.assets.SearchTypes.call_args_list]
    assert second.next_page_token == "next"
    for request in (first, second):
        assert request.query.WhichOneof("query") == "and"
        assert list(getattr(request.query, "and").queries) == [
            asset_pb2.SearchTypesQuery(search_text="Pump"),
            asset_pb2.SearchTypesQuery(workspace="workspace-1"),
        ]
        assert set(request.archived_statuses.archived_statuses) == {
            types_pb2.ARCHIVED,
            types_pb2.NOT_ARCHIVED,
        }


def test_missing_type_raises_sdk_not_found() -> None:
    """An omitted batch-lookup result becomes a meaningful single-resource lookup failure."""
    clients = MagicMock()
    clients.assets.GetTypes.return_value = asset_pb2.GetTypesResponse()
    with pytest.raises(NominalNotFoundError):
        NominalClient(_clients=clients).get_asset_type("missing")


def test_definition_refresh_uses_authoritative_response() -> None:
    """Refresh replaces metadata and property rules on the same resource instance."""
    clients = MagicMock()
    kind = AssetType._from_proto(clients, asset_pb2.Type(rid="type-1", name="Old"))
    clients.assets.GetTypes.return_value = asset_pb2.GetTypesResponse(
        responses={
            "type-1": asset_pb2.Type(rid="type-1", name="New", primary_key="serial", is_archived=True),
        }
    )
    assert kind.refresh() is kind
    assert kind.name == "New"
    assert kind.primary_key == "serial"
    assert kind.is_archived


@pytest.mark.parametrize(("value_type", "required"), [("string", False), ("number", True)])
def test_property_rules_round_trip(value_type, required) -> None:
    """Both property types and required/suggested rules survive transport conversion."""
    rule = AssetTypeProperty(value_type=value_type, required=required)
    assert AssetTypeProperty._from_proto(rule._to_proto()) == rule


def test_unknown_property_rule_is_not_silently_weakened() -> None:
    """Unknown server rules fail explicitly rather than becoming optional string rules."""
    with pytest.raises(ValueError, match="value type"):
        AssetTypeProperty._from_proto(asset_pb2.PropertyConfig(value_type=99, requirement=asset_pb2.REQUIRED))
    with pytest.raises(ValueError, match="requirement"):
        AssetTypeProperty._from_proto(asset_pb2.PropertyConfig(value_type=asset_pb2.STRING, requirement=99))
