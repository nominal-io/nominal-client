from __future__ import annotations

from typing import Sequence
from unittest.mock import MagicMock, call

import grpc
import pytest
from nominal_api import scout_checks_api, scout_notebook_api, scout_template_api

from nominal import core
from nominal.core.client import NominalClient, WorkspaceSearchType
from nominal.core.workspace import Workspace
from nominal.exceptions import NominalPermissionDeniedError
from nominal.protos.metadata.v2 import resource_metadata_pb2 as metadata_pb2
from nominal.protos.metadata.v2 import resource_metadata_pb2_grpc

WORKSPACE_RID = "ri.workspace.main.workspace.flight-test"


def _client() -> tuple[NominalClient, MagicMock]:
    clients = MagicMock()
    clients.auth_header = "Bearer token"
    clients.resolve_default_workspace_rid.return_value = WORKSPACE_RID
    clients.resource_metadata = MagicMock(spec=resource_metadata_pb2_grpc.ResourceMetadataServiceStub(MagicMock()))
    return NominalClient(_clients=clients), clients


def _assert_search_pages(
    rpc: MagicMock, resource_type: metadata_pb2.ResourceType.ValueType, tokens: Sequence[str]
) -> None:
    assert [request.args[0].params.page_token for request in rpc.call_args_list] == list(tokens)
    for request in rpc.call_args_list:
        params = request.args[0].params
        assert list(params.resource_types) == [resource_type]
        assert list(params.workspaces) == [WORKSPACE_RID]
        assert params.sort_by == metadata_pb2.ALPHABETICAL


def test_labels_include_every_page_beyond_the_aggregate_endpoint_limit() -> None:
    """A label picker must include labels after the old aggregate endpoint's 500-result cap."""
    client, clients = _client()
    labels = [f"flight-{index:03d}" for index in range(510)]
    clients.resource_metadata.SearchLabels.side_effect = [
        metadata_pb2.SearchLabelsResponse(
            labels=[metadata_pb2.LabelWithCount(label=label) for label in labels[:500]],
            next_page_token="remaining-flights",
        ),
        metadata_pb2.SearchLabelsResponse(
            labels=[metadata_pb2.LabelWithCount(label=label) for label in reversed(labels[500:])]
        ),
    ]
    workspace = Workspace(rid=WORKSPACE_RID, id="flight-test", org="test-org")

    assert client.list_labels(core.MetadataResourceType.ASSET, workspace=workspace) == labels

    _assert_search_pages(clients.resource_metadata.SearchLabels, metadata_pb2.ASSET, ["", "remaining-flights"])
    clients.resolve_default_workspace_rid.assert_not_called()
    clients.resolve_workspace.assert_not_called()
    clients.resource_metadata.ListPropertiesAndLabels.assert_not_called()


def test_property_keys_include_numeric_properties_and_later_pages() -> None:
    """Key discovery includes numeric keys even though their values use a different metadata API."""
    client, clients = _client()
    clients.resolve_workspace.return_value = Workspace(rid=WORKSPACE_RID, id="flight-test", org="test-org")
    clients.resource_metadata.SearchPropertyKeys.side_effect = [
        metadata_pb2.SearchPropertyKeysResponse(
            property_keys=[
                metadata_pb2.PropertyKeyWithCount(
                    property_key="serial", string=metadata_pb2.StringPropertySummary(distinct_value_count=2)
                )
            ],
            next_page_token="numeric-keys",
        ),
        metadata_pb2.SearchPropertyKeysResponse(
            property_keys=[
                metadata_pb2.PropertyKeyWithCount(
                    property_key="batch_number", numeric=metadata_pb2.NumericPropertySummary(mean=7.5)
                )
            ]
        ),
    ]

    assert client.list_property_keys(core.MetadataResourceType.RUN, workspace=WORKSPACE_RID) == [
        "batch_number",
        "serial",
    ]

    _assert_search_pages(clients.resource_metadata.SearchPropertyKeys, metadata_pb2.RUN, ["", "numeric-keys"])
    clients.resolve_workspace.assert_called_once_with(WORKSPACE_RID)


def test_property_values_include_every_page_without_coercing_strings() -> None:
    """Property value discovery retains the key and workspace while preserving string values such as '007'."""
    client, clients = _client()
    clients.resource_metadata.SearchPropertyValues.side_effect = [
        metadata_pb2.SearchPropertyValuesResponse(
            property_values=[metadata_pb2.PropertyValueWithCount(property_value="tail-12")],
            next_page_token="more-tails",
        ),
        metadata_pb2.SearchPropertyValuesResponse(
            property_values=[
                metadata_pb2.PropertyValueWithCount(property_value="tail-01"),
                metadata_pb2.PropertyValueWithCount(property_value="007"),
            ]
        ),
    ]

    values = client.list_property_values(core.MetadataResourceType.DATASET, "tail_number")

    assert values == ["007", "tail-01", "tail-12"]
    assert all(type(value) is str for value in values)
    _assert_search_pages(clients.resource_metadata.SearchPropertyValues, metadata_pb2.DATASET, ["", "more-tails"])
    assert all(
        request.args[0].property_key == "tail_number"
        for request in clients.resource_metadata.SearchPropertyValues.call_args_list
    )
    clients.resolve_default_workspace_rid.assert_called_once_with()


@pytest.mark.parametrize(
    ("workspace_kwargs", "expected_workspaces"),
    [
        ({}, [WORKSPACE_RID]),
        ({"workspace": None}, [WORKSPACE_RID]),
        ({"workspace": WorkspaceSearchType.DEFAULT}, [WORKSPACE_RID]),
        ({"workspace": WorkspaceSearchType.ALL}, []),
    ],
    ids=["omitted", "none", "default", "all"],
)
def test_default_workspace_is_scoped_and_all_searches_every_permitted_workspace(
    workspace_kwargs: dict[str, WorkspaceSearchType | None], expected_workspaces: list[str]
) -> None:
    client, clients = _client()
    clients.resource_metadata.SearchLabels.return_value = metadata_pb2.SearchLabelsResponse()

    assert client.list_labels(core.MetadataResourceType.EVENT, **workspace_kwargs) == []

    request = clients.resource_metadata.SearchLabels.call_args.args[0]
    assert list(request.params.workspaces) == expected_workspaces
    if expected_workspaces:
        clients.resolve_default_workspace_rid.assert_called_once_with()
    else:
        clients.resolve_default_workspace_rid.assert_not_called()
    clients.resolve_workspace.assert_not_called()


def test_later_page_failure_raises_nominal_error_instead_of_returning_partial_labels(fake_rpc_error) -> None:
    client, clients = _client()
    error = fake_rpc_error(grpc.StatusCode.PERMISSION_DENIED)
    clients.resource_metadata.SearchLabels.side_effect = [
        metadata_pb2.SearchLabelsResponse(
            labels=[metadata_pb2.LabelWithCount(label="first-page")], next_page_token="forbidden-page"
        ),
        error,
    ]

    with pytest.raises(NominalPermissionDeniedError) as exc:
        client.list_labels(core.MetadataResourceType.VIDEO)

    assert exc.value.__cause__ is error
    _assert_search_pages(clients.resource_metadata.SearchLabels, metadata_pb2.VIDEO, ["", "forbidden-page"])


@pytest.mark.parametrize(
    ("resource_type_name", "service_name", "response_type"),
    [
        ("WORKBOOK", "notebook", scout_notebook_api.GetAllLabelsAndPropertiesResponse),
        ("WORKBOOK_TEMPLATE", "template", scout_template_api.GetAllLabelsAndPropertiesResponse),
        ("CHECKLIST", "checklist", scout_checks_api.GetAllLabelsAndPropertiesResponse),
    ],
)
def test_dedicated_metadata_routes_normalize_picker_options_and_unknown_keys(
    resource_type_name: str,
    service_name: str,
    response_type: type[
        scout_notebook_api.GetAllLabelsAndPropertiesResponse
        | scout_template_api.GetAllLabelsAndPropertiesResponse
        | scout_checks_api.GetAllLabelsAndPropertiesResponse
    ],
) -> None:
    """These resource families use their dedicated aggregate APIs because they are absent from the shared index."""
    client, clients = _client()
    service = getattr(clients, service_name)
    service.get_all_labels_and_properties.return_value = response_type(
        labels=["review", "flight", "review"],
        properties={"tail_number": ["N12", "N01", "N12"], "operator": ["Ada"]},
    )
    resource_type = core.MetadataResourceType[resource_type_name]

    assert client.list_labels(resource_type) == ["flight", "review"]
    assert client.list_property_keys(resource_type, workspace=WorkspaceSearchType.ALL) == ["operator", "tail_number"]
    assert client.list_property_values(resource_type, "tail_number", workspace=WorkspaceSearchType.ALL) == [
        "N01",
        "N12",
    ]
    assert client.list_property_values(resource_type, "missing", workspace=WorkspaceSearchType.ALL) == []

    assert service.get_all_labels_and_properties.call_args_list == [
        call("Bearer token", workspaces=[WORKSPACE_RID]),
        *[call("Bearer token", workspaces=[])] * 3,
    ]
    clients.resource_metadata.SearchLabels.assert_not_called()
    clients.resource_metadata.SearchPropertyKeys.assert_not_called()
    clients.resource_metadata.SearchPropertyValues.assert_not_called()
    clients.resolve_default_workspace_rid.assert_called_once_with()
