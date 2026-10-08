from __future__ import annotations

from enum import Enum
from typing import Sequence

from nominal_api import scout_checks_api, scout_notebook_api, scout_template_api

from nominal.core._clientsbunch import ClientsBunch
from nominal.core._utils.pagination_tools import paginate_grpc
from nominal.protos.metadata.v2 import resource_metadata_pb2 as metadata_pb2


class MetadataResourceType(Enum):
    """Resource families supported by label and property discovery.

    Assets, runs, datasets, events, and videos discover metadata on active indexed documents.
    Workbooks, workbook templates, and checklists use dedicated backend routes and retain those
    routes' archive and publication semantics.
    """

    ASSET = "ASSET"
    RUN = "RUN"
    DATASET = "DATASET"
    EVENT = "EVENT"
    VIDEO = "VIDEO"
    WORKBOOK = "WORKBOOK"
    WORKBOOK_TEMPLATE = "WORKBOOK_TEMPLATE"
    CHECKLIST = "CHECKLIST"


_INDEXED_RESOURCE_TYPES: dict[MetadataResourceType, metadata_pb2.ResourceType.ValueType] = {
    MetadataResourceType.ASSET: metadata_pb2.ASSET,
    MetadataResourceType.RUN: metadata_pb2.RUN,
    MetadataResourceType.DATASET: metadata_pb2.DATASET,
    MetadataResourceType.EVENT: metadata_pb2.EVENT,
    MetadataResourceType.VIDEO: metadata_pb2.VIDEO,
}


def _search_params(
    resource_type: MetadataResourceType, workspace_rid: str | None, token: str | None
) -> metadata_pb2.MetadataSearchParams:
    return metadata_pb2.MetadataSearchParams(
        resource_types=[_INDEXED_RESOURCE_TYPES[resource_type]],
        workspaces=[] if workspace_rid is None else [workspace_rid],
        sort_by=metadata_pb2.ALPHABETICAL,
        page_token=token or "",
    )


def _get_dedicated_metadata(
    clients: ClientsBunch, resource_type: MetadataResourceType, workspace_rid: str | None
) -> (
    scout_notebook_api.GetAllLabelsAndPropertiesResponse
    | scout_template_api.GetAllLabelsAndPropertiesResponse
    | scout_checks_api.GetAllLabelsAndPropertiesResponse
):
    workspaces = [] if workspace_rid is None else [workspace_rid]
    match resource_type:
        case MetadataResourceType.WORKBOOK:
            return clients.notebook.get_all_labels_and_properties(clients.auth_header, workspaces=workspaces)
        case MetadataResourceType.WORKBOOK_TEMPLATE:
            return clients.template.get_all_labels_and_properties(clients.auth_header, workspaces=workspaces)
        case MetadataResourceType.CHECKLIST:
            return clients.checklist.get_all_labels_and_properties(clients.auth_header, workspaces=workspaces)
        case _:
            raise ValueError(f"Unsupported metadata resource type: {resource_type}")


def _list_labels(
    clients: ClientsBunch, resource_type: MetadataResourceType, workspace_rid: str | None
) -> Sequence[str]:
    if resource_type in _INDEXED_RESOURCE_TYPES:
        responses = paginate_grpc(
            clients.resource_metadata.SearchLabels,
            request_factory=lambda token: metadata_pb2.SearchLabelsRequest(
                params=_search_params(resource_type, workspace_rid, token)
            ),
        )
        return sorted({label.label for response in responses for label in response.labels})
    return sorted(set(_get_dedicated_metadata(clients, resource_type, workspace_rid).labels))


def _list_property_keys(
    clients: ClientsBunch, resource_type: MetadataResourceType, workspace_rid: str | None
) -> Sequence[str]:
    if resource_type in _INDEXED_RESOURCE_TYPES:
        responses = paginate_grpc(
            clients.resource_metadata.SearchPropertyKeys,
            request_factory=lambda token: metadata_pb2.SearchPropertyKeysRequest(
                params=_search_params(resource_type, workspace_rid, token)
            ),
        )
        return sorted({key.property_key for response in responses for key in response.property_keys})
    return sorted(_get_dedicated_metadata(clients, resource_type, workspace_rid).properties)


def _list_property_values(
    clients: ClientsBunch, resource_type: MetadataResourceType, property_key: str, workspace_rid: str | None
) -> Sequence[str]:
    if resource_type in _INDEXED_RESOURCE_TYPES:
        responses = paginate_grpc(
            clients.resource_metadata.SearchPropertyValues,
            request_factory=lambda token: metadata_pb2.SearchPropertyValuesRequest(
                params=_search_params(resource_type, workspace_rid, token), property_key=property_key
            ),
        )
        return sorted({value.property_value for response in responses for value in response.property_values})
    response = _get_dedicated_metadata(clients, resource_type, workspace_rid)
    return sorted(set(response.properties.get(property_key, [])))
