from __future__ import annotations

from enum import Enum

from nominal_api import scout_checks_api, scout_notebook_api, scout_template_api

from nominal.core._clientsbunch import ClientsBunch
from nominal.protos.metadata.v2 import resource_metadata_pb2 as metadata_pb2


class MetadataResourceType(Enum):
    """Resource families supported by label and property discovery.

    Assets, runs, datasets, events, and videos discover metadata on active indexed documents.
    Dedicated routes search permitted workspaces and include draft and archived workbooks,
    unpublished and archived workbook templates, and retained metadata for archived or deleted
    checklists.
    """

    ASSET = "ASSET"
    RUN = "RUN"
    DATASET = "DATASET"
    EVENT = "EVENT"
    VIDEO = "VIDEO"
    WORKBOOK = "WORKBOOK"
    WORKBOOK_TEMPLATE = "WORKBOOK_TEMPLATE"
    CHECKLIST = "CHECKLIST"


def _search_params(
    resource_type: MetadataResourceType, workspace_rid: str | None, token: str | None
) -> metadata_pb2.MetadataSearchParams:
    return metadata_pb2.MetadataSearchParams(
        resource_types=[metadata_pb2.ResourceType.Value(resource_type.name)],
        workspaces=[] if workspace_rid is None else [workspace_rid],
        sort_by=metadata_pb2.ALPHABETICAL,
        page_token=token,
    )


def _get_dedicated_metadata(
    clients: ClientsBunch, resource_type: MetadataResourceType, workspace_rid: str | None
) -> (
    scout_notebook_api.GetAllLabelsAndPropertiesResponse
    | scout_template_api.GetAllLabelsAndPropertiesResponse
    | scout_checks_api.GetAllLabelsAndPropertiesResponse
    | None
):
    """Fetch aggregate metadata for dedicated services; return None for indexed resources."""
    workspaces = [] if workspace_rid is None else [workspace_rid]
    match resource_type:
        case MetadataResourceType.WORKBOOK:
            return clients.notebook.get_all_labels_and_properties(clients.auth_header, workspaces=workspaces)
        case MetadataResourceType.WORKBOOK_TEMPLATE:
            return clients.template.get_all_labels_and_properties(clients.auth_header, workspaces=workspaces)
        case MetadataResourceType.CHECKLIST:
            return clients.checklist.get_all_labels_and_properties(clients.auth_header, workspaces=workspaces)
        case _:
            return None
