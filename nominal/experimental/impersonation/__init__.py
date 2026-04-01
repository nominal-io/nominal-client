from __future__ import annotations

from nominal.core._clientsbunch import ON_BEHALF_OF_USER_RID_HEADER
from nominal.core.client import NominalClient


def as_user(client: NominalClient, user_rid: str) -> NominalClient:
    """Return an experimental derived client for user impersonation.

    The returned client injects the on-behalf-of header for dataset, asset, run, and workbook creation endpoints.
    """
    header = {ON_BEHALF_OF_USER_RID_HEADER: user_rid}
    clients = client._clients.with_service_request_headers(
        {
            "CatalogService": header,
            "AssetService": header,
            "RunService": header,
            "NotebookService": header,
        }
    )
    return NominalClient(_clients=clients, _profile=client._profile)
