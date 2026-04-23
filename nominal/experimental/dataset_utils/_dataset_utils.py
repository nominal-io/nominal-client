from collections.abc import Mapping, Sequence
from urllib.parse import urlparse

from nominal_api import scout_catalog

from nominal.core import Dataset, NominalClient, User


def create_dataset_with_uuid(
    client: NominalClient,
    dataset_uuid: str,
    name: str,
    *,
    description: str | None = None,
    labels: Sequence[str] = (),
    properties: Mapping[str, str] | None = None,
) -> Dataset:
    """Create a dataset with a specific UUID.

    This is useful for migrations where the dataset UUID must be controlled by the caller.
    Throws a conflict error if a dataset with the specified UUID already exists.

    This endpoint is not intended for general use. Use `NominalClient.create_dataset` instead
    to create a new dataset with an auto-generated UUID.

    Args:
        client: The NominalClient to use for creating the dataset.
        dataset_uuid: The UUID to assign to the new dataset.
        name: Name of the dataset to create.
        description: Human readable description of the dataset.
        labels: Text labels to apply to the created dataset.
        properties: Key-value properties to apply to the created dataset.

    Returns:
        Reference to the created dataset in Nominal.
    """
    create_dataset_request = scout_catalog.CreateDataset(
        name=name,
        description=description,
        labels=list(labels),
        properties={} if properties is None else dict(properties),
        is_v2_dataset=True,
        metadata={},
        origin_metadata=scout_catalog.DatasetOriginMetadata(),
        workspace=client._clients.resolve_default_workspace_rid(),
        marking_rids=[],
    )
    request = scout_catalog.CreateDatasetWithUuidRequest(
        create_dataset=create_dataset_request,
        uuid=dataset_uuid,
    )
    response = client._clients.catalog.create_dataset_with_uuid(client._clients.auth_header, request)
    return Dataset._from_conjure(client._clients, response)


def get_dataset_owner_rid(dataset: Dataset) -> str:
    """Retrieve the owner RID for a dataset via the role service.

    This helper is experimental because it depends on optional gRPC proto packages
    (`nominal[protos]`) that are not part of the default install surface.

    Args:
        dataset: Dataset to resolve the owner RID for.

    Returns:
        The RID of the user with the dataset owner role.

    Raises:
        ImportError: `nominal[protos]` is required for this lookup.
        ValueError: No owner assignment could be resolved for the dataset.
    """
    owner_rid = _lookup_dataset_owner_rid(
        auth_header=dataset._clients.auth_header,
        api_base_url=dataset._clients._api_base_url,  # type: ignore[attr-defined]
        dataset_rid=dataset.rid,
    )
    if owner_rid is None:
        raise ValueError(f"Could not resolve an owner for dataset {dataset.rid}")
    return owner_rid


def get_dataset_owner(dataset: Dataset) -> User:
    """Retrieve the owner user for a dataset via the role service."""
    owner_rid = get_dataset_owner_rid(dataset)
    return User._from_conjure(
        dataset._clients.authentication.get_user(dataset._clients.auth_header, owner_rid)  # type: ignore[attr-defined]
    )


def _lookup_dataset_owner_rid(*, auth_header: str, api_base_url: str, dataset_rid: str) -> str | None:
    try:
        import grpc  # type: ignore[import-untyped]
        from nominal_api_protos.nominal.authorization.roles.v1 import roles_pb2, roles_pb2_grpc
    except ImportError as ex:
        raise ImportError("nominal[protos] is required to use experimental dataset owner lookup") from ex

    target = _api_base_url_to_grpc_target(api_base_url)
    metadata = (("authorization", auth_header),)
    channel = grpc.secure_channel(target, grpc.ssl_channel_credentials())

    with channel:
        stub = roles_pb2_grpc.RoleServiceStub(channel)  # type: ignore[no-untyped-call]
        response = stub.GetResourceRoles(
            roles_pb2.GetResourceRolesRequest(resource=dataset_rid),
            metadata=metadata,
        )

    for assignment in response.role_assignments:
        if assignment.role != roles_pb2.ROLE_OWNER:
            continue
        user_rid = assignment.user_rid
        if isinstance(user_rid, str) and user_rid.strip():
            return user_rid

    return None


def _api_base_url_to_grpc_target(api_base_url: str) -> str:
    parsed = urlparse(api_base_url)
    if not parsed.netloc:
        raise ValueError(f"Could not derive gRPC target from API base URL: {api_base_url}")
    return parsed.netloc
