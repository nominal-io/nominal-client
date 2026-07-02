from collections.abc import Mapping, Sequence

from nominal_api import scout_catalog

from nominal.core import Dataset, NominalClient, User
from nominal.core._utils.grpc_tools import translate_grpc_errors
from nominal.protos.authorization.roles.v1 import roles_pb2


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
    """Retrieve the owner RID for a dataset via the Role Service.

    Args:
        dataset: Dataset to resolve the owner RID for.

    Returns:
        The RID of the user with the dataset owner role.

    Raises:
        ValueError: No owner assignment could be resolved for the dataset.
    """
    with translate_grpc_errors():
        response = dataset._clients.roles.GetResourceRoles(roles_pb2.GetResourceRolesRequest(resource=dataset.rid))
    for assignment in response.role_assignments:
        if assignment.role == roles_pb2.ROLE_OWNER and assignment.user_rid:
            return str(assignment.user_rid)
    raise ValueError(f"Could not resolve an owner for dataset {dataset.rid}")


def get_dataset_owner(dataset: Dataset) -> User:
    """Retrieve the owner user for a dataset via the Role Service."""
    owner_rid = get_dataset_owner_rid(dataset)
    return User._from_conjure(dataset._clients.authentication.get_user(dataset._clients.auth_header, owner_rid))
