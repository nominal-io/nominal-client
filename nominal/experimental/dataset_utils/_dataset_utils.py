from collections.abc import Mapping, Sequence

from nominal_api import scout_catalog

from nominal.core import Dataset, NominalClient


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
        workspace=client._clients.workspace_rid,
        marking_rids=[],
    )
    request = scout_catalog.CreateDatasetWithUuidRequest(
        create_dataset=create_dataset_request,
        uuid=dataset_uuid,
    )
    response = client._clients.catalog.create_dataset_with_uuid(client._clients.auth_header, request)
    return Dataset._from_conjure(client._clients, response)
