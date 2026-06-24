from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING

from nominal_api import scout_catalog

from nominal.core._utils.api_tools import rid_from_instance_or_string
from nominal.core.client import NominalClient
from nominal.core.dataset import Dataset
from nominal.experimental.compute.derived_datasets._compute_bridge import to_conjure_dataset

if TYPE_CHECKING:
    import nominal_compute


def create_derived_dataset(
    client: NominalClient,
    name: str,
    spec: nominal_compute.Dataset,
    *,
    message: str = "Initial derived definition",
    description: str | None = None,
    labels: Sequence[str] = (),
    properties: Mapping[str, str] | None = None,
) -> Dataset:
    """Create a derived dataset defined by a ``nominal_compute`` graph rather than ingested files.

    A derived dataset is a regular catalog dataset whose contents are computed from a ``nominal_compute``
    graph (``spec``) instead of ingested files. It is returned as a core
    :class:`~nominal.core.dataset.Dataset`, exactly like a normal dataset; the derived-definition lifecycle
    is managed via :func:`get_derived_definition` and :func:`commit_derived_definition`.

    Args:
        client: The NominalClient to use for creating the derived dataset.
        name: Name of the derived dataset to create.
        spec: ``nominal_compute`` graph defining how the dataset's contents are computed.
        message: Commit message for the initial derived definition.
        description: Human readable description of the dataset.
        labels: Text labels to apply to the created dataset.
        properties: Key-value properties to apply to the created dataset.

    Returns:
        Reference to the created derived dataset in Nominal.
    """
    request = scout_catalog.CreateDataset(
        name=name,
        description=description,
        labels=list(labels),
        properties={} if properties is None else dict(properties),
        is_v2_dataset=True,
        metadata={},
        origin_metadata=scout_catalog.DatasetOriginMetadata(),
        workspace=client._clients.resolve_default_workspace_rid(),
        marking_rids=[],
        derived_definition=scout_catalog.CreateDerivedDefinition(spec=to_conjure_dataset(spec), message=message),
    )
    response = client._clients.catalog.create_dataset(client._clients.auth_header, request)
    return Dataset._from_conjure(client._clients, response)


def get_derived_definition(
    client: NominalClient,
    dataset: Dataset | str,
    *,
    commit: str | None = None,
) -> scout_catalog.DerivedDefinition:
    """Fetch a dataset's derived definition (its compute spec plus the commit that produced it).

    Args:
        client: The NominalClient to use for the lookup.
        dataset: The derived dataset, or its RID, to fetch the definition for.
        commit: If provided, fetch the definition at this specific commit rather than the latest.

    Returns:
        The dataset's derived definition: its compute spec and the commit that produced it.
    """
    rid = rid_from_instance_or_string(dataset)
    return client._clients.catalog.get_dataset_derived_definition(client._clients.auth_header, rid, commit)


def commit_derived_definition(
    client: NominalClient,
    dataset: Dataset | str,
    spec: nominal_compute.Dataset,
    *,
    message: str,
    latest_commit: str | None = None,
) -> scout_catalog.DerivedDefinition:
    """Replace a derived dataset's definition by creating a new commit.

    Args:
        client: The NominalClient to use for the commit.
        dataset: The derived dataset, or its RID, whose definition to replace.
        spec: ``nominal_compute`` graph defining the new derived definition.
        message: Commit message describing the change.
        latest_commit: If provided, the dataset's expected current commit, used for optimistic
            concurrency control.

    Returns:
        The newly committed derived definition.
    """
    rid = rid_from_instance_or_string(dataset)
    request = scout_catalog.CommitDerivedDefinitionRequest(
        spec=to_conjure_dataset(spec),
        message=message,
        latest_commit=latest_commit,
    )
    return client._clients.catalog.commit_derived_definition(client._clients.auth_header, rid, request)
