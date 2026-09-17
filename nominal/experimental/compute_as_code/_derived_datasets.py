from __future__ import annotations

import json
from typing import Mapping, Sequence

import nominal_compute
from conjure_python_client import ConjureDecoder
from nominal_api import scout_catalog, scout_compute_api

from nominal.core import Marking, NominalClient
from nominal.core._utils.api_tools import rid_from_instance_or_string
from nominal.core.dataset import Dataset
from nominal.experimental.derived_datasets._derived_datasets import (
    DerivedDataset,
    _commit_definition,
    _create_derived_dataset,
    _get_definition,
)


def _to_conjure_dataset(spec: nominal_compute.Dataset) -> scout_compute_api.Dataset:
    """Convert a ``nominal_compute.Dataset`` into the ``scout_compute_api.Dataset`` the catalog API expects."""
    wire_json = spec.to_json()  # type: ignore[attr-defined]
    dataset: scout_compute_api.Dataset = ConjureDecoder().decode(json.loads(wire_json), scout_compute_api.Dataset)
    return dataset


def create_derived_dataset(
    client: NominalClient,
    name: str,
    spec: nominal_compute.Dataset,
    *,
    message: str = "Initial derived definition",
    description: str | None = None,
    labels: Sequence[str] = (),
    properties: Mapping[str, str] | None = None,
    markings: Sequence[Marking | str] | None = None,
) -> DerivedDataset:
    """Create a derived dataset defined by a ``nominal_compute`` graph.

    A derived dataset is a regular dataset whose contents are computed from a
    ``nominal_compute`` graph (``spec``) instead of ingested files. It is returned
    as a :class:`~nominal.experimental.derived_datasets.DerivedDataset`, which is not a `Dataset`:
    look it up with `NominalClient.get_dataset` to read its data or attach it to an asset.

    For the common case of a union of tag-filtered input datasets,
    `nominal.experimental.derived_datasets.create_derived_dataset` expresses the same thing without the
    ``compute`` extra, and its inputs can be edited afterwards.

    Args:
        client: The NominalClient to use for creating the derived dataset.
        name: Name of the derived dataset to create.
        spec: ``nominal_compute`` graph defining how the dataset's contents are computed.
        message: Commit message for the initial derived definition.
        description: Human readable description of the dataset.
        labels: Text labels to apply to the created dataset.
        properties: Key-value properties to apply to the created dataset.
        markings: If present, markings (or marking RIDs) applied to the dataset. Sent as part of
            the creation request rather than applied in a follow-up call.

    Returns:
        Reference to the created derived dataset in Nominal.
    """
    return _create_derived_dataset(
        client._clients,
        name,
        _to_conjure_dataset(spec),
        message=message,
        description=description,
        labels=labels,
        properties=properties,
        markings=markings,
    )


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
    return _get_definition(client._clients, rid_from_instance_or_string(dataset), commit)


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
    return _commit_definition(
        client._clients, rid_from_instance_or_string(dataset), _to_conjure_dataset(spec), message, latest_commit
    )
