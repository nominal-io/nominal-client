from __future__ import annotations

from unittest.mock import MagicMock, Mock, patch

import pytest
from nominal_api import scout_compute_api

from nominal.experimental.compute import (
    commit_derived_definition,
    create_derived_dataset,
    get_derived_definition,
)
from nominal.experimental.compute._derived_datasets import _to_conjure_dataset


@pytest.fixture
def client(mock_clients: MagicMock) -> MagicMock:
    """A mock NominalClient whose ``_clients`` is the shared mock_clients fixture."""
    client = MagicMock()
    client._clients = mock_clients
    return client


def _conjure_saved(rid: str) -> scout_compute_api.Dataset:
    return scout_compute_api.Dataset(
        saved=scout_compute_api.SavedDataset(rid=scout_compute_api.StringConstant(literal=rid))
    )


# --- bridge: nominal_compute -> scout_compute_api ---


def test_bridge_decodes_saved_dataset() -> None:
    """A saved dataset bridges to the conjure saved-dataset type."""
    nc = pytest.importorskip("nominal_compute")
    bridged = _to_conjure_dataset(nc.Dataset.Saved("ri.catalog.ws.dataset.abc"))
    assert bridged == _conjure_saved("ri.catalog.ws.dataset.abc")


def test_bridge_decodes_dataset_transform() -> None:
    """A dataset transform (time_shift) bridges to the matching conjure type."""
    nc = pytest.importorskip("nominal_compute")
    bridged = _to_conjure_dataset(nc.Dataset.Saved("ri.catalog.ws.dataset.abc").time_shift(nc.Duration.Seconds(5)))
    assert isinstance(bridged, scout_compute_api.Dataset)
    assert bridged.type == "timeShift"


# --- lifecycle functions ---


def test_create_derived_dataset_sets_derived_definition(client: MagicMock) -> None:
    """create_derived_dataset bridges the spec and sets it as the create request's derived definition."""
    nc = pytest.importorskip("nominal_compute")
    spec = nc.Dataset.Saved("ri.catalog.ws.dataset.abc")
    sentinel = object()
    client._clients.resolve_default_workspace_rid.return_value = "ri.workspace.w"
    client._clients.catalog.create_dataset = Mock()

    with patch(
        "nominal.experimental.compute._derived_datasets.Dataset._from_conjure",
        return_value=sentinel,
    ):
        result = create_derived_dataset(client, "deriv", spec, message="init", labels=["a"], properties={"k": "v"})

    assert result is sentinel
    auth, details = client._clients.catalog.create_dataset.call_args[0]
    assert auth == "Bearer test-token"
    # The nominal_compute spec is bridged to the conjure wire type.
    assert details.derived_definition.spec == _conjure_saved("ri.catalog.ws.dataset.abc")
    assert details.derived_definition.message == "init"
    assert details.is_v2_dataset is True
    assert details.workspace == "ri.workspace.w"
    assert details.labels == ["a"]
    assert details.properties == {"k": "v"}


def test_get_derived_definition_forwards_rid_and_commit(client: MagicMock) -> None:
    """get_derived_definition forwards the dataset RID and a null commit to the catalog client."""
    result = get_derived_definition(client, "ri.catalog.ws.dataset.abc")
    assert result is client._clients.catalog.get_dataset_derived_definition.return_value
    assert client._clients.catalog.get_dataset_derived_definition.call_args == (
        ("Bearer test-token", "ri.catalog.ws.dataset.abc", None),
    )


def test_get_derived_definition_accepts_dataset_and_commit(client: MagicMock) -> None:
    """get_derived_definition accepts a Dataset instance and forwards an explicit commit."""
    dataset = MagicMock()
    dataset.rid = "ri.catalog.ws.dataset.abc"
    get_derived_definition(client, dataset, commit="ri.commit.123")
    assert client._clients.catalog.get_dataset_derived_definition.call_args == (
        ("Bearer test-token", "ri.catalog.ws.dataset.abc", "ri.commit.123"),
    )


def test_commit_derived_definition_builds_request(client: MagicMock) -> None:
    """commit_derived_definition builds the request with the bridged spec, message, and latest commit."""
    nc = pytest.importorskip("nominal_compute")
    spec = nc.Dataset.Saved("ri.catalog.ws.dataset.abc").time_shift(nc.Duration.Seconds(5))
    result = commit_derived_definition(
        client, "ri.catalog.ws.dataset.abc", spec, message="update", latest_commit="ri.commit.1"
    )
    assert result is client._clients.catalog.commit_derived_definition.return_value
    auth, rid, request = client._clients.catalog.commit_derived_definition.call_args[0]
    assert auth == "Bearer test-token"
    assert rid == "ri.catalog.ws.dataset.abc"
    assert isinstance(request.spec, scout_compute_api.Dataset)
    assert request.spec.type == "timeShift"
    assert request.message == "update"
    assert request.latest_commit == "ri.commit.1"
