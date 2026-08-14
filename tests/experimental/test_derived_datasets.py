from __future__ import annotations

from unittest.mock import MagicMock, Mock, patch

import pytest
from nominal_api import scout_catalog, scout_compute_api, scout_versioning_api

from nominal.experimental.compute_as_code import (
    commit_and_persist_derived_definition,
    commit_derived_definition,
    create_derived_dataset,
    get_derived_definition,
)
from nominal.experimental.compute_as_code._derived_datasets import _to_conjure_dataset


@pytest.fixture
def client(mock_clients: MagicMock) -> MagicMock:
    """A mock NominalClient whose ``_clients`` is the shared mock_clients fixture."""
    client = MagicMock()
    client._clients = mock_clients
    return client


DATASET_RID = "ri.catalog.ws.dataset.abc"


def _conjure_saved(rid: str) -> scout_compute_api.Dataset:
    return scout_compute_api.Dataset(
        saved=scout_compute_api.SavedDataset(rid=scout_compute_api.StringConstant(literal=rid))
    )


def _commit(commit_id: str, *, working_state: bool = True) -> scout_versioning_api.Commit:
    return scout_versioning_api.Commit(
        committed_at="2026-01-01T00:00:00Z",
        committed_by="ri.authn.ws.user.1",
        id=commit_id,
        is_working_state=working_state,
        message=f"commit {commit_id}",
        resource_rid=DATASET_RID,
    )


def _stub_commit_response(client: MagicMock, commit: scout_versioning_api.Commit) -> scout_catalog.DerivedDefinition:
    """Make the catalog's commit call return a definition carrying `commit`."""
    definition = scout_catalog.DerivedDefinition(commit=commit, spec=_conjure_saved(DATASET_RID))
    client._clients.catalog.commit_derived_definition.return_value = definition
    return definition


def _persisted_ids(client: MagicMock) -> list[str]:
    """The commit IDs the client passed to persist_commits."""
    _, request = client._clients.versioning.persist_commits.call_args[0]
    assert [entry.resource_rid for entry in request] == [DATASET_RID] * len(request)
    return [entry.commit_id for entry in request]


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
        "nominal.experimental.compute_as_code._derived_datasets.Dataset._from_conjure",
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
    with pytest.warns(DeprecationWarning):
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


# --- commit persistence ---


def _commit_spec() -> object:
    nc = pytest.importorskip("nominal_compute")
    return nc.Dataset.Saved(DATASET_RID)


def test_commit_and_persist_persists_working_state_commit(client: MagicMock) -> None:
    """The new commit is persisted, and the definition returned unchanged."""
    definition = _stub_commit_response(client, _commit("ri.commit.new"))

    result = commit_and_persist_derived_definition(client, DATASET_RID, _commit_spec(), message="update")

    assert result is definition
    auth, _ = client._clients.versioning.persist_commits.call_args[0]
    assert auth == "Bearer test-token"
    assert _persisted_ids(client) == ["ri.commit.new"]


def test_commit_and_persist_persists_against_the_resolved_rid(client: MagicMock) -> None:
    """A Dataset instance resolves to its RID, which is what the commit is persisted against."""
    dataset = MagicMock()
    dataset.rid = DATASET_RID
    _stub_commit_response(client, _commit("ri.commit.new"))

    commit_and_persist_derived_definition(client, dataset, _commit_spec(), message="update")

    # _persisted_ids asserts the resource_rid of every entry.
    assert _persisted_ids(client) == ["ri.commit.new"]


def test_commit_and_persist_skips_persist_when_commit_already_permanent(client: MagicMock) -> None:
    """A commit that is already permanent is not persisted again."""
    _stub_commit_response(client, _commit("ri.commit.new", working_state=False))

    commit_and_persist_derived_definition(client, DATASET_RID, _commit_spec(), message="update")

    client._clients.versioning.persist_commits.assert_not_called()


def test_commit_and_persist_skips_persist_when_opted_out(client: MagicMock) -> None:
    """persist=False commits without the extra versioning request."""
    _stub_commit_response(client, _commit("ri.commit.new"))

    commit_and_persist_derived_definition(client, DATASET_RID, _commit_spec(), message="update", persist=False)

    client._clients.catalog.commit_derived_definition.assert_called_once()
    client._clients.versioning.persist_commits.assert_not_called()


def test_commit_and_persist_propagates_a_failed_persist(client: MagicMock) -> None:
    """A failed persist surfaces to the caller, with the commit already landed."""
    _stub_commit_response(client, _commit("ri.commit.new"))
    client._clients.versioning.persist_commits.side_effect = RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        commit_and_persist_derived_definition(client, DATASET_RID, _commit_spec(), message="update")

    client._clients.catalog.commit_derived_definition.assert_called_once()


def test_commit_derived_definition_is_deprecated_and_does_not_persist(client: MagicMock) -> None:
    """The old function warns, and delegates with persistence off, so it makes no versioning request."""
    _stub_commit_response(client, _commit("ri.commit.new"))

    with pytest.warns(DeprecationWarning, match="commit_and_persist_derived_definition"):
        commit_derived_definition(client, DATASET_RID, _commit_spec(), message="update")

    client._clients.versioning.persist_commits.assert_not_called()
