from __future__ import annotations

from unittest.mock import MagicMock

import grpc
import pytest

from nominal.core.dataset import Dataset, DatasetBounds
from nominal.core.exceptions import NominalPermissionDeniedError
from nominal.core.user import User
from nominal.experimental.dataset_utils import get_dataset_owner, get_dataset_owner_rid
from nominal.protos.authorization.roles.v1 import roles_pb2


@pytest.fixture
def mock_dataset() -> Dataset:
    clients = MagicMock()
    clients.auth_header = "Bearer test-token"
    return Dataset(
        rid="test-rid",
        name="Test Dataset",
        description="A dataset for testing",
        bounds=DatasetBounds(start=123455, end=123456),
        properties={},
        labels=[],
        _clients=clients,
    )


def _role_assignment(role: int, user_rid: str) -> MagicMock:
    assignment = MagicMock()
    assignment.role = role
    assignment.user_rid = user_rid
    return assignment


def _set_role_assignments(dataset: Dataset, assignments: list[MagicMock]) -> None:
    response = MagicMock()
    response.role_assignments = assignments
    dataset._clients.roles.GetResourceRoles.return_value = response  # type: ignore[attr-defined]


def test_get_dataset_owner_rid_returns_rid_of_the_owner_assignment(mock_dataset: Dataset) -> None:
    """get_dataset_owner_rid returns the user RID carrying the ROLE_OWNER assignment."""
    _set_role_assignments(mock_dataset, [_role_assignment(roles_pb2.ROLE_OWNER, "ri.authn.user.owner")])

    assert get_dataset_owner_rid(mock_dataset) == "ri.authn.user.owner"


def test_get_dataset_owner_rid_queries_roles_for_the_dataset_rid(mock_dataset: Dataset) -> None:
    """get_dataset_owner_rid resolves roles for the dataset's own RID."""
    _set_role_assignments(mock_dataset, [_role_assignment(roles_pb2.ROLE_OWNER, "ri.authn.user.owner")])

    get_dataset_owner_rid(mock_dataset)

    request = mock_dataset._clients.roles.GetResourceRoles.call_args.args[0]  # type: ignore[attr-defined]
    assert request.resource == "test-rid"


def test_get_dataset_owner_rid_raises_when_no_assignment_is_an_owner(mock_dataset: Dataset) -> None:
    """get_dataset_owner_rid raises when the dataset has roles but none is ROLE_OWNER."""
    _set_role_assignments(mock_dataset, [_role_assignment(roles_pb2.ROLE_UNSPECIFIED, "ri.authn.user.viewer")])

    with pytest.raises(ValueError, match="Could not resolve an owner for dataset"):
        get_dataset_owner_rid(mock_dataset)


def test_get_dataset_owner_returns_the_user_for_the_resolved_owner(mock_dataset: Dataset) -> None:
    """get_dataset_owner resolves the owner RID and returns the user fetched for it."""
    _set_role_assignments(mock_dataset, [_role_assignment(roles_pb2.ROLE_OWNER, "ri.authn.user.owner")])
    mock_dataset._clients.authentication.get_user.return_value = User(  # type: ignore[attr-defined]
        rid="ri.authn.user.owner",
        display_name="Owner User",
        email="owner@nominal.io",
    )

    owner = get_dataset_owner(mock_dataset)

    assert owner.rid == "ri.authn.user.owner"
    mock_dataset._clients.authentication.get_user.assert_called_once_with(  # type: ignore[attr-defined]
        mock_dataset._clients.auth_header, "ri.authn.user.owner"
    )


def test_get_dataset_owner_rid_translates_grpc_errors(mock_dataset: Dataset, fake_rpc_error) -> None:
    """A grpc.RpcError from the roles stub surfaces as a NominalError subclass, not raw grpc."""
    mock_dataset._clients.roles.GetResourceRoles.side_effect = fake_rpc_error(  # type: ignore[attr-defined]
        grpc.StatusCode.PERMISSION_DENIED
    )

    with pytest.raises(NominalPermissionDeniedError):
        get_dataset_owner_rid(mock_dataset)
