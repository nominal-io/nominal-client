"""Credential ownership and failure handling for the opt-in E2E fixture helper."""

from contextlib import ExitStack
from dataclasses import replace
from unittest.mock import MagicMock

import grpc
import pytest

from nominal.core import NominalClient
from nominal.exceptions import NominalPermissionDeniedError
from nominal.protos.authorization.apikeys.v1 import api_keys_pb2
from tests.e2e import credentials
from tests.e2e.test_workspace_resolution import _client_with_workspace_override


@pytest.fixture
def credential_setup():
    """Provide a real parent client and stub only credential service I/O and fixture registration."""
    parent = NominalClient.create("https://api.example.com/api", "parent-key", workspace_rid="workspace")
    service = MagicMock()
    service.CreateApiKey.return_value = api_keys_pb2.CreateApiKeyResponse(
        api_key=api_keys_pb2.ApiKey(display_name="test", created_by="user", rid="key-rid"),
        api_key_value="child-key",
    )
    parent = replace(parent, _clients=replace(parent._clients, api_keys=service))
    request = MagicMock(spec=pytest.FixtureRequest)
    request.config.getoption.return_value = True
    with ExitStack() as cleanup:
        request.addfinalizer.side_effect = cleanup.callback
        yield parent, service, request, cleanup


def test_default_keeps_original_client(credential_setup) -> None:
    """Without opt-in, the helper preserves the original client and creates no credential."""
    parent, service, request, _ = credential_setup
    request.config.getoption.return_value = False
    assert credentials.with_temporary_api_key(parent, request) is parent
    service.CreateApiKey.assert_not_called()
    request.addfinalizer.assert_not_called()


def test_temporary_client_preserves_configuration_and_revokes(credential_setup) -> None:
    """The child preserves connection settings and its one-day key is revoked with the parent credential."""
    parent, service, request, cleanup = credential_setup
    child = credentials.with_temporary_api_key(parent, request)
    assert child._clients.auth_header == "Bearer child-key"
    assert child._clients.api_base_url == parent._clients.api_base_url
    assert child._clients.workspace_rid == parent._clients.workspace_rid
    assert child._clients._service_config.connect_timeout == parent._clients._service_config.connect_timeout
    assert child._clients._service_config.security == parent._clients._service_config.security
    assert child._clients.header_provider is parent._clients.header_provider
    create_request = service.CreateApiKey.call_args.args[0]
    assert create_request.expires_after_days == 1
    assert create_request.HasField("expires_after_days")
    assert create_request.api_key.display_name.startswith("e2e-")
    service.DeleteApiKey.assert_not_called()
    cleanup.close()
    service.DeleteApiKey.assert_called_once_with(api_keys_pb2.DeleteApiKeyRequest(api_key_rid="key-rid"))


def test_failed_client_setup_still_revokes(credential_setup, monkeypatch) -> None:
    """Construction failure after minting cannot bypass the already-registered revocation."""
    parent, service, request, cleanup = credential_setup
    monkeypatch.setattr(credentials.NominalClient, "from_token", MagicMock(side_effect=RuntimeError("setup failed")))
    with pytest.raises(RuntimeError, match="setup failed"):
        credentials.with_temporary_api_key(parent, request)
    cleanup.close()
    service.DeleteApiKey.assert_called_once_with(api_keys_pb2.DeleteApiKeyRequest(api_key_rid="key-rid"))


def test_creation_failure_does_not_fall_back(credential_setup) -> None:
    """Failed key creation aborts setup without returning a client using the shared credential."""
    parent, service, request, _ = credential_setup
    service.CreateApiKey.side_effect = RuntimeError("creation failed")
    with pytest.raises(RuntimeError, match="creation failed"):
        credentials.with_temporary_api_key(parent, request)
    request.addfinalizer.assert_not_called()


def test_credentials_are_not_printed(credential_setup, capsys) -> None:
    """Creating and revoking a credential produces no token-bearing console output."""
    parent, _, request, cleanup = credential_setup
    child = credentials.with_temporary_api_key(parent, request)
    cleanup.close()
    captured = capsys.readouterr()
    assert captured.out == captured.err == ""
    assert "child-key" not in repr(child)


def test_workspace_override_keeps_temporary_credential(credential_setup) -> None:
    """Changing a workspace pin keeps the active key instead of reloading a profile's parent credential."""
    parent, _, request, _ = credential_setup
    parent = replace(parent, _profile="original-profile")
    child = credentials.with_temporary_api_key(parent, request)
    unpinned = _client_with_workspace_override(child, workspace_rid=None)
    assert unpinned._clients.workspace_rid is None
    assert unpinned._clients.auth_header == "Bearer child-key"
    assert unpinned._clients.api_base_url == child._clients.api_base_url
    assert unpinned._clients._service_config is child._clients._service_config
    assert unpinned._clients._default_workspace is not child._clients._default_workspace


def test_creation_rpc_failure_is_translated(credential_setup, fake_rpc_error) -> None:
    """Rejected key creation raises an SDK error without registering cleanup."""
    parent, service, request, _ = credential_setup
    error = fake_rpc_error(grpc.StatusCode.PERMISSION_DENIED)
    service.CreateApiKey.side_effect = error
    with pytest.raises(NominalPermissionDeniedError) as caught:
        credentials.with_temporary_api_key(parent, request)
    request.addfinalizer.assert_not_called()
    assert caught.value.__cause__ is error


def test_revocation_rpc_failure_is_translated(credential_setup, fake_rpc_error) -> None:
    """Cleanup reports an SDK error while retaining the original RPC failure."""
    parent, service, request, cleanup = credential_setup
    credentials.with_temporary_api_key(parent, request)
    error = fake_rpc_error(grpc.StatusCode.PERMISSION_DENIED)
    service.DeleteApiKey.side_effect = error
    with pytest.raises(NominalPermissionDeniedError) as caught:
        cleanup.close()
    assert caught.value.__cause__ is error
