"""Credential ownership and failure handling for the opt-in E2E fixture helper."""

from contextlib import ExitStack, nullcontext
from unittest.mock import MagicMock

import pytest
from nominal_api import authorization

from nominal.core import NominalClient
from tests.e2e import credentials


@pytest.fixture
def credential_setup(monkeypatch):
    """Provide a real parent client and stub only credential service I/O and fixture registration."""
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
    parent = NominalClient.create("https://api.example.com/api", "parent-key", workspace_rid="workspace")
    service = MagicMock(spec=authorization.AuthorizationService)
    service.create_api_key.return_value = authorization.CreateApiKeyResponse(
        api_key_metadata=authorization.ApiKey(
            api_key_name="test", created_at="2026-01-01T00:00:00Z", created_by="user", is_deleted=False, rid="key-rid"
        ),
        api_key_value="child-key",
    )
    monkeypatch.setattr(credentials, "create_conjure_service_client", lambda *args, **kwargs: service)
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
    service.create_api_key.assert_not_called()
    request.addfinalizer.assert_not_called()


def test_temporary_client_preserves_configuration_and_revokes(credential_setup) -> None:
    """The child preserves connection settings and its one-day key is revoked with the parent credential."""
    parent, service, request, cleanup = credential_setup
    child = credentials.with_temporary_api_key(parent, request)
    assert child._clients.auth_header == "Bearer child-key"
    assert child._clients.api_base_url == parent._clients.api_base_url
    assert child._clients.workspace_rid == parent._clients.workspace_rid
    assert child._clients._service_config is parent._clients._service_config
    assert service.create_api_key.call_args.args[1].expires_after_days == 1
    service.revoke_api_key.assert_not_called()
    cleanup.close()
    service.revoke_api_key.assert_called_once_with("Bearer parent-key", "key-rid")


def test_failed_client_setup_still_revokes(credential_setup, monkeypatch) -> None:
    """Construction failure after minting cannot bypass the already-registered revocation."""
    parent, service, request, cleanup = credential_setup
    monkeypatch.setattr(credentials.ClientsBunch, "from_config", MagicMock(side_effect=RuntimeError("setup failed")))
    with pytest.raises(RuntimeError, match="setup failed"):
        credentials.with_temporary_api_key(parent, request)
    cleanup.close()
    service.revoke_api_key.assert_called_once_with("Bearer parent-key", "key-rid")


def test_creation_failure_does_not_fall_back(credential_setup) -> None:
    """Failed key creation aborts setup without returning a client using the shared credential."""
    parent, service, request, _ = credential_setup
    service.create_api_key.side_effect = RuntimeError("creation failed")
    with pytest.raises(RuntimeError, match="creation failed"):
        credentials.with_temporary_api_key(parent, request)
    request.addfinalizer.assert_not_called()


def test_revocation_failure_is_visible(credential_setup) -> None:
    """A failed revocation surfaces as a teardown error instead of silently leaking a key."""
    parent, service, request, cleanup = credential_setup
    credentials.with_temporary_api_key(parent, request)
    service.revoke_api_key.side_effect = RuntimeError("revocation failed")
    with pytest.raises(RuntimeError, match="revocation failed"):
        cleanup.close()


def test_ci_masks_before_client_construction(credential_setup, monkeypatch, capsys) -> None:
    """The mask command bypasses pytest capture before a child client can use the generated key."""
    parent, _, request, _ = credential_setup
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    capture = request.config.pluginmanager.getplugin.return_value
    capture.global_and_fixture_disabled.side_effect = nullcontext
    monkeypatch.setattr(credentials.ClientsBunch, "from_config", MagicMock(side_effect=RuntimeError("stop here")))
    with pytest.raises(RuntimeError, match="stop here"):
        credentials.with_temporary_api_key(parent, request)
    capture.global_and_fixture_disabled.assert_called_once()
    assert capsys.readouterr().err == "::add-mask::child-key\n"
