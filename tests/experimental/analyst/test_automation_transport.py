import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import requests
from conjure_python_client import ServiceConfiguration
from conjure_python_client._http.configuration import SslConfiguration

from nominal.exceptions import NominalAlreadyExistsError, NominalAuthenticationError, NominalConflictError
from nominal.experimental.analyst._transport import _AutomationTransport
from nominal.protos.ai.v1 import analyst_automation_pb2 as api


def response(status: int, content: bytes) -> requests.Response:
    result = requests.Response()
    result.status_code = status
    result._content = content
    result._content_consumed = True
    return result


def test_exchange_cached_and_refreshes_before_expiry(monkeypatch: pytest.MonkeyPatch) -> None:
    """User keys exchange once per token lifetime and never appear in automation requests."""
    session = MagicMock()
    transport = _AutomationTransport(
        "https://example.com/api", "Bearer nominal_api_key_test", ServiceConfiguration(), "test", None
    )
    monkeypatch.setattr(transport, "_session", session)
    monkeypatch.setattr("nominal.experimental.analyst._transport.time.time", lambda: 100)
    session.request.side_effect = [
        response(200, b'{"orgRid":"org"}'),
        response(200, b'{"accessToken":"jwt","expiresAtSeconds":200}'),
        response(200, api.GetAutomationSchemaResponse(api_version="v1").SerializeToString()),
        response(200, api.GetAutomationSchemaResponse(api_version="v1").SerializeToString()),
        response(200, b'{"orgRid":"org"}'),
        response(200, b'{"accessToken":"new-jwt","expiresAtSeconds":300}'),
        response(200, b""),
    ]
    for _ in range(2):
        transport.call("GetAutomationSchema", api.GetAutomationSchemaRequest(), api.GetAutomationSchemaResponse)
    assert session.request.call_count == 4
    assert session.request.call_args.kwargs["headers"]["Authorization"] == "Bearer jwt"
    monkeypatch.setattr("nominal.experimental.analyst._transport.time.time", lambda: 175)
    transport.call("GetAutomationSchema", api.GetAutomationSchemaRequest(), api.GetAutomationSchemaResponse)
    assert session.request.call_count == 7
    assert session.request.call_args.kwargs["headers"]["Authorization"] == "Bearer new-jwt"


def test_rejected_session_is_exchanged_on_the_next_call(monkeypatch: pytest.MonkeyPatch) -> None:
    """A revoked session fails the current request and the next explicit call obtains a new session."""
    session = MagicMock()
    transport = _AutomationTransport(
        "https://example.com/api", "Bearer nominal_api_key_test", ServiceConfiguration(), "test", None
    )
    monkeypatch.setattr(transport, "_session", session)
    monkeypatch.setattr("nominal.experimental.analyst._transport.time.time", lambda: 100)
    session.request.side_effect = [
        response(200, b'{"orgRid":"org"}'),
        response(200, b'{"accessToken":"revoked","expiresAtSeconds":600}'),
        response(401, b'{"code":"unauthenticated"}'),
        response(200, b'{"orgRid":"org"}'),
        response(200, b'{"accessToken":"renewed","expiresAtSeconds":600}'),
        response(200, b""),
    ]
    with pytest.raises(NominalAuthenticationError):
        transport.call("CreateAutomation", api.CreateAutomationRequest(), api.CreateAutomationResponse)
    assert session.request.call_count == 3
    transport.call("GetAutomationSchema", api.GetAutomationSchemaRequest(), api.GetAutomationSchemaResponse)
    assert session.request.call_count == 6
    assert session.request.call_args.kwargs["headers"]["Authorization"] == "Bearer renewed"


@pytest.mark.parametrize(
    ("status", "code", "error"),
    [
        (409, "aborted", NominalConflictError),
        (409, "already_exists", NominalAlreadyExistsError),
        (401, "unauthenticated", NominalAuthenticationError),
    ],
)
def test_failure_is_redacted_and_not_replayed(
    status: int, code: str, error: type[Exception], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Mutations are never replayed and server messages cannot expose credentials in SDK exceptions."""
    session = MagicMock()
    session.request.return_value = response(status, json.dumps({"code": code, "msg": "secret-token"}).encode())
    transport = _AutomationTransport("https://example.com/api", "Bearer jwt", ServiceConfiguration(), "test", None)
    monkeypatch.setattr(transport, "_session", session)
    with pytest.raises(error) as exc:
        transport.call("CreateAutomation", api.CreateAutomationRequest(), api.CreateAutomationResponse)
    assert "secret-token" not in str(exc.value)
    assert session.request.call_count == 1
    assert session.request.call_args.kwargs["allow_redirects"] is False


def test_request_keeps_explicit_credentials_and_ca_bundle(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Inspect the request after Requests applies environment CA and netrc settings."""
    netrc = tmp_path / "netrc"
    netrc.write_text("machine example.com login local-user password local-password\n")
    netrc.chmod(0o600)
    monkeypatch.setenv("NETRC", str(netrc))
    monkeypatch.setenv("REQUESTS_CA_BUNDLE", "/environment/roots.pem")
    config = ServiceConfiguration(security=SslConfiguration(trust_store_path="/client/roots.pem"))
    transport = _AutomationTransport("https://example.com/api", "Bearer jwt", config, "test", None)
    send = MagicMock(return_value=response(200, b""))
    monkeypatch.setattr(transport._session, "send", send)
    transport.call("GetAutomationSchema", api.GetAutomationSchemaRequest(), api.GetAutomationSchemaResponse)
    assert send.call_args.args[0].headers["Authorization"] == "Bearer jwt"
    assert send.call_args.kwargs["verify"] == "/client/roots.pem"
