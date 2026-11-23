import json
from unittest.mock import MagicMock

import pytest
import requests
from conjure_python_client import ServiceConfiguration

from nominal.exceptions import NominalAuthenticationError, NominalConflictError
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


@pytest.mark.parametrize(
    ("status", "code", "error"),
    [(409, "aborted", NominalConflictError), (401, "unauthenticated", NominalAuthenticationError)],
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
