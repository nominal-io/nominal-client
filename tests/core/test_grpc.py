from __future__ import annotations

import json
from unittest.mock import MagicMock

import grpc
import pytest
from conjure_python_client import ServiceConfiguration

from nominal.core._utils import grpc_tools
from nominal.core._utils.grpc_tools import (
    _AuthMetadataInterceptor,
    _ClientCallDetails,
    _DefaultDeadlineInterceptor,
    _grpc_root_certificates,
    _service_config_json,
    api_base_url_to_grpc_target,
    create_grpc_channel,
    translate_grpc_errors,
)
from nominal.core._utils.networking import StaticHeaderProvider
from nominal.core.exceptions import (
    HeaderConflictError,
    NominalAuthenticationError,
    NominalError,
    NominalInvalidArgumentError,
    NominalNotFoundError,
    NominalPermissionDeniedError,
)


def _config() -> ServiceConfiguration:
    return ServiceConfiguration(uris=["https://api.gov.nominal.io/api"])


def _details(timeout: float | None = None, metadata: list[tuple[str, str]] | None = None) -> _ClientCallDetails:
    return _ClientCallDetails("/svc/Method", timeout, metadata, None, None, None)


def test_api_base_url_to_grpc_target_strips_scheme_and_api_suffix() -> None:
    """api_base_url_to_grpc_target reduces an API base URL to its host:port target."""
    assert api_base_url_to_grpc_target("https://api.gov.nominal.io/api") == "api.gov.nominal.io"
    assert api_base_url_to_grpc_target("https://api.gov.nominal.io") == "api.gov.nominal.io"
    assert api_base_url_to_grpc_target("http://localhost:8080/api") == "localhost:8080"


def test_api_base_url_to_grpc_target_rejects_url_without_netloc() -> None:
    """api_base_url_to_grpc_target rejects a URL it cannot derive a host from."""
    with pytest.raises(ValueError, match="Could not derive gRPC target"):
        api_base_url_to_grpc_target("not-a-url")


def test_root_certificates_on_macos_use_the_trust_store_only(tmp_path, monkeypatch) -> None:
    """On macOS the bundle is the trust-store PEM alone (the keychain is not auto-read)."""
    _grpc_root_certificates.cache_clear()
    monkeypatch.setattr(grpc_tools.sys, "platform", "darwin")
    pem = tmp_path / "certifi.pem"
    pem.write_bytes(b"CERTIFI-ROOTS")
    assert _grpc_root_certificates(str(pem)) == b"CERTIFI-ROOTS"


def test_root_certificates_return_none_when_no_roots_are_found(monkeypatch) -> None:
    """With no trust store and no OS roots, the bundle is None so gRPC falls back to its built-in defaults."""
    _grpc_root_certificates.cache_clear()
    monkeypatch.setattr(grpc_tools.sys, "platform", "darwin")
    assert _grpc_root_certificates(None) is None


def test_root_certificates_on_windows_union_the_system_store(tmp_path, monkeypatch) -> None:
    """On Windows the bundle unions the trust-store PEM with the ROOT/CA system certs."""
    _grpc_root_certificates.cache_clear()
    monkeypatch.setattr(grpc_tools.sys, "platform", "win32")
    ctx = MagicMock()
    ctx.get_ca_certs.return_value = [b"der-bytes"]
    monkeypatch.setattr(grpc_tools.ssl, "SSLContext", lambda proto: ctx)
    monkeypatch.setattr(grpc_tools.ssl, "DER_cert_to_PEM_cert", lambda der: "PEMIFIED\n")
    pem = tmp_path / "certifi.pem"
    pem.write_bytes(b"CERTIFI-ROOTS")
    result = _grpc_root_certificates(str(pem))
    assert b"CERTIFI-ROOTS" in result
    assert b"PEMIFIED" in result


def test_root_certificates_on_linux_union_the_default_cafile(tmp_path, monkeypatch) -> None:
    """On Linux the bundle unions the trust-store PEM with the OS default CA bundle file."""
    _grpc_root_certificates.cache_clear()
    monkeypatch.setattr(grpc_tools.sys, "platform", "linux")
    cafile = tmp_path / "system-ca.pem"
    cafile.write_bytes(b"SYSTEM-CA")
    monkeypatch.setattr(grpc_tools.ssl, "get_default_verify_paths", lambda: MagicMock(cafile=str(cafile)))
    pem = tmp_path / "certifi.pem"
    pem.write_bytes(b"CERTIFI-ROOTS")
    result = _grpc_root_certificates(str(pem))
    assert b"CERTIFI-ROOTS" in result
    assert b"SYSTEM-CA" in result


def test_service_config_json_applies_conjure_retry_to_every_method() -> None:
    """The retry service-config applies conjure-derived retry uniformly to all methods on the channel."""
    config = json.loads(_service_config_json(_config()))["methodConfig"][0]
    assert config["name"] == [{}]  # every method
    policy = config["retryPolicy"]
    assert policy["maxAttempts"] == 5
    assert policy["initialBackoff"] == "0.25s"
    assert policy["maxBackoff"] == "120s"
    assert policy["retryableStatusCodes"] == ["UNAVAILABLE", "RESOURCE_EXHAUSTED"]


def test_auth_interceptor_injects_the_bearer_authorization_header() -> None:
    """The auth interceptor adds the bearer authorization header to the call metadata."""
    continuation = MagicMock()
    _AuthMetadataInterceptor("Bearer tok", None).intercept_unary_unary(continuation, _details(), object())
    assert ("authorization", "Bearer tok") in continuation.call_args.args[0].metadata


def test_auth_interceptor_lowercases_header_provider_keys() -> None:
    """The auth interceptor injects HeaderProvider headers with lowercased metadata keys."""
    provider = StaticHeaderProvider({"X-Nominal-On-Behalf-Of-User": "ri.authn.user.target"})
    continuation = MagicMock()
    _AuthMetadataInterceptor("Bearer tok", provider).intercept_unary_unary(continuation, _details(), object())
    metadata = dict(continuation.call_args.args[0].metadata)
    assert metadata["x-nominal-on-behalf-of-user"] == "ri.authn.user.target"


def test_auth_interceptor_raises_when_a_header_provider_header_conflicts() -> None:
    """A HeaderProvider header colliding with an already-set header raises HeaderConflictError."""
    provider = StaticHeaderProvider({"authorization": "Bearer other"})
    with pytest.raises(HeaderConflictError):
        _AuthMetadataInterceptor("Bearer tok", provider).intercept_unary_unary(MagicMock(), _details(), object())


def test_auth_interceptor_raises_when_the_caller_already_set_authorization() -> None:
    """The auth header is conflict-checked too: caller-supplied authorization metadata raises."""
    with pytest.raises(HeaderConflictError):
        _AuthMetadataInterceptor("Bearer tok", None).intercept_unary_unary(
            MagicMock(), _details(metadata=[("authorization", "Bearer caller")]), object()
        )


def test_deadline_interceptor_injects_the_default_when_the_caller_omits_one() -> None:
    """The deadline interceptor applies the default timeout when the call has none."""
    continuation = MagicMock()
    _DefaultDeadlineInterceptor(300.0).intercept_unary_unary(continuation, _details(timeout=None), object())
    assert continuation.call_args.args[0].timeout == 300.0


def test_deadline_interceptor_preserves_a_caller_supplied_timeout() -> None:
    """The deadline interceptor leaves a caller-supplied timeout untouched."""
    continuation = MagicMock()
    _DefaultDeadlineInterceptor(300.0).intercept_unary_unary(continuation, _details(timeout=5.0), object())
    assert continuation.call_args.args[0].timeout == 5.0


def _patch_channel(monkeypatch) -> tuple[MagicMock, MagicMock, MagicMock]:
    secure_channel = MagicMock(return_value="raw-channel")
    intercept_channel = MagicMock(return_value="intercepted-channel")
    ssl_channel_credentials = MagicMock(return_value="creds")
    monkeypatch.setattr(grpc_tools.grpc, "secure_channel", secure_channel)
    monkeypatch.setattr(grpc_tools.grpc, "intercept_channel", intercept_channel)
    monkeypatch.setattr(grpc_tools.grpc, "ssl_channel_credentials", ssl_channel_credentials)
    monkeypatch.setattr(grpc_tools, "_grpc_root_certificates", lambda path: b"BUNDLE")
    return secure_channel, intercept_channel, ssl_channel_credentials


def test_create_grpc_channel_wires_credentials_options_and_interceptors(monkeypatch) -> None:
    """create_grpc_channel builds a gzip secure channel with union-bundle TLS and retry, plus interceptors."""
    secure_channel, intercept_channel, ssl_channel_credentials = _patch_channel(monkeypatch)

    channel = create_grpc_channel(
        api_base_url="https://api.gov.nominal.io/api",
        service_config=_config(),
        user_agent="test-agent",
        auth_header="Bearer tok",
        header_provider=None,
    )

    assert channel == "intercepted-channel"
    assert secure_channel.call_args.args[0] == "api.gov.nominal.io"
    assert secure_channel.call_args.kwargs["compression"] == grpc_tools.grpc.Compression.Gzip
    assert ssl_channel_credentials.call_args.kwargs["root_certificates"] == b"BUNDLE"
    options = dict(secure_channel.call_args.kwargs["options"])
    assert options["grpc.primary_user_agent"] == "test-agent"
    assert options["grpc.enable_retries"] == 1
    assert "retryPolicy" in options["grpc.service_config"]
    assert options["grpc.max_send_message_length"] == 2**31 - 1
    assert options["grpc.max_receive_message_length"] == 2**31 - 1
    assert intercept_channel.call_args.args[0] == "raw-channel"
    assert len(intercept_channel.call_args.args[1:]) == 2


@pytest.mark.parametrize(
    "code, expected",
    [
        (grpc.StatusCode.PERMISSION_DENIED, NominalPermissionDeniedError),
        (grpc.StatusCode.UNAUTHENTICATED, NominalAuthenticationError),
        (grpc.StatusCode.NOT_FOUND, NominalNotFoundError),
        (grpc.StatusCode.INVALID_ARGUMENT, NominalInvalidArgumentError),
    ],
)
def test_translate_grpc_errors_maps_known_status_codes(code, expected, fake_rpc_error) -> None:
    """Each mapped status code raises its dedicated NominalError subclass, chained to the RpcError."""
    with pytest.raises(expected) as exc_info:
        with translate_grpc_errors():
            raise fake_rpc_error(code)
    assert isinstance(exc_info.value.__cause__, grpc.RpcError)
    assert "fake rpc error" in str(exc_info.value)


def test_translate_grpc_errors_falls_back_to_base_for_unmapped_code(fake_rpc_error) -> None:
    """An unmapped status code raises the base NominalError, not a subclass."""
    with pytest.raises(NominalError) as exc_info:
        with translate_grpc_errors():
            raise fake_rpc_error(grpc.StatusCode.INTERNAL)
    assert type(exc_info.value) is NominalError


def test_translate_grpc_errors_passes_through_on_success() -> None:
    """A block that does not raise is unaffected."""
    with translate_grpc_errors():
        value = 5
    assert value == 5
