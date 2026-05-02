from __future__ import annotations

import sys
import types
import urllib.parse
from unittest.mock import MagicMock, patch

import pytest

from nominal.core._utils import _openssl_provider as openssl_provider
from nominal.core._utils import smartcard as smartcard_module
from nominal.core._utils.smartcard import (
    SmartcardError,
    SmartcardSession,
    _build_pkcs11_uri,
    _verify_callback,
    discover_pkcs11_module,
)


@pytest.fixture(autouse=True)
def _reset_singleton() -> None:
    SmartcardSession.reset_for_test()
    yield
    SmartcardSession.reset_for_test()


def test_discover_pkcs11_module_honors_env_var(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    """NOMINAL_PKCS11_MODULE is the highest-priority source for the module path."""
    fake_module = tmp_path / "opensc-pkcs11.so"
    fake_module.write_bytes(b"")

    monkeypatch.setenv("NOMINAL_PKCS11_MODULE", str(fake_module))

    assert discover_pkcs11_module() == str(fake_module)


def test_discover_pkcs11_module_rejects_missing_env_path(monkeypatch: pytest.MonkeyPatch) -> None:
    """A NOMINAL_PKCS11_MODULE value pointing to a missing file is a hard error, not silent fallback."""
    monkeypatch.setenv("NOMINAL_PKCS11_MODULE", "/nonexistent/opensc-pkcs11.so")

    with pytest.raises(SmartcardError, match="does not exist"):
        discover_pkcs11_module()


def test_discover_pkcs11_module_falls_back_to_standard_paths(monkeypatch: pytest.MonkeyPatch) -> None:
    """When the env var is unset and no standard path exists, raise with install guidance."""
    monkeypatch.delenv("NOMINAL_PKCS11_MODULE", raising=False)

    with patch("os.path.exists", return_value=False):
        with pytest.raises(SmartcardError, match="No PKCS#11 module found"):
            discover_pkcs11_module()


class _FakePyKCS11Error(Exception):
    pass


def _install_fake_pykcs11(
    monkeypatch: pytest.MonkeyPatch,
    *,
    token_label: str = "TestToken",
    slots=(1,),
    cert_attrs=((b"not-a-real-cert", [0xAA]),),
) -> MagicMock:
    """Inject a minimal fake PyKCS11 used by `_probe_token_label`.

    The provider-based path uses PyKCS11 only for public metadata: token label and certificate CKA_ID.
    """
    fake_module = types.ModuleType("PyKCS11")
    fake_module.PyKCS11Error = _FakePyKCS11Error  # type: ignore[attr-defined]
    fake_module.CKA_CLASS = "CKA_CLASS"  # type: ignore[attr-defined]
    fake_module.CKO_CERTIFICATE = "CKO_CERTIFICATE"  # type: ignore[attr-defined]
    fake_module.CKA_VALUE = "CKA_VALUE"  # type: ignore[attr-defined]
    fake_module.CKA_ID = "CKA_ID"  # type: ignore[attr-defined]

    token_info = MagicMock()
    token_info.label = token_label

    cert_objects = [object() for _ in cert_attrs]
    session = MagicMock()
    session.findObjects.return_value = cert_objects
    session.getAttributeValue.side_effect = list(cert_attrs)

    lib = MagicMock()
    lib.getSlotList.return_value = list(slots)
    lib.getTokenInfo.return_value = token_info
    lib.openSession.return_value = session

    fake_module.PyKCS11Lib = MagicMock(return_value=lib)  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "PyKCS11", fake_module)
    return lib


def test_smartcard_session_no_token_raises(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    """If the reader has no token inserted, fail fast at session construction with a clear error."""
    module_path = tmp_path / "opensc-pkcs11.so"
    module_path.write_bytes(b"")

    _install_fake_pykcs11(monkeypatch, slots=())

    with pytest.raises(SmartcardError, match="No smartcard tokens detected"):
        SmartcardSession.get(module_path=str(module_path))


def test_smartcard_session_construct_does_not_prompt_pin(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Token probe must NOT prompt for a PIN — the prompt only fires when ssl_context is first accessed."""
    module_path = tmp_path / "opensc-pkcs11.so"
    module_path.write_bytes(b"")

    _install_fake_pykcs11(monkeypatch)

    with patch("nominal.core._utils.smartcard._prompt_pin") as prompt:
        sc = SmartcardSession.get(module_path=str(module_path))
        assert sc.token_label == "TestToken"

    prompt.assert_not_called()


def test_build_pkcs11_uri_encodes_module_path_and_pin() -> None:
    """Both module path and PIN must be URL-encoded — module paths can contain spaces, PINs can contain
    reserved characters that would break URI parsing inside the provider.
    """
    uri = _build_pkcs11_uri("/lib path/opensc.so", "p&i=n;1")

    assert uri.startswith("pkcs11:?")
    qs = uri[len("pkcs11:?") :]
    parsed = urllib.parse.parse_qs(qs)
    assert parsed["module-path"] == ["/lib path/opensc.so"]
    assert parsed["pin-value"] == ["p&i=n;1"]


def test_build_pkcs11_uri_escapes_reserved_pin_characters() -> None:
    """Reserved URI chars in the PIN (?, #, &, =) must be percent-encoded; otherwise the provider would
    parse them as URI structure and misinterpret the PIN.
    """
    uri = _build_pkcs11_uri("/lib.so", "?&=#")

    pin_field = [seg for seg in uri.split("&") if seg.startswith("pin-value=")][0]
    assert pin_field == "pin-value=%3F%26%3D%23"


def test_build_pkcs11_uri_scopes_to_certificate_id_when_known() -> None:
    uri = _build_pkcs11_uri("/lib.so", "1234", b"\x01\xaa")

    assert uri.startswith("pkcs11:id=%01%AA?")
    assert "module-path=%2Flib.so" in uri


def test_verify_callback_returns_openssl_preverify_result() -> None:
    assert _verify_callback(None, None, 0, 0, 1) is True
    assert _verify_callback(None, None, 20, 0, 0) is False


def _stub_openssl_provider(
    monkeypatch: pytest.MonkeyPatch,
    *,
    load_cert_and_key_side_effect=None,
    load_provider_side_effect=None,
) -> dict[str, MagicMock]:
    """Replace the openssl_provider FFI surface with MagicMocks.

    Returns the mocks so tests can assert on call args. Also wires a fake `OpenSSL.SSL` so we don't try
    to construct a real Context (which would need a real, well-formed cert + key).
    """
    handle = MagicMock(name="LibHandle")
    fake_provider = types.SimpleNamespace(
        OpenSSLProviderError=openssl_provider.OpenSSLProviderError,
        lib_handle=MagicMock(return_value=handle),
        load_provider=MagicMock(side_effect=load_provider_side_effect),
        load_cert_and_key=MagicMock(side_effect=load_cert_and_key_side_effect),
        install_on_ssl_context=MagicMock(),
        cast_pyopenssl_ssl_ctx=MagicMock(return_value="<ssl_ctx_cdata>"),
        cast_pyopenssl_ssl=MagicMock(return_value="<ssl_cdata>"),
        configure_hostname_verification=MagicMock(),
        assert_verify_ok=MagicMock(),
        validate_pyopenssl_context=MagicMock(),
        reset_for_test=MagicMock(),
    )
    monkeypatch.setattr(smartcard_module, "openssl_provider", fake_provider)

    fake_context = MagicMock(name="Context")
    fake_ssl_module = MagicMock(name="SSL")
    fake_ssl_module.Context.return_value = fake_context
    fake_ssl_module.TLS_CLIENT_METHOD = object()
    fake_ssl_module.VERIFY_PEER = 1
    fake_ssl_module.VERIFY_FAIL_IF_NO_PEER_CERT = 2
    monkeypatch.setitem(sys.modules, "OpenSSL", types.SimpleNamespace(SSL=fake_ssl_module))
    monkeypatch.setitem(sys.modules, "OpenSSL.SSL", fake_ssl_module)

    return {
        "provider": fake_provider,
        "handle": handle,
        "ssl_module": fake_ssl_module,
        "context": fake_context,
    }


def test_ssl_context_load_orchestrates_provider_calls(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Building an SSL context should: load the provider once, prompt once, OSSL_STORE-load by URI, install
    cert+key onto pyOpenSSL's Context, and configure verification.
    """
    module_path = tmp_path / "opensc-pkcs11.so"
    module_path.write_bytes(b"")

    _install_fake_pykcs11(monkeypatch)
    mocks = _stub_openssl_provider(
        monkeypatch,
        load_cert_and_key_side_effect=lambda _handle, _uri: ("<cert>", "<pkey>"),
    )

    with patch("nominal.core._utils.smartcard._prompt_pin", return_value="1234") as prompt:
        sc = SmartcardSession.get(module_path=str(module_path))
        ctx = sc.ssl_context
        # Cached on second access — no second prompt, no second provider load.
        assert sc.ssl_context is ctx

    prompt.assert_called_once()
    mocks["provider"].load_provider.assert_called_once()
    args, _ = mocks["provider"].load_provider.call_args
    assert args[0] is mocks["handle"]
    assert args[1] == "pkcs11"

    # PIN was passed inside the URI to load_cert_and_key.
    mocks["provider"].load_cert_and_key.assert_called_once()
    _, uri_arg = mocks["provider"].load_cert_and_key.call_args.args
    assert "pin-value=1234" in uri_arg
    assert "id=%AA" in uri_arg
    assert urllib.parse.quote(str(module_path), safe="") in uri_arg

    # Cert + key were installed on the SSL_CTX cdata.
    mocks["provider"].install_on_ssl_context.assert_called_once_with(
        mocks["handle"], "<ssl_ctx_cdata>", "<cert>", "<pkey>"
    )
    mocks["context"].set_default_verify_paths.assert_called_once()
    mocks["context"].set_verify.assert_called_once()


def test_ssl_context_wraps_provider_errors_as_smartcard_errors(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Provider-layer errors must surface as SmartcardError with the token label in the message."""
    module_path = tmp_path / "opensc-pkcs11.so"
    module_path.write_bytes(b"")

    _install_fake_pykcs11(monkeypatch)

    def _boom(_handle, _uri):
        raise openssl_provider.OpenSSLProviderError("OSSL_STORE_open failed: pkcs11: bad PIN")

    _stub_openssl_provider(monkeypatch, load_cert_and_key_side_effect=_boom)

    with patch("nominal.core._utils.smartcard._prompt_pin", return_value="bad"):
        sc = SmartcardSession.get(module_path=str(module_path))
        with pytest.raises(SmartcardError, match="token 'TestToken'.*OSSL_STORE_open failed: pkcs11: bad PIN"):
            _ = sc.ssl_context


def test_ssl_context_does_not_swallow_unrelated_errors(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A non-provider exception (e.g. an interrupted PIN prompt) should propagate untouched."""
    module_path = tmp_path / "opensc-pkcs11.so"
    module_path.write_bytes(b"")

    _install_fake_pykcs11(monkeypatch)
    _stub_openssl_provider(monkeypatch)

    with patch("nominal.core._utils.smartcard._prompt_pin", side_effect=KeyboardInterrupt):
        sc = SmartcardSession.get(module_path=str(module_path))
        with pytest.raises(KeyboardInterrupt):
            _ = sc.ssl_context


def test_smartcard_connection_configures_sni_and_hostname_verification(monkeypatch: pytest.MonkeyPatch) -> None:
    mocks = _stub_openssl_provider(monkeypatch)
    ssl_conn = MagicMock(name="SSL.Connection")
    mocks["ssl_module"].Connection.return_value = ssl_conn
    monkeypatch.setattr(
        smartcard_module.SmartcardSession,
        "get",
        MagicMock(return_value=types.SimpleNamespace(ssl_context="<ctx>")),
    )

    conn = smartcard_module.SmartcardHTTPSConnection("api.example.com", port=443)
    conn._new_conn = MagicMock(return_value="<tcp-sock>")  # type: ignore[method-assign]

    conn.connect()

    mocks["ssl_module"].Connection.assert_called_once_with("<ctx>", "<tcp-sock>")
    ssl_conn.set_tlsext_host_name.assert_called_once_with(b"api.example.com")
    mocks["provider"].cast_pyopenssl_ssl.assert_called_once_with(mocks["handle"], ssl_conn)
    mocks["provider"].configure_hostname_verification.assert_called_once_with(
        mocks["handle"], "<ssl_cdata>", "api.example.com"
    )
    mocks["provider"].assert_verify_ok.assert_called_once_with(mocks["handle"], "<ssl_cdata>", "api.example.com")
    assert conn.sock is ssl_conn
