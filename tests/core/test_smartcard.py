from __future__ import annotations

import sys
import types
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from nominal.core._utils import _pkcs11_bridge as pkcs11_bridge
from nominal.core._utils import smartcard as smartcard_module
from nominal.core._utils.smartcard import (
    SmartcardError,
    SmartcardSession,
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
    cert_der: bytes = b"\x30\x82\x01\x00",
    raise_on_login: bool = False,
) -> tuple[MagicMock, MagicMock]:
    """Inject a fake PyKCS11 module into sys.modules and return (lib, session) mocks."""
    fake_module = types.ModuleType("PyKCS11")

    fake_module.PyKCS11Error = _FakePyKCS11Error  # type: ignore[attr-defined]
    fake_module.CKA_CLASS = "CKA_CLASS"  # type: ignore[attr-defined]
    fake_module.CKO_CERTIFICATE = "CKO_CERTIFICATE"  # type: ignore[attr-defined]
    fake_module.CKO_PRIVATE_KEY = "CKO_PRIVATE_KEY"  # type: ignore[attr-defined]
    fake_module.CKA_VALUE = "CKA_VALUE"  # type: ignore[attr-defined]
    fake_module.CKA_ID = "CKA_ID"  # type: ignore[attr-defined]
    fake_module.CKM_SHA256_RSA_PKCS = "CKM_SHA256_RSA_PKCS"  # type: ignore[attr-defined]
    fake_module.CKM_SHA384_RSA_PKCS = "CKM_SHA384_RSA_PKCS"  # type: ignore[attr-defined]
    fake_module.CKM_SHA512_RSA_PKCS = "CKM_SHA512_RSA_PKCS"  # type: ignore[attr-defined]
    fake_module.CKM_SHA1_RSA_PKCS = "CKM_SHA1_RSA_PKCS"  # type: ignore[attr-defined]

    class _Mechanism:
        def __init__(self, mech, _params=None):
            self.mech = mech

    fake_module.Mechanism = _Mechanism  # type: ignore[attr-defined]

    cert_object = object()
    key_object = object()

    session = MagicMock()
    if raise_on_login:
        session.login.side_effect = _FakePyKCS11Error("PIN incorrect")
    session.findObjects.side_effect = lambda template: (
        [cert_object] if any(p[1] == "CKO_CERTIFICATE" for p in template) else [key_object]
    )
    session.getAttributeValue.return_value = (list(cert_der), [0xAA])

    token_info = MagicMock()
    token_info.label = "TestToken"

    lib = MagicMock()
    lib.getSlotList.return_value = [1]
    lib.getTokenInfo.return_value = token_info
    lib.openSession.return_value = session

    fake_module.PyKCS11Lib = MagicMock(return_value=lib)  # type: ignore[attr-defined]

    monkeypatch.setitem(sys.modules, "PyKCS11", fake_module)
    return lib, session


def test_smartcard_session_login_failure_raises_smartcard_error(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """If C_Login fails (wrong PIN), surface a SmartcardError, not the underlying PyKCS11 type."""
    module_path = tmp_path / "opensc-pkcs11.so"
    module_path.write_bytes(b"")

    _install_fake_pykcs11(monkeypatch, raise_on_login=True)

    with patch("nominal.core._utils.smartcard._prompt_pin", return_value="wrong"):
        with pytest.raises(SmartcardError, match="login failed"):
            SmartcardSession.get(module_path=str(module_path))


def test_smartcard_session_prompts_pin_only_once(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    """SmartcardSession is a singleton — the PIN prompt happens exactly once for repeated get() calls."""
    module_path = tmp_path / "opensc-pkcs11.so"
    module_path.write_bytes(b"")

    _, session = _install_fake_pykcs11(monkeypatch)

    with patch("nominal.core._utils.smartcard._prompt_pin", return_value="1234") as prompt:
        first = SmartcardSession.get(module_path=str(module_path))
        second = SmartcardSession.get(module_path=str(module_path))

    assert first is second
    prompt.assert_called_once()
    session.login.assert_called_once_with("1234")


def test_smartcard_session_finds_no_token_raises(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    """If the reader has no token inserted, surface a clear error rather than hanging or KeyError-ing."""
    module_path = tmp_path / "opensc-pkcs11.so"
    module_path.write_bytes(b"")

    lib, _ = _install_fake_pykcs11(monkeypatch)
    lib.getSlotList.return_value = []

    with patch("nominal.core._utils.smartcard._prompt_pin", return_value="1234"):
        with pytest.raises(SmartcardError, match="No smartcard tokens detected"):
            SmartcardSession.get(module_path=str(module_path))


def _stub_pkcs11_bridge(
    monkeypatch: pytest.MonkeyPatch,
    *,
    install_pkcs11_key_side_effect: Any = None,
) -> dict[str, Any]:
    """Replace the bridge FFI surface with MagicMocks and wire a fake `OpenSSL` module.

    The smartcard session never invokes the FFI in unit tests; we just need to assert that the
    orchestration calls the bridge in the right order with the right values.
    """
    handle = MagicMock(name="LibHandle")
    if install_pkcs11_key_side_effect is None:
        install_pkcs11_key_side_effect = lambda _h, _cb, _c: "<pkey_cdata>"  # noqa: E731

    fake_bridge = types.SimpleNamespace(
        PKCS11BridgeError=pkcs11_bridge.PKCS11BridgeError,
        lib_handle=MagicMock(return_value=handle),
        install_pkcs11_key=MagicMock(side_effect=install_pkcs11_key_side_effect),
        cast_ssl_ctx=MagicMock(return_value="<ssl_ctx_cdata>"),
        install_on_ssl_context=MagicMock(),
        reset_for_test=MagicMock(),
    )
    monkeypatch.setattr(smartcard_module, "_pkcs11_bridge", fake_bridge)

    fake_context = MagicMock(name="Context")
    fake_ssl = MagicMock(name="SSL")
    fake_ssl.Context.return_value = fake_context
    fake_ssl.TLS_CLIENT_METHOD = object()
    fake_ssl.VERIFY_PEER = 1
    fake_ssl.VERIFY_FAIL_IF_NO_PEER_CERT = 2

    fake_crypto = MagicMock(name="crypto")
    fake_crypto.FILETYPE_ASN1 = 1
    fake_crypto.load_certificate.return_value = "<pyopenssl_cert>"

    monkeypatch.setitem(
        sys.modules, "OpenSSL", types.SimpleNamespace(SSL=fake_ssl, crypto=fake_crypto)
    )
    monkeypatch.setitem(sys.modules, "OpenSSL.SSL", fake_ssl)
    monkeypatch.setitem(sys.modules, "OpenSSL.crypto", fake_crypto)

    return {
        "bridge": fake_bridge,
        "handle": handle,
        "ssl": fake_ssl,
        "crypto": fake_crypto,
        "context": fake_context,
    }


def test_ssl_context_orchestrates_bridge_calls(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Building the SSL context must: get a lib handle, install the PKCS#11-backed EVP_PKEY via the
    bridge, parse the cert DER, create an SSL.Context, install cert+pkey onto its SSL_CTX cdata, and
    configure server-cert verification. Second access reuses the cached context.
    """
    module_path = tmp_path / "opensc-pkcs11.so"
    module_path.write_bytes(b"")

    _install_fake_pykcs11(monkeypatch, cert_der=b"<cert-der-bytes>")
    mocks = _stub_pkcs11_bridge(monkeypatch)

    with patch("nominal.core._utils.smartcard._prompt_pin", return_value="1234"):
        sc = SmartcardSession.get(module_path=str(module_path))
        ctx = sc.ssl_context
        # Cached on second access — no extra bridge work.
        assert sc.ssl_context is ctx

    mocks["bridge"].lib_handle.assert_called_once()
    mocks["bridge"].install_pkcs11_key.assert_called_once()
    args, _ = mocks["bridge"].install_pkcs11_key.call_args
    assert args[0] is mocks["handle"]
    # 3rd arg is the cert DER from the smartcard.
    assert args[2] == b"<cert-der-bytes>"
    # 2nd arg is the sign callable — invoking it should route to the SmartcardSession's _sign.
    sign_cb = args[1]
    assert callable(sign_cb)

    mocks["bridge"].cast_ssl_ctx.assert_called_once_with(mocks["context"])
    mocks["bridge"].install_on_ssl_context.assert_called_once_with(
        mocks["handle"], "<ssl_ctx_cdata>", "<pyopenssl_cert>", "<pkey_cdata>"
    )
    mocks["context"].set_default_verify_paths.assert_called_once()
    mocks["context"].set_verify.assert_called_once()


def test_ssl_context_wraps_bridge_errors_as_smartcard_errors(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Bridge-layer errors must surface as SmartcardError with the token label in the message."""
    module_path = tmp_path / "opensc-pkcs11.so"
    module_path.write_bytes(b"")

    _install_fake_pykcs11(monkeypatch)

    def _boom(*_args: Any) -> Any:
        raise pkcs11_bridge.PKCS11BridgeError("SSL_CTX_use_PrivateKey failed")

    _stub_pkcs11_bridge(monkeypatch, install_pkcs11_key_side_effect=_boom)

    with patch("nominal.core._utils.smartcard._prompt_pin", return_value="1234"):
        sc = SmartcardSession.get(module_path=str(module_path))
        with pytest.raises(SmartcardError, match="token 'TestToken'.*SSL_CTX_use_PrivateKey"):
            _ = sc.ssl_context


def test_smartcard_session_signs_via_pkcs11_session(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Once logged in, signing operations delegate to the cached PKCS#11 session — no re-prompt."""
    module_path = tmp_path / "opensc-pkcs11.so"
    module_path.write_bytes(b"")

    _, session = _install_fake_pykcs11(monkeypatch)
    session.sign.return_value = [0x01, 0x02, 0x03]

    with patch("nominal.core._utils.smartcard._prompt_pin", return_value="1234"):
        sc = SmartcardSession.get(module_path=str(module_path))

    sig = sc._sign(b"to-sign", mechanism="CKM_SHA256_RSA_PKCS")

    assert sig == bytes([0x01, 0x02, 0x03])
    session.sign.assert_called_once()
