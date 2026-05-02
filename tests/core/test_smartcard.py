from __future__ import annotations

import sys
import types
from unittest.mock import MagicMock, patch

import pytest

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


def test_smartcard_session_ssl_context_raises_until_tls_bridge_lands(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Until the libp11/RSA_METHOD TLS bridge is wired, accessing `ssl_context` must fail with a clear,
    actionable message that names the integration point (so users / reviewers see exactly what's missing
    rather than a confusing low-level OpenSSL error).
    """
    module_path = tmp_path / "opensc-pkcs11.so"
    module_path.write_bytes(b"")

    _install_fake_pykcs11(monkeypatch)

    with patch("nominal.core._utils.smartcard._prompt_pin", return_value="1234"):
        sc = SmartcardSession.get(module_path=str(module_path))

    with pytest.raises(SmartcardError, match="TLS bridge has not been wired"):
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
