from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from nominal.smartcard import _pkcs11
from nominal.smartcard._errors import SmartcardConfigurationError
from nominal.smartcard._pkcs11 import (
    NOMINAL_PKCS11_MODULE_ENV_VAR,
    PyKCS11Backend,
    _build_pkcs11_uri,
    discover_pkcs11_module,
)
from tests.smartcard._helpers import FAKE_DER


def _make_mock_pykcs11_env():
    """Return (mock_PyKCS11_module, mock_session)."""
    PyKCS11 = MagicMock()
    PyKCS11.CKF_SERIAL_SESSION = 4
    PyKCS11.CKA_CLASS = 0
    PyKCS11.CKO_CERTIFICATE = 1
    PyKCS11.CKA_CERTIFICATE_TYPE = 0x80
    PyKCS11.CKC_X_509 = 0
    PyKCS11.CKA_LABEL = 3
    PyKCS11.CKA_ID = 0x102
    PyKCS11.CKA_VALUE = 0x11

    token_info = MagicMock()
    token_info.label = "CAC TOKEN     "

    cert_obj = MagicMock()
    session = MagicMock()
    session.findObjects.return_value = [cert_obj]
    session.getAttributeValue.return_value = ["PIV Authentication", b"\x01", FAKE_DER]

    lib = MagicMock()
    lib.getSlotList.return_value = [0]
    lib.getTokenInfo.return_value = token_info
    lib.openSession.return_value = session

    PyKCS11.PyKCS11Lib.return_value = lib
    PyKCS11.PyKCS11Error = type("PyKCS11Error", (Exception,), {"value": 0})

    return PyKCS11, session


# discover_pkcs11_module


def test_discover_pkcs11_module_uses_env_override(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    module_path = tmp_path / "opensc-pkcs11.so"
    module_path.write_text("")
    monkeypatch.setenv(NOMINAL_PKCS11_MODULE_ENV_VAR, str(module_path))

    assert discover_pkcs11_module() == module_path


def test_discover_pkcs11_module_explicit_path_takes_priority_over_env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    env_path = tmp_path / "env.so"
    env_path.write_text("")
    explicit_path = tmp_path / "explicit.so"
    explicit_path.write_text("")
    monkeypatch.setenv(NOMINAL_PKCS11_MODULE_ENV_VAR, str(env_path))

    assert discover_pkcs11_module(explicit_path) == explicit_path


def test_discover_pkcs11_module_rejects_missing_explicit_path(tmp_path: Path) -> None:
    with pytest.raises(SmartcardConfigurationError, match="does not exist"):
        discover_pkcs11_module(tmp_path / "missing.so")


def test_discover_pkcs11_module_rejects_missing_env_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(NOMINAL_PKCS11_MODULE_ENV_VAR, str(tmp_path / "missing.so"))
    with pytest.raises(SmartcardConfigurationError, match="does not exist"):
        discover_pkcs11_module()


def test_discover_pkcs11_module_falls_back_to_platform_paths(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    fake_so = tmp_path / "opensc-pkcs11.so"
    fake_so.write_text("")
    monkeypatch.delenv(NOMINAL_PKCS11_MODULE_ENV_VAR, raising=False)
    monkeypatch.setattr(_pkcs11, "_platform_default_paths", lambda: (str(fake_so),))
    assert discover_pkcs11_module() == fake_so


def test_discover_pkcs11_module_raises_when_no_platform_path_exists(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(NOMINAL_PKCS11_MODULE_ENV_VAR, raising=False)
    monkeypatch.setattr(_pkcs11, "_platform_default_paths", lambda: ())
    with pytest.raises(SmartcardConfigurationError, match="OpenSC"):
        discover_pkcs11_module()


# _build_pkcs11_uri


def test_build_pkcs11_uri_single_byte() -> None:
    assert _build_pkcs11_uri("MY TOKEN", b"\x01") == "pkcs11:token=MY%20TOKEN;id=%01"


def test_build_pkcs11_uri_multi_byte() -> None:
    assert _build_pkcs11_uri("CAC", b"\x0a\xff") == "pkcs11:token=CAC;id=%0a%ff"


def test_build_pkcs11_uri_empty_id() -> None:
    assert _build_pkcs11_uri("CAC", b"") == "pkcs11:token=CAC;id="


# PyKCS11Backend


def test_pykcs11_backend_list_certificate_candidates(tmp_path: Path) -> None:
    mock_pykcs11, _ = _make_mock_pykcs11_env()
    module_path = tmp_path / "opensc-pkcs11.so"
    module_path.write_text("")

    with patch.dict("sys.modules", {"PyKCS11": mock_pykcs11}):
        backend = PyKCS11Backend(module_path)
        candidates = backend.list_certificate_candidates()

    assert len(candidates) == 1
    c = candidates[0]
    assert c.label == "PIV Authentication"
    assert c.slot == "9A"
    assert c.pkcs11_uri == "pkcs11:token=CAC%20TOKEN;id=%01"
    assert c.der_certificate == FAKE_DER


def test_pykcs11_backend_close_closes_sessions(tmp_path: Path) -> None:
    mock_pykcs11, mock_session = _make_mock_pykcs11_env()
    module_path = tmp_path / "opensc-pkcs11.so"
    module_path.write_text("")

    with patch.dict("sys.modules", {"PyKCS11": mock_pykcs11}):
        backend = PyKCS11Backend(module_path)
        backend.list_certificate_candidates()
        backend.close()

    mock_session.closeSession.assert_called_once()


def test_pykcs11_backend_skips_slots_that_fail_to_open(tmp_path: Path) -> None:
    mock_pykcs11, _ = _make_mock_pykcs11_env()
    lib = mock_pykcs11.PyKCS11Lib.return_value
    lib.getSlotList.return_value = [0, 1]
    good_session = lib.openSession.return_value
    lib.openSession.side_effect = [good_session, mock_pykcs11.PyKCS11Error("no token")]

    module_path = tmp_path / "opensc-pkcs11.so"
    module_path.write_text("")

    with patch.dict("sys.modules", {"PyKCS11": mock_pykcs11}):
        backend = PyKCS11Backend(module_path)
        candidates = backend.list_certificate_candidates()

    assert len(candidates) == 1
