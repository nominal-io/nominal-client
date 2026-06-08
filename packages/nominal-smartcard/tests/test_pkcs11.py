from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from _helpers import FAKE_DER

from nominal.smartcard import _pkcs11
from nominal.smartcard._errors import SmartcardConfigurationError
from nominal.smartcard._pkcs11 import (
    NOMINAL_PKCS11_MODULE_ENV_VAR,
    DefaultPkcs11Backend,
    _build_pkcs11_uri,
    _pct_encode_pk11_pchar,
    discover_pkcs11_module,
)


def _make_mock_pkcs11_env() -> tuple[Any, Any]:
    """Return (mock_pkcs11_module, mock_session)."""
    mock_pkcs11 = MagicMock()
    mock_pkcs11.exceptions.PKCS11Error = type("PKCS11Error", (Exception,), {})

    # Build cert object whose __getitem__ routes by attribute identity.
    cert_obj = MagicMock()

    def _getitem(attr):
        if attr is mock_pkcs11.Attribute.LABEL:
            return "PIV Authentication"
        if attr is mock_pkcs11.Attribute.ID:
            return b"\x01"
        if attr is mock_pkcs11.Attribute.VALUE:
            return FAKE_DER
        raise mock_pkcs11.exceptions.PKCS11Error(f"unknown attr: {attr}")

    cert_obj.__getitem__ = MagicMock(side_effect=_getitem)

    mock_session = MagicMock()
    mock_session.get_objects.return_value = [cert_obj]

    session_cm = MagicMock()
    session_cm.__enter__ = MagicMock(return_value=mock_session)
    session_cm.__exit__ = MagicMock(return_value=False)

    mock_token = MagicMock()
    mock_token.label = "CAC TOKEN     "
    mock_token.open.return_value = session_cm

    mock_slot = MagicMock()
    mock_slot.get_token.return_value = mock_token

    mock_lib = MagicMock()
    mock_lib.get_slots.return_value = [mock_slot]

    mock_pkcs11.lib.return_value = mock_lib

    return mock_pkcs11, mock_session


# discover_pkcs11_module


def test_discover_pkcs11_module_uses_env_override(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    module_path = tmp_path / "opensc-pkcs11.so"
    module_path.write_text("")
    monkeypatch.setenv(NOMINAL_PKCS11_MODULE_ENV_VAR, str(module_path))

    assert discover_pkcs11_module() == module_path


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


# _pct_encode_pk11_pchar


def test_pct_encode_pk11_pchar_passes_safe_alphanumeric() -> None:
    assert _pct_encode_pk11_pchar("CAC") == "CAC"


def test_pct_encode_pk11_pchar_encodes_space() -> None:
    assert _pct_encode_pk11_pchar("MY TOKEN") == "MY%20TOKEN"


def test_pct_encode_pk11_pchar_encodes_semicolon() -> None:
    # Semicolon is the path component separator in PKCS#11 URIs and must be encoded
    assert _pct_encode_pk11_pchar(";") == "%3b"


def test_pct_encode_pk11_pchar_encodes_equals() -> None:
    # Equals sign is the name-value separator and must be encoded
    assert _pct_encode_pk11_pchar("=") == "%3d"


def test_pct_encode_pk11_pchar_encodes_unicode() -> None:
    # é is UTF-8 0xc3 0xa9
    assert _pct_encode_pk11_pchar("é") == "%c3%a9"


def test_pct_encode_pk11_pchar_safe_special_chars_pass_through() -> None:
    safe = "-._~:[]@!$&'()*+,"
    assert _pct_encode_pk11_pchar(safe) == safe


# _build_pkcs11_uri


def test_build_pkcs11_uri_single_byte() -> None:
    assert _build_pkcs11_uri("MY TOKEN", b"\x01") == "pkcs11:token=MY%20TOKEN;id=%01"


def test_build_pkcs11_uri_multi_byte() -> None:
    assert _build_pkcs11_uri("CAC", b"\x0a\xff") == "pkcs11:token=CAC;id=%0a%ff"


def test_build_pkcs11_uri_empty_id() -> None:
    assert _build_pkcs11_uri("CAC", b"") == "pkcs11:token=CAC;id="


def test_build_pkcs11_uri_with_object_type() -> None:
    assert _build_pkcs11_uri("CAC", b"\x01", object_type="private") == "pkcs11:token=CAC;id=%01;type=private"


# DefaultPkcs11Backend


def test_default_pkcs11_backend_list_certificate_candidates(tmp_path: Path) -> None:
    mock_pkcs11, _ = _make_mock_pkcs11_env()
    module_path = tmp_path / "opensc-pkcs11.so"
    module_path.write_text("")

    with patch.object(_pkcs11, "pkcs11", mock_pkcs11):
        backend = DefaultPkcs11Backend(module_path)
        candidates = backend.list_certificate_candidates()

    assert len(candidates) == 1
    c = candidates[0]
    assert c.label == "PIV Authentication"
    assert c.slot == "9A"
    assert c.certificate_uri == "pkcs11:token=CAC%20TOKEN;id=%01;type=cert"
    assert c.private_key_uri == "pkcs11:token=CAC%20TOKEN;id=%01;type=private"
    assert c.der_certificate == FAKE_DER


def test_default_pkcs11_backend_session_closed_after_listing_candidates(tmp_path: Path) -> None:
    mock_pkcs11, _ = _make_mock_pkcs11_env()
    module_path = tmp_path / "opensc-pkcs11.so"
    module_path.write_text("")

    lib = mock_pkcs11.lib.return_value
    token = lib.get_slots.return_value[0].get_token.return_value
    session_cm = token.open.return_value

    with patch.object(_pkcs11, "pkcs11", mock_pkcs11):
        backend = DefaultPkcs11Backend(module_path)
        backend.list_certificate_candidates()

    session_cm.__exit__.assert_called_once()


def test_default_pkcs11_backend_session_closed_with_no_certificate_candidates(tmp_path: Path) -> None:
    mock_pkcs11, mock_session = _make_mock_pkcs11_env()
    mock_session.get_objects.return_value = []
    module_path = tmp_path / "opensc-pkcs11.so"
    module_path.write_text("")

    lib = mock_pkcs11.lib.return_value
    token = lib.get_slots.return_value[0].get_token.return_value
    session_cm = token.open.return_value

    with patch.object(_pkcs11, "pkcs11", mock_pkcs11):
        backend = DefaultPkcs11Backend(module_path)
        assert backend.list_certificate_candidates() == []

    session_cm.__exit__.assert_called_once()


def test_default_pkcs11_backend_session_closed_when_certificate_lookup_fails(tmp_path: Path) -> None:
    mock_pkcs11, mock_session = _make_mock_pkcs11_env()
    mock_session.get_objects.side_effect = mock_pkcs11.exceptions.PKCS11Error("lookup failed")
    module_path = tmp_path / "opensc-pkcs11.so"
    module_path.write_text("")

    lib = mock_pkcs11.lib.return_value
    token = lib.get_slots.return_value[0].get_token.return_value
    session_cm = token.open.return_value

    with patch.object(_pkcs11, "pkcs11", mock_pkcs11):
        backend = DefaultPkcs11Backend(module_path)
        assert backend.list_certificate_candidates() == []

    session_cm.__exit__.assert_called_once()


def test_default_pkcs11_backend_skips_slots_that_fail_to_open(tmp_path: Path) -> None:
    mock_pkcs11, _ = _make_mock_pkcs11_env()
    lib = mock_pkcs11.lib.return_value

    good_slot = lib.get_slots.return_value[0]
    bad_slot = MagicMock()
    bad_slot.get_token.side_effect = mock_pkcs11.exceptions.PKCS11Error("no token")
    lib.get_slots.return_value = [good_slot, bad_slot]

    module_path = tmp_path / "opensc-pkcs11.so"
    module_path.write_text("")

    with patch.object(_pkcs11, "pkcs11", mock_pkcs11):
        backend = DefaultPkcs11Backend(module_path)
        candidates = backend.list_certificate_candidates()

    assert len(candidates) == 1


def test_default_pkcs11_backend_close_clears_lib(tmp_path: Path) -> None:
    mock_pkcs11, _ = _make_mock_pkcs11_env()
    module_path = tmp_path / "opensc-pkcs11.so"
    module_path.write_text("")

    with patch.object(_pkcs11, "pkcs11", mock_pkcs11):
        backend = DefaultPkcs11Backend(module_path)
        backend.list_certificate_candidates()  # populates _lib
        assert backend._lib is not None
        backend.close()
        assert backend._lib is None


def test_default_pkcs11_backend_strips_trailing_whitespace_from_token_label(tmp_path: Path) -> None:
    mock_pkcs11, _ = _make_mock_pkcs11_env()
    lib = mock_pkcs11.lib.return_value
    lib.get_slots.return_value[0].get_token.return_value.label = "  PADDED LABEL  "
    module_path = tmp_path / "opensc-pkcs11.so"
    module_path.write_text("")

    with patch.object(_pkcs11, "pkcs11", mock_pkcs11):
        backend = DefaultPkcs11Backend(module_path)
        candidates = backend.list_certificate_candidates()

    assert len(candidates) == 1
    # Token label embedded in URIs must have whitespace stripped
    assert "PADDED%20LABEL" in candidates[0].certificate_uri
    assert "  " not in candidates[0].certificate_uri
