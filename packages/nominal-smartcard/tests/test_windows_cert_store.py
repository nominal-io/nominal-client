from __future__ import annotations

import datetime
import sys
import types
from typing import Any, Iterator

import pytest

from nominal.smartcard._errors import SmartcardConfigurationError
from nominal.smartcard._windows_cert_store import (
    _OID_CLIENT_AUTH,
    _OID_ENHANCED_KEY_USAGE,
    _OID_KEY_USAGE,
    _OID_SMARTCARD_LOGON,
    _has_digital_signature_key_usage,
    _has_eku,
    _is_usable_client_certificate,
    _select_certificate,
    _to_identity,
)
from nominal.smartcard._windows_cng_signer import _OID_RSA

_DOTNET_MODULE_NAMES = (
    "System",
    "System.Security",
    "System.Security.Cryptography",
    "System.Security.Cryptography.X509Certificates",
)


@pytest.fixture(autouse=True)
def _fake_dotnet_key_usage_flags() -> Iterator[None]:
    """Register a fake System...X509KeyUsageFlags so the cert-store .NET import resolves off-Windows."""

    class _X509KeyUsageFlags:
        # Matches the real System.Security.Cryptography.X509Certificates.X509KeyUsageFlags value.
        DigitalSignature = 0x80

    saved = {name: sys.modules.get(name) for name in _DOTNET_MODULE_NAMES}

    system = types.ModuleType("System")
    security = types.ModuleType("System.Security")
    cryptography = types.ModuleType("System.Security.Cryptography")
    x509 = types.ModuleType("System.Security.Cryptography.X509Certificates")
    x509.X509KeyUsageFlags = _X509KeyUsageFlags  # type: ignore[attr-defined]
    system.Security = security  # type: ignore[attr-defined]
    security.Cryptography = cryptography  # type: ignore[attr-defined]
    cryptography.X509Certificates = x509  # type: ignore[attr-defined]

    sys.modules.update(dict(zip(_DOTNET_MODULE_NAMES, (system, security, cryptography, x509))))
    try:
        yield
    finally:
        for name, module in saved.items():
            if module is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = module


# clientAuth / EKU / KeyUsage OIDs are imported from _windows_cert_store and the RSA public-key
# OID (_OID_RSA) from _windows_cng_signer; only OIDs with no source-side definition live here.
_OID_EMAIL_PROTECTION = "1.3.6.1.5.5.7.3.4"

# X509KeyUsageFlags bit values (the .NET public-API encoding the selection logic relies on).
_KU_DIGITAL_SIGNATURE = 0x80
_KU_NON_REPUDIATION = 0x40
_KU_KEY_ENCIPHERMENT = 0x20

_NOW = datetime.datetime(2026, 6, 8, 12, 0, 0)
_DAY_AGO = _NOW - datetime.timedelta(days=1)
_YEAR_AGO = _NOW - datetime.timedelta(days=365)
_DAY_AHEAD = _NOW + datetime.timedelta(days=1)
_YEAR_AHEAD = _NOW + datetime.timedelta(days=365)


# ---------------------------------------------------------------------------
# Fakes mimicking the duck-typed surface the cert store reads off pythonnet objects.
# ---------------------------------------------------------------------------


class _FakeValue:
    def __init__(self, value: str) -> None:
        self.Value = value


class _FakePublicKey:
    def __init__(self, oid_value: str) -> None:
        self.Oid = _FakeValue(oid_value)


class _FakeExtension:
    def __init__(
        self,
        oid_value: str,
        *,
        enhanced_key_usages: list[str] | None = None,
        key_usages: int | None = None,
    ) -> None:
        self.Oid = _FakeValue(oid_value)
        # .NET only exposes EnhancedKeyUsages on the EKU extension and KeyUsages on the
        # KeyUsage extension; leave the attribute off otherwise so getattr(..., None) fires.
        if enhanced_key_usages is not None:
            self.EnhancedKeyUsages = [_FakeValue(v) for v in enhanced_key_usages]
        if key_usages is not None:
            self.KeyUsages = key_usages


def _eku(*oids: str) -> _FakeExtension:
    return _FakeExtension(_OID_ENHANCED_KEY_USAGE, enhanced_key_usages=list(oids))


def _key_usage(flags: int) -> _FakeExtension:
    return _FakeExtension(_OID_KEY_USAGE, key_usages=flags)


class _FakeCert:
    def __init__(
        self,
        *,
        has_private_key: bool = True,
        not_before: datetime.datetime = _DAY_AGO,
        not_after: datetime.datetime = _YEAR_AHEAD,
        extensions: list[_FakeExtension] | None = None,
        thumbprint: str = "AA BB CC DD",
        raw_data: bytes = b"\x30\x82\x01\x00",
        subject: str = "CN=Test User",
        issuer: str = "CN=Test CA",
        public_key_oid: str = _OID_RSA,
    ) -> None:
        self.HasPrivateKey = has_private_key
        self.NotBefore = not_before
        self.NotAfter = not_after
        self.Extensions = extensions if extensions is not None else []
        self.Thumbprint = thumbprint
        self.RawData = raw_data
        self.Subject = subject
        self.Issuer = issuer
        self.PublicKey = _FakePublicKey(public_key_oid)


def _piv_auth_cert(**overrides: Any) -> _FakeCert:
    """A PIV Authentication certificate: clientAuth + Smart Card Logon, digitalSignature."""
    defaults: dict[str, Any] = dict(
        extensions=[_eku(_OID_CLIENT_AUTH, _OID_SMARTCARD_LOGON), _key_usage(_KU_DIGITAL_SIGNATURE)],
        subject="CN=PIV Auth",
    )
    defaults.update(overrides)
    return _FakeCert(**defaults)


def _signature_cert(**overrides: Any) -> _FakeCert:
    """A CAC digital-signature cert that also carries clientAuth (no Smart Card Logon)."""
    defaults: dict[str, Any] = dict(
        extensions=[
            _eku(_OID_CLIENT_AUTH, _OID_EMAIL_PROTECTION),
            _key_usage(_KU_DIGITAL_SIGNATURE | _KU_NON_REPUDIATION),
        ],
        subject="CN=Signature",
    )
    defaults.update(overrides)
    return _FakeCert(**defaults)


def _encryption_cert(**overrides: Any) -> _FakeCert:
    """A CAC key-management cert: clientAuth but keyEncipherment only (no digitalSignature)."""
    defaults: dict[str, Any] = dict(
        extensions=[_eku(_OID_CLIENT_AUTH, _OID_EMAIL_PROTECTION), _key_usage(_KU_KEY_ENCIPHERMENT)],
        subject="CN=Encryption",
    )
    defaults.update(overrides)
    return _FakeCert(**defaults)


# ---------------------------------------------------------------------------
# _has_eku
# ---------------------------------------------------------------------------


def test_has_eku_finds_target_oid() -> None:
    cert = _FakeCert(extensions=[_eku(_OID_CLIENT_AUTH, _OID_SMARTCARD_LOGON)])
    assert _has_eku(cert, _OID_CLIENT_AUTH) is True
    assert _has_eku(cert, _OID_SMARTCARD_LOGON) is True


def test_has_eku_missing_target_oid() -> None:
    cert = _FakeCert(extensions=[_eku(_OID_EMAIL_PROTECTION)])
    assert _has_eku(cert, _OID_CLIENT_AUTH) is False


def test_has_eku_no_extension() -> None:
    assert _has_eku(_FakeCert(extensions=[]), _OID_CLIENT_AUTH) is False


def test_has_eku_extension_without_enhanced_key_usages_is_false() -> None:
    # An EKU extension object that .NET did not upcast (no EnhancedKeyUsages) fails closed.
    cert = _FakeCert(extensions=[_FakeExtension(_OID_ENHANCED_KEY_USAGE)])
    assert _has_eku(cert, _OID_CLIENT_AUTH) is False


# ---------------------------------------------------------------------------
# _has_digital_signature_key_usage
# ---------------------------------------------------------------------------


def test_digital_signature_bit_present() -> None:
    cert = _FakeCert(extensions=[_key_usage(_KU_DIGITAL_SIGNATURE)])
    assert _has_digital_signature_key_usage(cert) is True


def test_digital_signature_with_other_bits_present() -> None:
    cert = _FakeCert(extensions=[_key_usage(_KU_DIGITAL_SIGNATURE | _KU_NON_REPUDIATION)])
    assert _has_digital_signature_key_usage(cert) is True


def test_digital_signature_bit_absent() -> None:
    cert = _FakeCert(extensions=[_key_usage(_KU_KEY_ENCIPHERMENT)])
    assert _has_digital_signature_key_usage(cert) is False


def test_no_key_usage_extension_permits_all_usages() -> None:
    # RFC 5280 §4.2.1.3: absence of KeyUsage means no restriction.
    assert _has_digital_signature_key_usage(_FakeCert(extensions=[])) is True


def test_key_usage_extension_without_key_usages_is_false() -> None:
    cert = _FakeCert(extensions=[_FakeExtension(_OID_KEY_USAGE)])
    assert _has_digital_signature_key_usage(cert) is False


# ---------------------------------------------------------------------------
# _is_usable_client_certificate
# ---------------------------------------------------------------------------


def test_usable_piv_auth_certificate() -> None:
    assert _is_usable_client_certificate(_piv_auth_cert(), _NOW) is True


def test_unusable_without_private_key() -> None:
    assert _is_usable_client_certificate(_piv_auth_cert(has_private_key=False), _NOW) is False


def test_unusable_when_expired() -> None:
    assert _is_usable_client_certificate(_piv_auth_cert(not_after=_DAY_AGO), _NOW) is False


def test_unusable_when_not_yet_valid() -> None:
    assert _is_usable_client_certificate(_piv_auth_cert(not_before=_DAY_AHEAD), _NOW) is False


def test_unusable_without_client_auth_eku() -> None:
    cert = _FakeCert(extensions=[_eku(_OID_EMAIL_PROTECTION), _key_usage(_KU_DIGITAL_SIGNATURE)])
    assert _is_usable_client_certificate(cert, _NOW) is False


def test_unusable_without_digital_signature_key_usage() -> None:
    # The CAC encryption certificate: clientAuth EKU but keyEncipherment-only key usage.
    assert _is_usable_client_certificate(_encryption_cert(), _NOW) is False


def test_attribute_access_error_is_treated_as_unusable() -> None:
    class _ExplodingCert:
        @property
        def HasPrivateKey(self) -> bool:
            raise RuntimeError("simulated COM/CSP failure")

    assert _is_usable_client_certificate(_ExplodingCert(), _NOW) is False


# ---------------------------------------------------------------------------
# _select_certificate
# ---------------------------------------------------------------------------


def test_select_single_usable_certificate() -> None:
    cert = _piv_auth_cert()
    result = _select_certificate([cert], now=_NOW)
    assert result.certificate is cert


def test_select_ignores_unusable_certificates() -> None:
    good = _piv_auth_cert()
    certs = [
        _piv_auth_cert(has_private_key=False),  # no key
        _piv_auth_cert(not_after=_YEAR_AGO),  # expired
        _encryption_cert(),  # no digitalSignature
        good,
    ]
    assert _select_certificate(certs, now=_NOW).certificate is good


def test_select_no_usable_certificate_raises() -> None:
    with pytest.raises(SmartcardConfigurationError, match="No unexpired client-auth certificate"):
        _select_certificate([_encryption_cert(), _piv_auth_cert(not_after=_YEAR_AGO)], now=_NOW)


def test_select_prefers_smartcard_logon_cert_on_real_cac() -> None:
    # The headline CAC scenario: the PIV Authentication cert AND the digital-signature cert
    # both pass the client-auth filter. The Smart Card Logon EKU breaks the tie toward the
    # PIV Authentication certificate — the same identity the PKCS#11 path selects via slot 9A.
    piv = _piv_auth_cert()
    signature = _signature_cert()
    assert _select_certificate([signature, piv], now=_NOW).certificate is piv
    # Order-independent.
    assert _select_certificate([piv, signature], now=_NOW).certificate is piv


def test_select_ambiguous_when_no_smartcard_logon_cert() -> None:
    # Two client-auth certs, neither marked for Smart Card Logon: cannot disambiguate.
    one = _signature_cert(subject="CN=One")
    two = _signature_cert(subject="CN=Two")
    with pytest.raises(SmartcardConfigurationError, match="could not be identified unambiguously"):
        _select_certificate([one, two], now=_NOW)


def test_select_ambiguous_when_multiple_smartcard_logon_certs() -> None:
    with pytest.raises(SmartcardConfigurationError, match="could not be identified unambiguously"):
        _select_certificate([_piv_auth_cert(subject="CN=A"), _piv_auth_cert(subject="CN=B")], now=_NOW)


def test_select_ambiguity_error_lists_candidates() -> None:
    with pytest.raises(SmartcardConfigurationError) as exc_info:
        _select_certificate([_signature_cert(subject="CN=One"), _signature_cert(subject="CN=Two")], now=_NOW)
    message = str(exc_info.value)
    assert "CN=One" in message
    assert "CN=Two" in message


# ---------------------------------------------------------------------------
# _to_identity
# ---------------------------------------------------------------------------


def test_to_identity_maps_and_normalizes_fields() -> None:
    cert = _FakeCert(
        thumbprint="aa bb cc dd",
        raw_data=b"\x30\x82\xff",
        subject="CN=PIV Auth",
        issuer="CN=DoD CA",
        public_key_oid=_OID_RSA,
    )
    identity = _to_identity(cert)
    assert identity.certificate is cert
    assert identity.der_certificate == b"\x30\x82\xff"
    assert identity.thumbprint == "AABBCCDD"  # whitespace stripped, upper-cased
    assert identity.subject == "CN=PIV Auth"
    assert identity.issuer == "CN=DoD CA"
    assert identity.public_key_oid == _OID_RSA
