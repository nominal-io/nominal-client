from __future__ import annotations

import pytest

from nominal.smartcard._cert_selection import _assert_client_auth_eku, select_piv_authentication_certificate
from nominal.smartcard._errors import SmartcardCertificateSelectionError
from tests.smartcard._helpers import _candidate, _make_der_cert

# CertificateCandidate.is_piv_authentication_candidate


def test_is_piv_candidate_slot_9a() -> None:
    assert _candidate(slot="9A").is_piv_authentication_candidate


def test_is_piv_candidate_slot_case_insensitive() -> None:
    assert _candidate(slot="9a").is_piv_authentication_candidate


def test_is_not_piv_candidate_slot_9c() -> None:
    c = _candidate(label="Digital Signature", slot="9C")
    assert not c.is_piv_authentication_candidate


def test_is_not_piv_candidate_no_slot() -> None:
    c = _candidate(slot=None)
    assert not c.is_piv_authentication_candidate


# select_piv_authentication_certificate


def test_select_raises_when_no_candidates() -> None:
    with pytest.raises(SmartcardCertificateSelectionError, match="No certificates"):
        select_piv_authentication_certificate([])


def test_select_single_piv_auth_candidate() -> None:
    pytest.importorskip("cryptography")
    piv = _candidate(slot="9A", der_certificate=_make_der_cert(client_auth_eku=True))
    dig = _candidate(label="Digital Signature", slot="9C")
    assert select_piv_authentication_certificate([dig, piv]) is piv


def test_select_rejects_ambiguous_piv_candidates() -> None:
    first = _candidate(label="PIV Authentication 1", certificate_uri="pkcs11:object=one;type=cert")
    second = _candidate(label="PIV Authentication 2", certificate_uri="pkcs11:object=two;type=cert")

    with pytest.raises(SmartcardCertificateSelectionError, match="Multiple PIV Authentication"):
        select_piv_authentication_certificate([first, second])


def test_select_no_piv_candidates_raises() -> None:
    c = _candidate(label="Digital Signature", slot="9C")
    with pytest.raises(SmartcardCertificateSelectionError, match="Could not find a PIV Authentication"):
        select_piv_authentication_certificate([c])


# _assert_client_auth_eku


def test_assert_client_auth_eku_accepts_client_auth_cert() -> None:
    pytest.importorskip("cryptography")
    candidate = _candidate(der_certificate=_make_der_cert(client_auth_eku=True))
    _assert_client_auth_eku(candidate)  # must not raise


def test_assert_client_auth_eku_rejects_wrong_eku() -> None:
    pytest.importorskip("cryptography")
    candidate = _candidate(der_certificate=_make_der_cert(client_auth_eku=False))
    with pytest.raises(SmartcardCertificateSelectionError, match="clientAuth"):
        _assert_client_auth_eku(candidate)


def test_assert_client_auth_eku_rejects_missing_eku_extension() -> None:
    pytest.importorskip("cryptography")
    candidate = _candidate(der_certificate=_make_der_cert(has_eku_extension=False))
    with pytest.raises(SmartcardCertificateSelectionError, match="no ExtendedKeyUsage"):
        _assert_client_auth_eku(candidate)


def test_assert_client_auth_eku_rejects_empty_der() -> None:
    candidate = _candidate(der_certificate=b"")
    with pytest.raises(SmartcardCertificateSelectionError, match="no DER data"):
        _assert_client_auth_eku(candidate)
