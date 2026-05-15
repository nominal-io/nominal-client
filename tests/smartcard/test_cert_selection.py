from __future__ import annotations

import pytest

from nominal.smartcard._cert_selection import select_piv_authentication_certificate
from nominal.smartcard._errors import SmartcardCertificateSelectionError
from tests.smartcard._helpers import _candidate

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
    piv = _candidate(slot="9A")
    dig = _candidate(label="Digital Signature", slot="9C")
    assert select_piv_authentication_certificate([dig, piv]) is piv


def test_select_rejects_ambiguous_piv_candidates() -> None:
    first = _candidate(label="PIV Authentication 1", pkcs11_uri="pkcs11:object=one")
    second = _candidate(label="PIV Authentication 2", pkcs11_uri="pkcs11:object=two")

    with pytest.raises(SmartcardCertificateSelectionError, match="Multiple PIV Authentication"):
        select_piv_authentication_certificate([first, second])


def test_select_no_piv_candidates_raises_with_discovered_list() -> None:
    c = _candidate(label="Digital Signature", slot="9C")
    with pytest.raises(SmartcardCertificateSelectionError, match="Digital Signature"):
        select_piv_authentication_certificate([c])
