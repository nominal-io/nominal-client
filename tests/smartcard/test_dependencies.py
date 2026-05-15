from __future__ import annotations

import pytest

from nominal.smartcard._dependencies import assert_required_dependencies_available
from nominal.smartcard._errors import SmartcardDependencyError


def test_smartcard_dependency_check_names_missing_extra(monkeypatch: pytest.MonkeyPatch) -> None:
    def find_spec(import_name: str):
        if import_name == "PyKCS11":
            return None
        return object()

    monkeypatch.setattr("nominal.smartcard._dependencies.importlib.util.find_spec", find_spec)

    with pytest.raises(SmartcardDependencyError, match="pip install 'nominal\\[smartcard\\]'"):
        assert_required_dependencies_available()


def test_smartcard_dependency_check_passes_when_all_present(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("nominal.smartcard._dependencies.importlib.util.find_spec", lambda _: object())
    assert_required_dependencies_available() is None  # no exception
