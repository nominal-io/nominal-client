from __future__ import annotations

import importlib.util

from nominal.smartcard._errors import SmartcardDependencyError

_REQUIRED_IMPORTS_TO_PACKAGES = {
    "PyKCS11": "PyKCS11",
    "OpenSSL": "pyOpenSSL",
    "cffi": "cffi",
    "cryptography": "cryptography",
}


def assert_required_dependencies_available() -> None:
    """Ensure the optional Python pieces for smartcard auth are installed."""
    missing_packages = [
        package
        for import_name, package in _REQUIRED_IMPORTS_TO_PACKAGES.items()
        if importlib.util.find_spec(import_name) is None
    ]

    if missing_packages:
        missing = ", ".join(sorted(missing_packages))
        raise SmartcardDependencyError(
            "Smartcard auth requires optional dependencies that are not installed: "
            f"{missing}. Install them with `pip install 'nominal[smartcard]'`."
        )
