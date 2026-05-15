from __future__ import annotations

import importlib.util

from nominal.smartcard._errors import SmartcardDependencyError

_REQUIRED_IMPORTS = (
    "PyKCS11",
    "cffi",
)


def assert_required_dependencies_available() -> None:
    """Ensure the optional Python pieces for smartcard auth are installed."""
    missing_imports = [
        import_name for import_name in _REQUIRED_IMPORTS if importlib.util.find_spec(import_name) is None
    ]

    if missing_imports:
        missing = ", ".join(sorted(missing_imports))
        raise SmartcardDependencyError(
            "Smartcard auth requires optional dependencies that are not installed: "
            f"{missing}. Install them with `pip install 'nominal[smartcard]'`."
        )
