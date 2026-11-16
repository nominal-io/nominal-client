"""``nominal.thirdparty.pandas`` keeps working, with a deprecation warning."""

from __future__ import annotations

import importlib
import sys

import pytest

import nominal.pandas

NAMES = [
    "channel_to_dataframe_decimated",
    "channel_to_series",
    "datasource_to_dataframe",
    "upload_dataframe",
    "upload_dataframe_to_dataset",
]


def _forget_old_modules() -> None:
    for name in [m for m in sys.modules if m.startswith("nominal.thirdparty.pandas")]:
        del sys.modules[name]


def test_old_path_warns_and_reexports_the_same_objects() -> None:
    """nominal.thirdparty.pandas warns on import and re-exports the same objects as nominal.pandas."""
    _forget_old_modules()
    with pytest.warns(DeprecationWarning, match=r"nominal\.thirdparty\.pandas is deprecated.*nominal\.pandas"):
        old = importlib.import_module("nominal.thirdparty.pandas")
    assert sorted(old.__all__) == NAMES
    for name in NAMES:
        assert getattr(old, name) is getattr(nominal.pandas, name)


def test_old_private_module_still_importable() -> None:
    """The old private module path still imports, with a warning, and gives the same function."""
    _forget_old_modules()
    with pytest.warns(DeprecationWarning):
        from nominal.thirdparty.pandas._pandas import upload_dataframe
    assert upload_dataframe is nominal.pandas.upload_dataframe


def test_tdms_does_not_import_the_deprecated_module() -> None:
    """Importing nominal.tdms emits no deprecation warning, because it uses nominal.pandas."""
    import subprocess
    import sys

    pytest.importorskip("nptdms")
    code = "import warnings; warnings.simplefilter('error', DeprecationWarning); import nominal.tdms"
    assert subprocess.run([sys.executable, "-c", code], check=False).returncode == 0
