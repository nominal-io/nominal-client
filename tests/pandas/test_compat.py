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
    _forget_old_modules()
    with pytest.warns(DeprecationWarning, match=r"nominal\.thirdparty\.pandas is deprecated.*nominal\.pandas"):
        old = importlib.import_module("nominal.thirdparty.pandas")
    assert sorted(old.__all__) == NAMES
    for name in NAMES:
        assert getattr(old, name) is getattr(nominal.pandas, name)


def test_old_private_module_still_importable() -> None:
    _forget_old_modules()
    with pytest.warns(DeprecationWarning):
        from nominal.thirdparty.pandas._pandas import upload_dataframe
    assert upload_dataframe is nominal.pandas.upload_dataframe
