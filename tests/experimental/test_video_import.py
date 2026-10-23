from __future__ import annotations

import builtins
import importlib
import sys
from unittest.mock import patch

import pytest


@pytest.mark.parametrize(
    ("error", "message"),
    [
        pytest.param(
            ModuleNotFoundError("No module named 'nominal_video'", name="nominal_video"),
            "pip install 'nominal[video]'",
            id="missing-extra",
        ),
        pytest.param(
            ImportError(
                "dlopen(/env/site-packages/nominal_video/nominal_video.so): Library not loaded: libgstwebrtc-1.0.dylib",
                name="nominal_video",
            ),
            "missing GStreamer 1.20+",
            id="missing-gstreamer",
        ),
        pytest.param(
            ModuleNotFoundError("No module named 'nominal_video._native'", name="nominal_video._native"),
            "Error loading rust bindings",
            id="missing-binding-submodule",
        ),
        pytest.param(
            ModuleNotFoundError("nominal_video requires video_dependency", name="video_dependency"),
            "Error loading rust bindings",
            id="missing-transitive-dependency",
        ),
        pytest.param(
            ImportError("cannot import name 'Codec' from 'nominal_video'", name="nominal_video"),
            "Error loading rust bindings",
            id="incompatible-bindings",
        ),
    ],
)
def test_video_import_reports_missing_extra_only_when_package_is_absent(error: ImportError, message: str):
    """Live-video imports distinguish an absent extra from installed bindings that cannot load."""
    original_import = builtins.__import__

    def import_with_binding_error(name, *args, **kwargs):
        """Fail at the optional native import without requiring a platform runtime."""
        if name == "nominal_video":
            raise error
        return original_import(name, *args, **kwargs)

    with patch.dict(sys.modules):
        sys.modules.pop("nominal.experimental.video", None)
        with (
            patch("builtins.__import__", side_effect=import_with_binding_error),
            pytest.raises(ImportError) as raised,
        ):
            importlib.import_module("nominal.experimental.video")

    assert message in str(raised.value)
    assert raised.value.__cause__ is error
