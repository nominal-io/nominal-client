from __future__ import annotations

import builtins
import subprocess
import sys
from typing import Any

import plotly.io as pio
import pytest

import nominal.style


def test_use_plotly() -> None:
    """use('plotly') registers the nominal template (transparent, teal first) and makes it the default."""
    previous = pio.templates.default
    try:
        nominal.style.use("plotly")
        assert pio.templates.default == "plotly_white+nominal"
        layout = pio.templates["nominal"].layout
        assert layout.paper_bgcolor == "rgba(0, 0, 0, 0)"
        assert tuple(layout.colorway) == nominal.style.COLORWAY
        assert layout.colorway[0] == "#00CC96"  # teal first
    finally:
        pio.templates.default = previous


def test_import_is_light() -> None:
    """Importing nominal.style must not import plotly itself."""
    code = "import sys, nominal.style; sys.exit('plotly' in sys.modules)"
    assert subprocess.run([sys.executable, "-c", code], check=False).returncode == 0


def test_unknown_library() -> None:
    """An unknown library names the ones that work."""
    with pytest.raises(ValueError, match="plotly"):
        nominal.style.use("matplotlib")  # type: ignore[arg-type]


def test_missing_plotly_is_actionable(monkeypatch: pytest.MonkeyPatch) -> None:
    """Without plotly, use raises an ImportError that says to install nominal[plotly]."""
    real_import = builtins.__import__

    def fake(name: str, *args: Any, **kwargs: Any) -> Any:
        if name.startswith("plotly"):
            raise ImportError("no plotly")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake)
    with pytest.raises(ImportError, match=r"nominal\[plotly\]"):
        nominal.style.use("plotly")
