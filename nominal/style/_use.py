"""The entry point."""

from __future__ import annotations

from typing import Literal

LIBRARIES = ("plotly",)
"""The plotting libraries `use` knows."""


def use(library: Literal["plotly"] = "plotly") -> None:
    """Make Nominal's style the default for a plotting library.

    ::

        nominal.style.use("plotly")

    Args:
        library: The plotting library to style. Only ``"plotly"`` for now; see `nominal.style.LIBRARIES`.

    Raises:
        ValueError: If ``library`` isn't one this module knows.
        ImportError: If the library isn't installed (``pip install nominal[plotly]``).
    """
    if library == "plotly":
        from nominal.style._plotly import use_plotly

        use_plotly()
        return
    raise ValueError(f"unknown library {library!r}; expected one of {', '.join(LIBRARIES)}")
