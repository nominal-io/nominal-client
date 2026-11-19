"""Nominal's look for plots: one palette, applied to a plotting library with `use`.

Importing this module doesn't import any plotting library; `use` does. Each library is an extra,
for example ``pip install nominal[plotly]``.
"""

from nominal.style._palette import COLORWAY, GRID, TEXT
from nominal.style._plotly import TEMPLATE
from nominal.style._use import LIBRARIES, use

__all__ = ["COLORWAY", "GRID", "LIBRARIES", "TEMPLATE", "TEXT", "use"]
