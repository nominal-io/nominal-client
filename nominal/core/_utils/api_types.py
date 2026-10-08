from __future__ import annotations

from typing import Mapping, TypeAlias

PropertyValue: TypeAlias = str | float
"""A string or numeric property value."""

NominalProperties: TypeAlias = Mapping[str, str]
"""A mapping of property names to string values."""
