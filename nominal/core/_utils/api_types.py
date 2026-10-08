from __future__ import annotations

from typing import Mapping, TypeAlias

PropertyValue: TypeAlias = str | float
"""A stored property value. Integer inputs are converted to floats; booleans are rejected."""

NominalProperties: TypeAlias = Mapping[str, PropertyValue]
"""A mapping of string or numeric properties for assets, runs, datasets, and events."""
