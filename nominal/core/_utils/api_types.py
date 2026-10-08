from __future__ import annotations

from typing import Mapping, TypeAlias

PropertyValue: TypeAlias = str | float
"""A stored property value. Integer inputs are converted to floats; booleans are rejected."""

StringProperties: TypeAlias = Mapping[str, str]
"""Properties for APIs whose wire format supports only string values."""

TypedProperties: TypeAlias = Mapping[str, PropertyValue]
"""A mapping of property names to string or numeric values."""

NominalProperties: TypeAlias = TypedProperties
"""Properties for assets, runs, datasets, and events."""
