from __future__ import annotations

import pytest

from nominal.core._utils.properties import warn_deprecated_search_properties
from nominal.core.exceptions import SearchPropertiesDeprecationWarning
from nominal.core.properties import PropertyFilter


def test_filter_factory_validation() -> None:
    """Numeric filters reject bool; string in_ requires at least one value."""
    with pytest.raises(TypeError, match="int or float"):
        PropertyFilter.gt("mass_kg", True)
    with pytest.raises(ValueError, match="at least one value"):
        PropertyFilter.in_("site", [])


def test_warn_deprecated_search_properties_uses_dedicated_category() -> None:
    """Public search_* warn via SearchPropertiesDeprecationWarning when properties= is passed."""
    with pytest.warns(SearchPropertiesDeprecationWarning, match="property_filters"):
        warn_deprecated_search_properties({"serial": "A1"})
    warn_deprecated_search_properties(None)
