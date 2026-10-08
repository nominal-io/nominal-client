from __future__ import annotations

import pytest
from nominal_api import api

from nominal.core._utils.properties import (
    properties_from_conjure,
    properties_from_proto,
    typed_properties_to_conjure,
    typed_properties_to_proto,
)
from nominal.core._utils.query_tools import (
    create_search_assets_query,
    create_search_datasets_query,
    create_search_runs_query,
)


def test_conjure_properties_preserve_strings_and_numbers() -> None:
    """Conjure retains scalar values, normalizes integers, and rejects booleans."""
    properties = {"serial": "", "mass_kg": 12.5, "count": 0}
    wire = typed_properties_to_conjure(properties)
    restored = properties_from_conjure(wire)
    assert wire["serial"].string_value == ""
    assert restored == properties
    assert isinstance(restored["count"], float)
    with pytest.raises(TypeError, match="must be str, int, or float"):
        typed_properties_to_conjure({"enabled": True})


def test_proto_properties_preserve_strings_and_numbers() -> None:
    """Proto retains empty strings and zero without confusing inactive oneof fields."""
    properties = {"serial": "", "mass_kg": 12.5, "count": 0}
    wire = typed_properties_to_proto(properties)
    restored = properties_from_proto(wire)
    assert wire["count"].WhichOneof("typed_property_value") == "numeric_value"
    assert restored == properties
    assert isinstance(restored["count"], float)


def test_numeric_property_search_uses_equality() -> None:
    """All three search routes encode numeric equality through the existing properties argument."""
    expected = api.NumericPropertyPredicate(name="count", operator=api.PropertyComparisonOperator.EQ, value=2.0)
    for create_query in (create_search_assets_query, create_search_datasets_query, create_search_runs_query):
        query = create_query(properties={"count": 2})
        assert query.and_ is not None
        assert query.and_[-1].numeric_property == expected
