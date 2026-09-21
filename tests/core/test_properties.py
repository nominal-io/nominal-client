from __future__ import annotations

import logging
from types import SimpleNamespace
from typing import Any, Callable

import pytest
from nominal_api import api, scout_run_api

from nominal.core._utils.properties import (
    typed_properties_from_conjure,
    typed_properties_from_proto,
    typed_properties_to_conjure,
    typed_properties_to_proto,
    warn_deprecated_search_properties,
)
from nominal.core._utils.query_tools import create_search_runs_query
from nominal.core.exceptions import SearchPropertiesDeprecationWarning
from nominal.core.properties import PropertyFilter
from nominal.protos.types import types_pb2


@pytest.mark.parametrize(
    "to_typed",
    [typed_properties_to_conjure, typed_properties_to_proto],
    ids=["conjure", "proto"],
)
def test_typed_properties_to_transport_converts_strings_and_floats(
    to_typed: Callable[..., Any],
) -> None:
    """Strings stay strings; ints and floats become numeric. None omits the map."""
    result = to_typed({"serial": "A1", "mass_kg": 12.0, "count": 5})

    assert result["serial"].string_value == "A1"
    assert result["mass_kg"].numeric_value == 12.0
    assert result["count"].numeric_value == 5.0
    assert to_typed(None) is None
    assert to_typed({}) == {}
    with pytest.raises(TypeError, match="str, int, or float"):
        to_typed({"flag": True})


@pytest.mark.parametrize(
    ("typed", "from_typed"),
    [
        (
            {
                "ok": types_pb2.TypedPropertyValue(string_value="A1"),
                "future": types_pb2.TypedPropertyValue(),
            },
            typed_properties_from_proto,
        ),
        (
            {
                "ok": api.TypedPropertyValue(string_value="A1"),
                "future": SimpleNamespace(type="booleanValue", numeric_value=None, string_value=None),
            },
            typed_properties_from_conjure,
        ),
    ],
    ids=["proto", "conjure"],
)
def test_typed_properties_from_transport_skips_unknown_variant(
    typed: dict[str, Any],
    from_typed: Callable[..., dict[str, Any]],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Unknown TypedPropertyValue variants are skipped with a warning instead of failing the read."""
    with caplog.at_level(logging.WARNING):
        result = from_typed(typed)

    assert result == {"ok": "A1"}
    assert "unknown type" in caplog.text


def test_filter_factory_validation() -> None:
    """Numeric factories reject bool; string in_ requires at least one value."""
    with pytest.raises(TypeError, match="int or float"):
        PropertyFilter.gt("mass_kg", True)
    with pytest.raises(ValueError, match="at least one value"):
        PropertyFilter.in_("site", [])


def _run_clauses(query: scout_run_api.SearchQuery) -> list[scout_run_api.SearchQuery]:
    assert query.and_ is not None
    return query.and_


def test_create_search_runs_query_mixed_filters() -> None:
    """String eq, numeric eq, comparison, and range filters AND together."""
    query = create_search_runs_query(
        property_filters=[
            PropertyFilter.eq("serial", "A1"),
            PropertyFilter.in_("site", ["pad-a", "pad-b"]),
            PropertyFilter.eq("mass_kg", 12.5),
            PropertyFilter.gt("temp_c", 0.0),
            PropertyFilter.between("mass_kg", 1.0, 20.0),
        ]
    )
    clauses = _run_clauses(query)
    assert len(clauses) == 5

    string_clause = next(c for c in clauses if c.properties is not None and c.properties.name == "serial")
    assert string_clause.properties.values == ["A1"]

    string_in = next(c for c in clauses if c.properties is not None and c.properties.name == "site")
    assert string_in.properties.values == ["pad-a", "pad-b"]

    numeric_eq = next(c for c in clauses if c.numeric_property is not None and c.numeric_property.name == "mass_kg")
    assert numeric_eq.numeric_property.operator == api.PropertyComparisonOperator.EQ
    assert numeric_eq.numeric_property.value == 12.5

    numeric_gt = next(c for c in clauses if c.numeric_property is not None and c.numeric_property.name == "temp_c")
    assert numeric_gt.numeric_property.operator == api.PropertyComparisonOperator.GT

    rng = next(c for c in clauses if c.numeric_property_range is not None)
    assert rng.numeric_property_range.operator == api.NumericPropertyRangeOperator.BETWEEN
    assert rng.numeric_property_range.min == 1.0
    assert rng.numeric_property_range.max == 20.0


def test_create_search_runs_query_deprecated_properties_expands() -> None:
    """Deprecated properties= still expands to PropertyFilter.eq() clauses."""
    query = create_search_runs_query(properties={"serial": "A1", "mass_kg": 12.5})

    clauses = _run_clauses(query)
    assert len(clauses) == 2

    string_clause = next(c for c in clauses if c.properties is not None)
    assert string_clause.properties.name == "serial"
    assert string_clause.properties.values == ["A1"]

    numeric_eq = next(c for c in clauses if c.numeric_property is not None)
    assert numeric_eq.numeric_property.name == "mass_kg"
    assert numeric_eq.numeric_property.operator == api.PropertyComparisonOperator.EQ
    assert numeric_eq.numeric_property.value == 12.5


def test_warn_deprecated_search_properties_uses_dedicated_category() -> None:
    """Public search_* warn via SearchPropertiesDeprecationWarning when properties= is passed."""
    with pytest.warns(SearchPropertiesDeprecationWarning, match="property_filters"):
        warn_deprecated_search_properties({"serial": "A1"})
    warn_deprecated_search_properties(None)
