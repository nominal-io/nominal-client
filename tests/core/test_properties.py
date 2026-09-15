from __future__ import annotations

import logging
from types import SimpleNamespace

import pytest
from nominal_api import api, scout_run_api

from nominal.core import properties as props
from nominal.core._utils.properties import (
    resource_properties_from_conjure,
    string_properties_for_ingest,
    typed_properties_from_conjure,
    typed_properties_to_conjure,
)
from nominal.core._utils.query_tools import create_search_runs_query


def test_typed_properties_to_conjure_converts_strings_and_floats() -> None:
    """Strings stay strings; floats become numericValue. None omits the map."""
    result = typed_properties_to_conjure({"serial": "A1", "mass_kg": 12.0, "temp_c": 1.5})

    assert result["serial"].type == "stringValue"
    assert result["serial"].string_value == "A1"
    assert result["mass_kg"].type == "numericValue"
    assert result["mass_kg"].numeric_value == 12.0
    assert result["temp_c"].numeric_value == 1.5
    assert typed_properties_to_conjure(None) is None
    assert typed_properties_to_conjure({}) == {}


def test_typed_properties_from_conjure_skips_unknown_variant(caplog: pytest.LogCaptureFixture) -> None:
    """Unknown TypedPropertyValue variants are skipped with a warning instead of failing the read."""
    typed = {
        "ok": api.TypedPropertyValue(string_value="A1"),
        "future": SimpleNamespace(type="booleanValue", numeric_value=None, string_value=None),  # type: ignore[dict-item]
    }

    with caplog.at_level(logging.WARNING):
        result = typed_properties_from_conjure(typed)

    assert result == {"ok": "A1"}
    assert "unknown type" in caplog.text
    assert "booleanValue" in caplog.text


def test_resource_properties_from_conjure_merges_legacy_with_typed_winning() -> None:
    """Legacy string properties fill gaps; typed values override duplicate keys."""
    typed = {
        "serial": api.TypedPropertyValue(string_value="typed"),
        "mass_kg": api.TypedPropertyValue(numeric_value=12.5),
    }
    legacy = {"serial": "legacy", "site": "pad-a"}

    result = resource_properties_from_conjure(typed, legacy)
    assert dict(result) == {"serial": "typed", "mass_kg": 12.5, "site": "pad-a"}

    legacy_only = resource_properties_from_conjure(None, {"serial": "A1"})
    assert dict(legacy_only) == {"serial": "A1"}


def test_string_properties_for_ingest_rejects_numerics() -> None:
    """Ingest destinations accept strings only; numbers raise TypeError."""
    assert string_properties_for_ingest({"serial": "A1"}) == {"serial": "A1"}
    with pytest.raises(TypeError, match="ingest"):
        string_properties_for_ingest({"mass_kg": 12.5})


def test_filter_factories_eq_gt_between() -> None:
    """Equality filters accept string or float; gt and between cover comparison and range."""
    string_eq = props.eq("serial", "A1")
    assert string_eq.name == "serial"
    assert string_eq.value == "A1"

    numeric_eq = props.eq("mass_kg", 12.0)
    assert numeric_eq.operator == props.PropertyComparisonOperator.EQ
    assert numeric_eq.value == 12.0

    gt = props.gt("mass_kg", 10.0)
    assert gt.operator == props.PropertyComparisonOperator.GT
    assert gt.value == 10.0

    rng = props.between("mass_kg", 1.0, 10.0)
    assert rng.min_value == 1.0
    assert rng.max_value == 10.0
    clause = rng.to_query_clause(scout_run_api.SearchQuery)
    assert clause.numeric_property_range is not None
    assert clause.numeric_property_range.operator == api.NumericPropertyRangeOperator.BETWEEN
    assert clause.numeric_property_range.min == 1.0
    assert clause.numeric_property_range.max == 10.0


def _run_clauses(query: scout_run_api.SearchQuery) -> list[scout_run_api.SearchQuery]:
    assert query.and_ is not None
    return query.and_


def test_create_search_runs_query_mixed_filters() -> None:
    """String eq, numeric eq, comparison, and range filters AND together."""
    query = create_search_runs_query(
        property_filters=[
            props.eq("serial", "A1"),
            props.eq("mass_kg", 12.5),
            props.gt("temp_c", 0.0),
            props.between("mass_kg", 1.0, 20.0),
        ]
    )
    clauses = _run_clauses(query)
    assert len(clauses) == 4

    string_clause = next(c for c in clauses if c.properties is not None)
    assert string_clause.properties.name == "serial"
    assert string_clause.properties.values == ["A1"]

    numeric_eq = next(c for c in clauses if c.numeric_property is not None and c.numeric_property.name == "mass_kg")
    assert numeric_eq.numeric_property.operator == api.PropertyComparisonOperator.EQ
    assert numeric_eq.numeric_property.value == 12.5

    numeric_gt = next(c for c in clauses if c.numeric_property is not None and c.numeric_property.name == "temp_c")
    assert numeric_gt.numeric_property.operator == api.PropertyComparisonOperator.GT

    rng = next(c for c in clauses if c.numeric_property_range is not None)
    assert rng.numeric_property_range.operator == api.NumericPropertyRangeOperator.BETWEEN
    assert rng.numeric_property_range.min == 1.0
    assert rng.numeric_property_range.max == 20.0


def test_create_search_runs_query_deprecated_properties_expands_and_warns() -> None:
    """Deprecated properties= still expands to eq() clauses and warns."""
    with pytest.warns(DeprecationWarning, match="properties="):
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
