from __future__ import annotations

import pytest
from nominal_api import api, scout_run_api

from nominal.core._utils.properties import (
    between,
    eq,
    gt,
    gte,
    lt,
    lte,
    neq,
    not_between,
    string_properties_for_ingest,
    typed_properties_from_conjure,
    typed_properties_to_conjure,
)
from nominal.core._utils.query_tools import (
    create_search_assets_query,
    create_search_datasets_query,
    create_search_runs_query,
)


def test_typed_properties_to_conjure_converts_strings_and_numbers() -> None:
    result = typed_properties_to_conjure({"serial": "A1", "mass_kg": 12, "temp_c": 1.5})

    assert result["serial"].type == "stringValue"
    assert result["serial"].string_value == "A1"
    assert result["mass_kg"].type == "numericValue"
    assert result["mass_kg"].numeric_value == 12.0
    assert result["temp_c"].numeric_value == 1.5


def test_typed_properties_to_conjure_rejects_bool_and_other_types() -> None:
    with pytest.raises(TypeError, match="must be str, int, or float"):
        typed_properties_to_conjure({"ok": True})
    with pytest.raises(TypeError, match="must be str, int, or float"):
        typed_properties_to_conjure({"ok": None})  # type: ignore[dict-item]


def test_typed_properties_from_conjure_reads_only_typed_map() -> None:
    typed = {
        "serial": api.TypedPropertyValue(string_value="A1"),
        "mass_kg": api.TypedPropertyValue(numeric_value=12.5),
    }

    result = typed_properties_from_conjure(typed)

    assert dict(result) == {"serial": "A1", "mass_kg": 12.5}


def test_typed_properties_from_conjure_empty() -> None:
    assert dict(typed_properties_from_conjure(None)) == {}
    assert dict(typed_properties_from_conjure({})) == {}


def test_string_properties_for_ingest_rejects_numerics() -> None:
    assert string_properties_for_ingest({"serial": "A1"}) == {"serial": "A1"}
    with pytest.raises(TypeError, match="ingest"):
        string_properties_for_ingest({"mass_kg": 12.5})


def test_numeric_property_filter_factories() -> None:
    assert eq("mass_kg", 10).to_comparison_predicate().operator == api.PropertyComparisonOperator.EQ
    assert neq("mass_kg", 10).to_comparison_predicate().operator == api.PropertyComparisonOperator.NEQ
    assert gt("mass_kg", 10).to_comparison_predicate().operator == api.PropertyComparisonOperator.GT
    assert gte("mass_kg", 10).to_comparison_predicate().operator == api.PropertyComparisonOperator.GTE
    assert lt("mass_kg", 10).to_comparison_predicate().operator == api.PropertyComparisonOperator.LT
    assert lte("mass_kg", 10).to_comparison_predicate().operator == api.PropertyComparisonOperator.LTE

    rng = between("mass_kg", 1, 10)
    assert rng.is_range()
    predicate = rng.to_range_predicate()
    assert predicate.operator == api.NumericPropertyRangeOperator.BETWEEN
    assert predicate.min == 1.0
    assert predicate.max == 10.0

    not_rng = not_between("mass_kg", 1, 10)
    assert not_rng.to_range_predicate().operator == api.NumericPropertyRangeOperator.NOT_BETWEEN


def test_numeric_property_filter_rejects_bool() -> None:
    with pytest.raises(TypeError, match="must be int or float"):
        gt("mass_kg", True)  # type: ignore[arg-type]


def test_numeric_property_filter_predicate_kind_mismatch() -> None:
    with pytest.raises(ValueError, match="not a comparison operator"):
        between("mass_kg", 1, 10).to_comparison_predicate()
    with pytest.raises(ValueError, match="not a range operator"):
        gt("mass_kg", 10).to_range_predicate()


def _run_clauses(query: scout_run_api.SearchQuery) -> list[scout_run_api.SearchQuery]:
    assert query.and_ is not None
    return query.and_


def test_create_search_runs_query_mixed_equality_and_filters() -> None:
    query = create_search_runs_query(
        properties={"serial": "A1", "mass_kg": 12.5},
        property_filters=[gt("temp_c", 0), between("mass_kg", 1, 20)],
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


def test_create_search_assets_query_numeric_equality() -> None:
    query = create_search_assets_query(properties={"mass_kg": 5}, property_filters=[lt("mass_kg", 10)])
    assert query.and_ is not None
    assert len(query.and_) == 2
    assert query.and_[0].numeric_property is not None
    assert query.and_[0].numeric_property.operator == api.PropertyComparisonOperator.EQ
    assert query.and_[1].numeric_property is not None
    assert query.and_[1].numeric_property.operator == api.PropertyComparisonOperator.LT


def test_create_search_datasets_query_numeric_range() -> None:
    query = create_search_datasets_query(properties={"serial": "A1"}, property_filters=[between("mass_kg", 0, 100)])
    assert query.and_ is not None
    string_clause = next(c for c in query.and_ if c.properties is not None)
    assert string_clause.properties.name == "serial"
    range_clause = next(c for c in query.and_ if c.numeric_property_range is not None)
    assert range_clause.numeric_property_range.min == 0.0
    assert range_clause.numeric_property_range.max == 100.0
