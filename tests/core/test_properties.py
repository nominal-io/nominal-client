from __future__ import annotations

from typing import Callable

import pytest
from nominal_api import api, scout_asset_api, scout_catalog, scout_run_api

from nominal.core._utils.properties import (
    check_property_value,
    properties_from_conjure,
    properties_from_proto,
    typed_properties_to_conjure,
    typed_properties_to_proto,
    warn_deprecated_search_properties,
)
from nominal.core._utils.query_tools import (
    create_search_assets_query,
    create_search_datasets_query,
    create_search_runs_query,
)
from nominal.core.property_filter import PropertyFilter
from nominal.exceptions import SearchPropertiesDeprecationWarning

SearchQuery = scout_asset_api.SearchAssetsQuery | scout_catalog.SearchDatasetsQuery | scout_run_api.SearchQuery


@pytest.mark.parametrize(
    "build_query",
    [create_search_assets_query, create_search_datasets_query, create_search_runs_query],
    ids=["assets", "datasets", "runs"],
)
def test_numeric_filters_build_typed_query_predicates(build_query: Callable[..., SearchQuery]) -> None:
    """Each search route encodes numeric comparisons and inclusive ranges in typed predicates."""
    query = build_query(
        property_filters=[
            PropertyFilter.eq("count", 2),
            PropertyFilter.gt("mass_kg", 10.5),
            PropertyFilter.between("temp_c", 0, 100),
        ]
    )

    assert query.and_ is not None
    comparisons = [clause.numeric_property for clause in query.and_ if clause.numeric_property is not None]
    assert [(p.name, p.operator, p.value) for p in comparisons] == [
        ("count", api.PropertyComparisonOperator.EQ, 2.0),
        ("mass_kg", api.PropertyComparisonOperator.GT, 10.5),
    ]
    assert isinstance(comparisons[0].value, float)
    ranges = [clause.numeric_property_range for clause in query.and_ if clause.numeric_property_range is not None]
    assert [(p.name, p.operator, p.min, p.max) for p in ranges] == [
        ("temp_c", api.NumericPropertyRangeOperator.BETWEEN, 0.0, 100.0)
    ]


@pytest.mark.parametrize("build_query", [create_search_assets_query, create_search_runs_query], ids=["assets", "runs"])
def test_string_in_filters_build_native_multivalue_predicates(build_query: Callable[..., SearchQuery]) -> None:
    """Asset and run searches retain every alternative in a native string property predicate."""
    query = build_query(property_filters=[PropertyFilter.in_("site", ["pad-a", "pad-b"])])

    assert query.and_ is not None
    assert len(query.and_) == 1
    predicate = query.and_[0].properties
    assert predicate is not None
    assert predicate.name == "site"
    assert predicate.values == ["pad-a", "pad-b"]


def test_dataset_string_in_filter_joins_equalities_with_or() -> None:
    """Dataset string IN uses OR across equality clauses alongside the default archive filter."""
    query = create_search_datasets_query(property_filters=[PropertyFilter.in_("site", ["pad-a", "pad-b"])])

    assert query.and_ is not None
    assert query.and_[0].archive_status is False
    alternatives = query.and_[1].or_
    assert alternatives is not None
    assert all(clause.properties is not None for clause in alternatives)
    assert [(clause.properties.name, clause.properties.value) for clause in alternatives] == [
        ("site", "pad-a"),
        ("site", "pad-b"),
    ]


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


def test_conjure_properties_preserve_strings_and_numbers() -> None:
    """Conjure property conversion retains strings and normalizes integer inputs to floats."""
    properties = {"serial": "A1", "mass_kg": 12.5, "count": 2, "zero": 0.0, "empty": ""}

    wire = typed_properties_to_conjure(properties)
    restored = properties_from_conjure(wire)

    assert wire["serial"].string_value == "A1"
    assert wire["count"].numeric_value == 2.0
    assert restored == properties
    assert isinstance(restored["count"], float)


def test_proto_properties_preserve_strings_and_numbers() -> None:
    """Proto property conversion retains scalar values, including zero and empty strings."""
    properties = {"serial": "A1", "mass_kg": 12.5, "count": 2, "zero": 0.0, "empty": ""}

    wire = typed_properties_to_proto(properties)
    restored = properties_from_proto(wire)

    assert wire["serial"].WhichOneof("typed_property_value") == "string_value"
    assert wire["count"].WhichOneof("typed_property_value") == "numeric_value"
    assert restored == properties
    assert isinstance(restored["count"], float)


@pytest.mark.parametrize("value", [True, None, []])
def test_property_values_reject_unsupported_types(value: object) -> None:
    """Property validation rejects booleans and values outside strings or numbers."""
    with pytest.raises(TypeError, match="property 'serial' must be str, int, or float"):
        check_property_value(value, name="serial")
