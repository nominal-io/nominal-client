from __future__ import annotations

import pytest
from nominal_api import api, scout_asset_api, scout_catalog, scout_rids_api, scout_run_api

from nominal.core._utils.properties import (
    properties_from_conjure,
    typed_properties_to_conjure,
)
from nominal.core._utils.query_tools import (
    _build_property_search_clause,
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


def test_property_search_uses_equality() -> None:
    """The shared builder preserves each string filter shape and numeric equality on all three routes."""
    expected = api.NumericPropertyPredicate(name="count", operator=api.PropertyComparisonOperator.EQ, value=2.0)
    for create_query, expected_string in (
        (create_search_assets_query, scout_asset_api.SearchAssetsQuery(property=api.Property(name="serial", value=""))),
        (
            create_search_datasets_query,
            scout_catalog.SearchDatasetsQuery(properties=api.Property(name="serial", value="")),
        ),
        (
            create_search_runs_query,
            scout_run_api.SearchQuery(properties=scout_rids_api.PropertiesFilter(name="serial", values=[""])),
        ),
    ):
        clause = _build_property_search_clause(name="count", value=2, search_type=type(expected_string))
        assert clause.numeric_property == expected
        query = create_query(properties={"serial": "", "count": 2})
        assert query.and_ is not None
        assert query.and_[-2] == expected_string
        assert query.and_[-1] == clause
