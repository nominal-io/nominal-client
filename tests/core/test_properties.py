from __future__ import annotations

import pytest
from nominal_api import api, scout_catalog

from nominal.core._utils.api_tools import typed_property_update
from nominal.core._utils.properties import (
    properties_from_conjure,
    properties_from_proto,
    typed_properties_to_conjure,
    typed_properties_to_proto,
)
from nominal.core._utils.query_tools import (
    _build_property_search_clause,
    create_search_assets_query,
    create_search_datasets_query,
    create_search_runs_query,
)
from nominal.protos.asset.v2 import asset_pb2
from nominal.protos.run.v1 import run_service_pb2
from nominal.protos.types import types_pb2


def test_properties_preserve_strings_and_numbers() -> None:
    """Both transports retain scalar values, normalize integers, and reject booleans."""
    properties = {"serial": "", "mass_kg": 12.5, "count": 0}
    for to_wire, from_wire in (
        (typed_properties_to_conjure, properties_from_conjure),
        (typed_properties_to_proto, properties_from_proto),
    ):
        wire = to_wire(properties)
        restored = from_wire(wire)
        assert wire["serial"].string_value == ""
        assert restored == properties
        assert isinstance(restored["count"], float)
        with pytest.raises(TypeError, match="must be str, int, or float"):
            to_wire({"enabled": True})
    assert typed_property_update(None) is None
    assert typed_property_update({}) == types_pb2.TypedPropertyUpdateWrapper(typed_properties={})


def test_property_search_uses_equality() -> None:
    """The shared builder preserves each string filter shape and numeric equality on all three routes."""
    proto_predicate = types_pb2.NumericPropertyPredicate(name="count", operator=types_pb2.EQ, value=2.0)
    conjure_predicate = api.NumericPropertyPredicate(
        name="count", operator=api.PropertyComparisonOperator.EQ, value=2.0
    )
    for expected_string, predicate in (
        (asset_pb2.SearchAssetsQuery(property=types_pb2.Property(name="serial", value="")), proto_predicate),
        (
            scout_catalog.SearchDatasetsQuery(properties=api.Property(name="serial", value="")),
            conjure_predicate,
        ),
        (
            run_service_pb2.SearchQuery(properties=run_service_pb2.PropertiesFilter(name="serial", values=[""])),
            proto_predicate,
        ),
    ):
        assert (
            _build_property_search_clause(name="serial", value="", search_type=type(expected_string)) == expected_string
        )
        clause = _build_property_search_clause(name="count", value=2, search_type=type(expected_string))
        assert clause.numeric_property == predicate
    properties = {"serial": "", "count": 2}
    assert (
        getattr(create_search_assets_query(properties=properties), "and").queries[-1].numeric_property
        == proto_predicate
    )
    assert create_search_runs_query(properties=properties).all_of.queries[-1].numeric_property == proto_predicate
    assert create_search_datasets_query(properties=properties).and_[-1].numeric_property == conjure_predicate
