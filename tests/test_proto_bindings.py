"""Tests validating that the nominal-api-protos package imports and works correctly.

Covers the three classes of breakage fixed in the PR:
1. Import-path rewriting: nominal.* → nominal_api_protos.nominal.*
2. Dependency proto stubs (nominal.gen.v1, nominal.conjure.v1, buf.validate)
3. googleapis-common-protos runtime dependency (google.api.*)
"""

import pytest


# ---------------------------------------------------------------------------
# Import smoke tests — each import exercises a different fix in the PR
# ---------------------------------------------------------------------------


def test_top_level_nominal_write_pb2() -> None:
    """Top-level proto (no sub-package path) should always have worked."""
    from nominal_api_protos import nominal_write_pb2  # noqa: F401

    assert nominal_write_pb2.DESCRIPTOR is not None


def test_nominal_types_pb2_import_rewriting() -> None:
    """nominal.types is a sub-package; requires import-path rewriting to be correct."""
    from nominal_api_protos.nominal.types import types_pb2  # noqa: F401

    assert types_pb2.DESCRIPTOR is not None


def test_nominal_events_pb2_import_rewriting() -> None:
    """Events proto lives deep in the package; exercises multi-level import rewriting."""
    from nominal_api_protos.nominal.events.v1 import events_pb2  # noqa: F401

    assert events_pb2.DESCRIPTOR is not None


def test_nominal_gen_v1_alias_pb2_dependency_proto() -> None:
    """nominal.gen.v1.alias_pb2 comes from a dependency proto (grpc-conjure-protos).
    Previously missing from sourceProtos, so Python stubs were not generated."""
    from nominal_api_protos.nominal.gen.v1 import alias_pb2  # noqa: F401

    assert alias_pb2.DESCRIPTOR is not None


def test_nominal_gen_v1_error_pb2_dependency_proto() -> None:
    """nominal.gen.v1.error_pb2 comes from a dependency proto (grpc-conjure-protos)."""
    from nominal_api_protos.nominal.gen.v1 import error_pb2  # noqa: F401

    assert error_pb2.DESCRIPTOR is not None


def test_nominal_conjure_v1_compat_pb2_dependency_proto() -> None:
    """nominal.conjure.v1.compat_pb2 comes from protoc-gen-java-protos.
    Previously missing from sourceProtos."""
    from nominal_api_protos.nominal.conjure.v1 import compat_pb2  # noqa: F401

    assert compat_pb2.DESCRIPTOR is not None


def test_buf_validate_pb2_relocated() -> None:
    """buf/validate stubs are relocated outside nominal_api_protos/ so they are
    importable as top-level 'buf.validate' (matches protovalidate's expected path)."""
    from buf.validate import validate_pb2  # noqa: F401

    assert validate_pb2.DESCRIPTOR is not None


def test_google_api_annotations_import() -> None:
    """google.api.* imports require the googleapis-common-protos runtime dep.
    The PR adds this to install_requires; without it this raises ImportError."""
    from google.api import annotations_pb2  # noqa: F401

    assert annotations_pb2.DESCRIPTOR is not None


# ---------------------------------------------------------------------------
# Functional tests — construct messages and round-trip through serialisation
# ---------------------------------------------------------------------------


def test_write_request_nominal_roundtrip() -> None:
    """Construct a WriteRequestNominal protobuf, serialise, and deserialise it."""
    from nominal_api_protos.nominal_write_pb2 import (
        Channel,
        DoublePoint,
        DoublePoints,
        Points,
        Series,
        WriteRequestNominal,
    )

    from google.protobuf.timestamp_pb2 import Timestamp

    timestamp_ns = 1_700_000_000_000_000_000  # arbitrary fixed nanosecond timestamp
    value = 42.0

    ts = Timestamp()
    ts.FromNanoseconds(timestamp_ns)
    point = DoublePoint(timestamp=ts, value=value)
    double_points = DoublePoints(points=[point])
    series = Series(
        channel=Channel(name="temperature"),
        points=Points(double_points=double_points),
    )
    request = WriteRequestNominal(series=[series])

    serialised = request.SerializeToString()
    restored = WriteRequestNominal()
    restored.ParseFromString(serialised)

    assert len(restored.series) == 1
    restored_series = restored.series[0]
    assert restored_series.channel.name == "temperature"
    assert len(restored_series.points.double_points.points) == 1
    assert restored_series.points.double_points.points[0].timestamp.ToNanoseconds() == timestamp_ns
    assert restored_series.points.double_points.points[0].value == pytest.approx(value)


def test_types_pb2_handle_roundtrip() -> None:
    """Construct a Handle message from nominal.types and round-trip it.
    This exercises that the import-rewriting produces functional, not just importable, code."""
    from nominal_api_protos.nominal.types import types_pb2

    handle = types_pb2.Handle()
    serialised = handle.SerializeToString()
    restored = types_pb2.Handle()
    restored.ParseFromString(serialised)
    # Both should be default/empty messages — equal after round-trip
    assert handle == restored


def test_gen_v1_alias_roundtrip() -> None:
    """Construct and round-trip a nominal.gen.v1.Alias message."""
    from nominal_api_protos.nominal.gen.v1 import alias_pb2

    alias = alias_pb2.Alias(java_name="com.example.Foo")
    serialised = alias.SerializeToString()
    restored = alias_pb2.Alias()
    restored.ParseFromString(serialised)
    assert restored.java_name == "com.example.Foo"


def test_conjure_v1_compat_roundtrip() -> None:
    """Construct and round-trip a nominal.conjure.v1.ConjureCompatibility message."""
    from nominal_api_protos.nominal.conjure.v1 import compat_pb2

    compat = compat_pb2.ConjureCompatibility()
    serialised = compat.SerializeToString()
    restored = compat_pb2.ConjureCompatibility()
    restored.ParseFromString(serialised)
    assert compat == restored


# ---------------------------------------------------------------------------
# Groups service tests
# ---------------------------------------------------------------------------


def test_groups_pb2_import() -> None:
    """Groups service proto is deep in the package hierarchy and exercises import rewriting."""
    from nominal_api_protos.nominal.authentication.groups.v1 import groups_pb2  # noqa: F401

    assert groups_pb2.DESCRIPTOR is not None


def test_groups_pb2_grpc_import() -> None:
    """Groups gRPC stubs must import cleanly (exercises _pb2_grpc.py import rewriting)."""
    from nominal_api_protos.nominal.authentication.groups.v1 import groups_pb2_grpc  # noqa: F401

    assert groups_pb2_grpc.GroupServiceStub is not None


def test_group_message_roundtrip() -> None:
    """Construct a Group message with nested Symbol (from nominal.scout.elements.v1)
    and verify it survives a serialise/deserialise round-trip.

    This exercises cross-package references — Group.symbol is typed as
    nominal.scout.elements.v1.Symbol, which requires correct import rewriting in
    both the groups and elements proto modules."""
    from nominal_api_protos.nominal.authentication.groups.v1 import groups_pb2
    from nominal_api_protos.nominal.scout.elements.v1.elements_pb2 import Symbol

    symbol = Symbol(emoji="🔑")
    group = groups_pb2.Group(
        rid="ri.groups.0.group.abc123",
        group_id="eng-team",
        display_name="Engineering",
        description="The engineering team",
        symbol=symbol,
    )

    serialised = group.SerializeToString()
    restored = groups_pb2.Group()
    restored.ParseFromString(serialised)

    assert restored.rid == "ri.groups.0.group.abc123"
    assert restored.group_id == "eng-team"
    assert restored.display_name == "Engineering"
    assert restored.description == "The engineering team"
    assert restored.symbol.emoji == "🔑"


def test_search_groups_request_roundtrip() -> None:
    """Construct a SearchGroupsRequest with a text query and round-trip it."""
    from nominal_api_protos.nominal.authentication.groups.v1 import groups_pb2

    query = groups_pb2.SearchGroupsQuery(exact_substring_text="eng")
    request = groups_pb2.SearchGroupsRequest(page_size=25, query=query)

    serialised = request.SerializeToString()
    restored = groups_pb2.SearchGroupsRequest()
    restored.ParseFromString(serialised)

    assert restored.page_size == 25
    assert restored.query.exact_substring_text == "eng"


def test_search_groups_response_roundtrip() -> None:
    """Construct a SearchGroupsResponse with multiple Group results and round-trip it."""
    from nominal_api_protos.nominal.authentication.groups.v1 import groups_pb2

    groups = [
        groups_pb2.Group(rid=f"ri.groups.0.group.{i}", display_name=f"Team {i}")
        for i in range(3)
    ]
    response = groups_pb2.SearchGroupsResponse(results=groups, next_page_token="tok_xyz")

    serialised = response.SerializeToString()
    restored = groups_pb2.SearchGroupsResponse()
    restored.ParseFromString(serialised)

    assert len(restored.results) == 3
    assert restored.results[1].display_name == "Team 1"
    assert restored.next_page_token == "tok_xyz"


def test_update_group_metadata_request_roundtrip() -> None:
    """Construct an UpdateGroupMetadataRequestWrapper with nested request and round-trip it."""
    from nominal_api_protos.nominal.authentication.groups.v1 import groups_pb2
    from nominal_api_protos.nominal.scout.elements.v1.elements_pb2 import Symbol

    symbol_wrapper = groups_pb2.UpdateGroupMetadataRequest.UpdateGroupSymbolWrapper(
        value=Symbol(icon="star")
    )
    inner = groups_pb2.UpdateGroupMetadataRequest(
        display_name="Renamed Team",
        description="Updated description",
        symbol=symbol_wrapper,
    )
    wrapper = groups_pb2.UpdateGroupMetadataRequestWrapper(
        group_rid="ri.groups.0.group.abc123",
        request=inner,
    )

    serialised = wrapper.SerializeToString()
    restored = groups_pb2.UpdateGroupMetadataRequestWrapper()
    restored.ParseFromString(serialised)

    assert restored.group_rid == "ri.groups.0.group.abc123"
    assert restored.request.display_name == "Renamed Team"
    assert restored.request.description == "Updated description"
    assert restored.request.symbol.value.icon == "star"


def test_get_groups_request_roundtrip() -> None:
    """Construct a GetGroupsRequest with multiple RIDs and round-trip it."""
    from nominal_api_protos.nominal.authentication.groups.v1 import groups_pb2

    rids = ["ri.groups.0.group.1", "ri.groups.0.group.2", "ri.groups.0.group.3"]
    request = groups_pb2.GetGroupsRequest(group_rids=rids)

    serialised = request.SerializeToString()
    restored = groups_pb2.GetGroupsRequest()
    restored.ParseFromString(serialised)

    assert list(restored.group_rids) == rids


def test_search_groups_compound_query_roundtrip() -> None:
    """Construct a compound AND query and verify the oneof semantics work correctly."""
    from nominal_api_protos.nominal.authentication.groups.v1 import groups_pb2

    q1 = groups_pb2.SearchGroupsQuery(exact_substring_text="eng")
    q2 = groups_pb2.SearchGroupsQuery(exact_substring_text="team")
    # 'and' is a Python keyword so protobuf exposes it via getattr
    and_query = groups_pb2.SearchQueryAnd(queries=[q1, q2])
    compound = groups_pb2.SearchGroupsQuery()
    getattr(compound, "and").CopyFrom(and_query)

    serialised = compound.SerializeToString()
    restored = groups_pb2.SearchGroupsQuery()
    restored.ParseFromString(serialised)

    assert len(getattr(restored, "and").queries) == 2
    assert getattr(restored, "and").queries[0].exact_substring_text == "eng"
    assert getattr(restored, "and").queries[1].exact_substring_text == "team"
