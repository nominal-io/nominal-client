from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest
from conjure_python_client import ConjureEncoder
from nominal_api import scout_api, scout_checks_api, scout_versioning_api

import nominal.compute as nc
from nominal.core import NominalClient, Priority, Workspace
from nominal.experimental.compute_as_code import ChannelBinding, ChecklistBuilder
from nominal.protos.authentication.users.v1 import users_pb2


@pytest.fixture
def client(mock_clients: MagicMock) -> NominalClient:
    """Use a real client and generated response around the mocked service boundary."""
    mock_clients.resolve_default_workspace_rid.return_value = "workspace-rid"
    mock_clients.checklist.create.return_value = scout_checks_api.VersionedChecklist(
        rid="checklist-rid",
        checklist_variables=[],
        checks=[],
        metadata=scout_checks_api.ChecklistMetadata(
            title="Server title",
            description="Server description",
            author_rid="author-rid",
            assignee_rid="assignee-rid",
            created_at="2026-01-01T00:00:00Z",
            is_archived=False,
            is_published=True,
            labels=["server-label"],
            properties={"source": "server"},
        ),
        commit=scout_versioning_api.Commit(
            id="commit-id",
            committed_at="2026-01-01T00:00:00Z",
            committed_by="author-rid",
            is_working_state=False,
            message="Initial checks",
            resource_rid="checklist-rid",
        ),
    )
    return NominalClient(mock_clients)


def test_create_serializes_failure_ranges_and_relative_bindings(client: NominalClient, mock_clients: MagicMock) -> None:
    """Preserve expression persistence, P0, relative scope, and tags in a published checklist."""
    ranges = (nc.NumericSeries.Reference("temperature") > 100).ranges(min_duration=nc.Duration.Seconds(2))
    builder = ChecklistBuilder()
    assert (
        builder.add_check(
            "Overheating",
            ranges,
            inputs={"temperature": ChannelBinding("telemetry", "engine.temperature", tags={"sensor": "left"})},
            priority=Priority.P0,
            description="Temperature exceeds the limit for two seconds",
        )
        is builder
    )
    mock_clients.checklist.create.assert_not_called()

    result = builder.create(
        client,
        "Engine acceptance",
        assignee="assignee-rid",
        description="Review engine telemetry",
        commit_message="Initial checks",
        labels=["acceptance"],
        properties={"engine": "demo"},
    )

    auth, request = mock_clients.checklist.create.call_args.args
    assert auth == "Bearer test-token"
    assert request.is_published is True
    assert request.workspace == "workspace-rid"
    assert request.title == "Engine acceptance"
    assert request.assignee_rid == "assignee-rid"
    assert request.description == "Review engine telemetry"
    assert request.commit_message == "Initial checks"
    assert request.labels == ["acceptance"]
    assert request.properties == {"engine": "demo"}
    check = request.checks[0].create_check
    assert check.priority == scout_api.Priority.P0
    assert check.title == "Overheating"
    assert check.description == "Temperature exceeds the limit for two seconds"
    condition = check.condition.parameterized_num_ranges_v1
    wire = json.loads(ConjureEncoder().encode(condition.ranges))
    assert wire["type"] == "booleanRanges"
    boolean_ranges = wire["booleanRanges"]
    assert boolean_ranges["minDuration"]["seconds"]["seconds"]["literal"] == 2
    comparison = boolean_ranges["input"]["greaterThan"]
    assert comparison["left"]["raw"]["name"] == "temperature"
    assert comparison["right"]["constant"]["double"]["literal"] == 100
    assert condition.compute_expression is None
    locator = condition.implementations[0].variables["temperature"].series
    assert locator.data_source_ref == "telemetry"
    assert locator.channel == "engine.temperature"
    assert locator.tags == {"sensor": "left"}
    assert locator.run is None and locator.asset is None
    assert result.rid == "checklist-rid"
    assert result.name == "Server title"
    assert result.description == "Server description"
    assert result.labels == ["server-label"]
    assert result.properties == {"source": "server"}


def test_checks_snapshot_shared_bindings(client: NominalClient, mock_clients: MagicMock) -> None:
    """Each loop iteration captures its channel tags before the caller reuses the mapping."""
    tags = {"sensor": "left"}
    inputs = {"temperature": ChannelBinding("telemetry", "temperature", tags=tags)}
    ranges = (nc.NumericSeries.Reference("temperature") > 100).ranges()
    builder = ChecklistBuilder()
    for sensor in ("left", "right"):
        tags["sensor"] = sensor
        builder.add_check(f"{sensor} overheating", ranges, inputs=inputs, priority=Priority.P1)
    tags.clear()
    inputs.clear()
    builder.create(client, "Engine", assignee="user")

    checks = mock_clients.checklist.create.call_args.args[1].checks
    assert [entry.create_check.title for entry in checks] == ["left overheating", "right overheating"]
    assert [
        entry.create_check.condition.parameterized_num_ranges_v1.implementations[0].variables["temperature"].series.tags
        for entry in checks
    ] == [{"sensor": "left"}, {"sensor": "right"}]


def test_create_keeps_builder_reusable_without_mutating_prior_requests(
    client: NominalClient, mock_clients: MagicMock
) -> None:
    """Each creation takes the checks accumulated so far without consuming or aliasing the builder list."""
    ranges = (nc.NumericSeries.Reference("x") > 0).ranges()
    builder = ChecklistBuilder().add_check(
        "First", ranges, inputs={"x": ChannelBinding("scope", "x")}, priority=Priority.P1
    )
    builder.create(client, "First version", assignee="user")
    first = mock_clients.checklist.create.call_args.args[1]
    builder.add_check("Second", ranges, inputs={"x": ChannelBinding("scope", "y")}, priority=Priority.P2)
    builder.create(client, "Second version", assignee="user")
    second = mock_clients.checklist.create.call_args.args[1]
    assert [entry.create_check.title for entry in first.checks] == ["First"]
    assert [entry.create_check.title for entry in second.checks] == ["First", "Second"]


def test_empty_builder_rejected_before_network(client: NominalClient, mock_clients: MagicMock) -> None:
    """Creating an empty checklist fails locally without a user lookup or creation request."""
    with pytest.raises(ValueError, match="at least one check"):
        ChecklistBuilder().create(client, "Empty")
    assert not mock_clients.mock_calls


def test_non_range_expression_rejected() -> None:
    """Series cannot accidentally be interpreted as a checklist's failure ranges."""
    with pytest.raises(TypeError, match="RangeSeries"):
        ChecklistBuilder().add_check("Wrong type", nc.NumericSeries.Reference("x"), inputs={}, priority=Priority.P1)


def test_explicit_workspace_and_default_assignee(client: NominalClient, mock_clients: MagicMock) -> None:
    """Use the authenticated user and explicit workspace without consulting the default workspace."""
    mock_clients.users.GetCurrentUser.return_value = users_pb2.GetCurrentUserResponse(
        user=users_pb2.User(rid="current-user", display_name="Test User", email="user@example.com")
    )
    ChecklistBuilder().add_check(
        "Check",
        (nc.NumericSeries.Reference("x") > 0).ranges(),
        inputs={"x": ChannelBinding("scope", "x")},
        priority=Priority.P1,
    ).create(client, "Checks", workspace=Workspace(rid="explicit-workspace", id="test", org="test"))
    request = mock_clients.checklist.create.call_args.args[1]
    assert request.workspace == "explicit-workspace"
    assert request.assignee_rid == "current-user"
    mock_clients.resolve_default_workspace_rid.assert_not_called()


@pytest.mark.parametrize("strict", [False, True])
def test_missing_bindings_rejected_before_adding_check(strict: bool, client: NominalClient) -> None:
    """A misspelled binding fails locally in either mode and leaves the builder empty."""
    builder = ChecklistBuilder(strict=strict)
    with pytest.raises(ValueError, match="Overheating.*missing input bindings.*temperature_deg"):
        builder.add_check(
            "Overheating",
            (nc.NumericSeries.Reference("temperature_deg") > 100).ranges(),
            inputs={"temperature": ChannelBinding("telemetry", "temperature")},
            priority=Priority.P1,
        )
    with pytest.raises(ValueError, match="at least one check"):
        builder.create(client, "Empty")


def test_shared_bindings_allowed_by_default(client: NominalClient, mock_clients: MagicMock) -> None:
    """Unused shared bindings never become dependencies in the published check."""
    temperature = nc.NumericSeries.Reference("temperature")
    builder = ChecklistBuilder().add_check(
        "Overheating",
        (temperature + temperature > 100).ranges(),
        inputs={
            "temperature": ChannelBinding("telemetry", "temperature"),
            "pressure": ChannelBinding("telemetry", "pressure"),
        },
        priority=Priority.P1,
    )

    builder.create(client, "Engine", assignee="user")
    condition = mock_clients.checklist.create.call_args.args[1].checks[0].create_check.condition
    assert set(condition.parameterized_num_ranges_v1.implementations[0].variables) == {"temperature"}


def test_strict_rejects_unused_bindings() -> None:
    """Strict mode identifies bindings unused by an otherwise fully bound expression."""
    with pytest.raises(ValueError, match="Overheating.*unused input bindings.*pressure"):
        ChecklistBuilder(strict=True).add_check(
            "Overheating",
            (nc.NumericSeries.Reference("temperature") > 100).ranges(),
            inputs={
                "temperature": ChannelBinding("telemetry", "temperature"),
                "pressure": ChannelBinding("telemetry", "pressure"),
            },
            priority=Priority.P1,
        )


@pytest.mark.parametrize(
    "ranges, missing",
    [
        (
            nc.RangeSeries.Union(
                [(nc.NumericSeries.Reference("x") > 0).ranges(), (nc.NumericSeries.Reference("y") > 0).ranges()]
            ),
            "y",
        ),
        (
            (
                nc.NumericSeries.Reference("x")
                > nc.NumericSeries.Constant(nc.ConstantNumericSeries.Float(nc.FloatConstant.Reference("limit")))
            ).ranges(),
            "limit",
        ),
        ((nc.NumericSeries.Reference("x") > 0).ranges(min_duration=nc.Duration.Reference("duration")), "duration"),
        (nc.RangeSeries.Reference("ranges"), "ranges"),
    ],
)
def test_nested_and_non_numeric_references_require_bindings(ranges: nc.RangeSeries, missing: str) -> None:
    """Traverse list operands and reference encodings beyond numeric series leaves."""
    with pytest.raises(ValueError, match=f"missing input bindings.*{missing}"):
        ChecklistBuilder().add_check("Check", ranges, inputs={"x": ChannelBinding("scope", "x")}, priority=Priority.P1)


def test_literal_channel_name_is_not_an_input_reference() -> None:
    """Explicit dataset selectors need no bindings for their literal channel and dataset names."""
    ranges = (nc.NumericSeries.SelectNumeric("temperature", dataset=nc.Dataset.Saved("dataset-rid")) > 0).ranges()
    ChecklistBuilder(strict=True).add_check("Check", ranges, inputs={}, priority=Priority.P1)
