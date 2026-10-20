from __future__ import annotations

import json
from unittest.mock import MagicMock
from uuid import UUID, uuid5

import pytest
from conjure_python_client import ConjureEncoder
from nominal_api import event, scout_api, scout_checks_api, scout_versioning_api

from nominal.core import Checklist, EventType, NominalClient, Priority, Workspace
from nominal.experimental.compute_as_code import ChannelBinding, CheckDefinition, ChecklistBuilder
from nominal.protos.authentication.users.v1 import users_pb2
from nominal.protos.datareview.v2 import data_review_pb2
from nominal.protos.run.v1 import run_service_pb2


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
    ranges = "(temperature > 100).ranges(min_duration=Duration.Seconds(2))"
    builder = ChecklistBuilder()
    assert (
        builder.add(
            CheckDefinition(
                "Overheating", ranges, priority=Priority.P0, description="Temperature exceeds the limit for two seconds"
            ),
            inputs={"temperature": ChannelBinding("telemetry", "engine.temperature", tags={"sensor": "left"})},
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
    assert check.generated_event_type is None
    assert check.generated_event_labels is None
    assert check.priority == scout_api.Priority.P0
    assert check.title == "Overheating"
    assert check.description == "Temperature exceeds the limit for two seconds"
    condition = check.condition.num_ranges_v3
    wire = json.loads(ConjureEncoder().encode(request.checklist_variables[-1].value.compute_node.series_node.ranges))
    assert wire["type"] == "booleanRanges"
    boolean_ranges = wire["booleanRanges"]
    assert boolean_ranges["minDuration"]["seconds"]["seconds"]["literal"] == 2
    comparison = boolean_ranges["input"]["greaterThan"]
    assert comparison["left"]["raw"]["name"] == "temperature"
    assert comparison["right"]["constant"]["double"]["literal"] == 100
    assert check.condition.parameterized_num_ranges_v1 is None
    assert condition.threshold == 0
    assert condition.operator.value == "GREATER_THAN"
    assert condition.function_spec == {}
    assert condition.compute_expression is None
    locator = request.checklist_variables[0].value.compute_node.context.variables["raw"].series
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
    ranges = "(temperature > 100).ranges()"
    builder = ChecklistBuilder()
    for sensor in ("left", "right"):
        tags["sensor"] = sensor
        builder.add(CheckDefinition(f"{sensor} overheating", ranges, priority=Priority.P1), inputs=inputs)
    tags.clear()
    inputs.clear()
    builder.create(client, "Engine", assignee="user")
    request = mock_clients.checklist.create.call_args.args[1]
    checks = request.checks
    assert [entry.create_check.title for entry in checks] == ["left overheating", "right overheating"]
    assert [
        variable.value.compute_node.context.variables["raw"].series.tags
        for variable in request.checklist_variables
        if variable.compute_expression is None
    ] == [{"sensor": "left"}, {"sensor": "right"}]


def test_create_keeps_builder_reusable_without_mutating_prior_requests(
    client: NominalClient, mock_clients: MagicMock
) -> None:
    """Each creation takes the checks accumulated so far without consuming or aliasing the builder list."""
    ranges = "(x > 0).ranges()"
    builder = ChecklistBuilder().add(
        CheckDefinition("First", ranges, priority=Priority.P1), inputs={"x": ChannelBinding("scope", "x")}
    )
    builder.create(client, "First version", assignee="user")
    first = mock_clients.checklist.create.call_args.args[1]
    builder.add(CheckDefinition("Second", ranges, priority=Priority.P2), inputs={"x": ChannelBinding("scope", "y")})
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
        ChecklistBuilder().add(
            CheckDefinition("Wrong type", "NumericSeries.Constant(1)", priority=Priority.P1), inputs={}
        )


def test_explicit_workspace_and_default_assignee(client: NominalClient, mock_clients: MagicMock) -> None:
    """Use the authenticated user and explicit workspace without consulting the default workspace."""
    mock_clients.users.GetCurrentUser.return_value = users_pb2.GetCurrentUserResponse(
        user=users_pb2.User(rid="current-user", display_name="Test User", email="user@example.com")
    )
    ChecklistBuilder().add(
        CheckDefinition("Check", "(x > 0).ranges()", priority=Priority.P1), inputs={"x": ChannelBinding("scope", "x")}
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
        builder.add(
            CheckDefinition(
                "Overheating", "(NumericSeries.Reference('temperature_deg') > 100).ranges()", priority=Priority.P1
            ),
            inputs={"temperature": ChannelBinding("telemetry", "temperature")},
        )
    with pytest.raises(ValueError, match="at least one check"):
        builder.create(client, "Empty")


def test_shared_bindings_allowed_by_default(client: NominalClient, mock_clients: MagicMock) -> None:
    """Unused shared bindings never become dependencies in the published check."""
    builder = ChecklistBuilder().add(
        CheckDefinition("Overheating", "(temperature + temperature > 100).ranges()", priority=Priority.P1),
        inputs={
            "temperature": ChannelBinding("telemetry", "temperature"),
            "pressure": ChannelBinding("telemetry", "pressure"),
        },
    )
    builder.create(client, "Engine", assignee="user")
    assert set(
        mock_clients.checklist.create.call_args.args[1].checklist_variables[-1].value.compute_node.context.variables
    ) == {"temperature"}


def test_strict_rejects_unused_bindings() -> None:
    """Strict mode identifies bindings unused by an otherwise fully bound expression."""
    with pytest.raises(ValueError, match="Overheating.*unused input bindings.*pressure"):
        ChecklistBuilder(strict=True).add(
            CheckDefinition("Overheating", "(temperature > 100).ranges()", priority=Priority.P1),
            inputs={
                "temperature": ChannelBinding("telemetry", "temperature"),
                "pressure": ChannelBinding("telemetry", "pressure"),
            },
        )


CHECK_ID = uuid5(UUID("52e0d79d-a67f-4d15-8cb6-a294f8c64b4e"), "first")
OTHER_CHECK_ID = uuid5(UUID("52e0d79d-a67f-4d15-8cb6-a294f8c64b4e"), "second")


def test_create_preserves_explicit_check_identity(client: NominalClient, mock_clients: MagicMock) -> None:
    """An identity in source is used on the initial deployment and the returned commit can be pinned."""
    checklist = (
        ChecklistBuilder()
        .add(
            CheckDefinition("Overheating", "(temperature > 100).ranges()", key="first", priority=Priority.P1),
            inputs={"temperature": ChannelBinding("scope", "temperature")},
        )
        .create(client, "Engine", assignee="user")
    )
    request = mock_clients.checklist.create.call_args.args[1]
    assert request.checks[0].create_check.check_lineage_rid == str(CHECK_ID)
    assert checklist.commit_id == "commit-id"


def test_commit_replaces_definition_and_refreshes_in_place(client: NominalClient, mock_clients: MagicMock) -> None:
    """A replacement removes omitted checks, preserves renamed/reordered lineage, and checks the loaded version."""
    original = mock_clients.checklist.create.return_value
    checklist = Checklist._from_conjure(mock_clients, original)
    response = scout_checks_api.VersionedChecklist(
        rid=original.rid,
        metadata=original.metadata,
        checks=[],
        checklist_variables=[],
        commit=scout_versioning_api.Commit(
            id="new-commit",
            committed_at="2026-01-02T00:00:00Z",
            committed_by="author-rid",
            is_working_state=False,
            message="Raise limit",
            resource_rid=original.rid,
        ),
    )
    mock_clients.checklist.commit.return_value = response
    ranges = "(temperature > 110).ranges()"
    builder = ChecklistBuilder()
    for key, name in [("second", "Renamed second"), ("first", "Renamed first")]:
        builder.add(
            CheckDefinition(name, ranges, key=key, priority=Priority.P2),
            inputs={"temperature": ChannelBinding("scope", "temperature")},
        )
    updated = builder.commit(checklist, commit_message="Raise limit")
    auth, rid, request = mock_clients.checklist.commit.call_args.args
    assert auth == "Bearer test-token"
    assert rid == checklist.rid
    assert request.latest_commit == "commit-id"
    assert request.commit_message == "Raise limit"
    assert len(request.checklist_variables) == 4
    assert request.checklist_variables[1].compute_expression.v1.python.expression == ranges
    assert request.checklist_variables[0].value.compute_node.context.variables["raw"].series.channel == "temperature"
    assert [entry.create_check.check_lineage_rid for entry in request.checks] == [str(OTHER_CHECK_ID), str(CHECK_ID)]
    assert [entry.create_check.title for entry in request.checks] == ["Renamed second", "Renamed first"]
    assert updated is checklist
    assert updated.rid == checklist.rid
    assert updated.commit_id == "new-commit"
    assert checklist.commit_id == "new-commit"
    assert updated.properties == {"source": "server"}
    assert updated.labels == ["server-label"]
    mock_clients.run.GetRun.return_value = run_service_pb2.GetRunResponse(
        run=run_service_pb2.Run(rid="run-rid", assets=["asset-rid"])
    )
    mock_clients.datareview.BatchInitiate.return_value = data_review_pb2.BatchInitiateResponse()
    client.data_review_builder().execute_checklist(
        "run-rid", updated, commit=updated.commit_id, asset="asset-rid"
    ).initiate(wait_for_completion=False)
    execution = mock_clients.datareview.BatchInitiate.call_args.args[0].requests[0]
    assert execution.checklist_rid == updated.rid
    assert execution.commit == "new-commit"
    assert execution.asset_rid == "asset-rid"
    mock_clients.checklist.get.assert_not_called()
    mock_clients.checklist.update_metadata.assert_not_called()
    mock_clients.checklist_execution.reload_streaming_checklist.assert_not_called()


def test_commit_accepts_automatic_identities(client: NominalClient, mock_clients: MagicMock) -> None:
    """A full replacement accepts automatically generated check identities."""
    checklist = Checklist._from_conjure(mock_clients, mock_clients.checklist.create.return_value)
    builder = ChecklistBuilder().add(
        CheckDefinition("Anonymous", "(x > 0).ranges()", priority=Priority.P1),
        inputs={"x": ChannelBinding("scope", "x")},
    )
    mock_clients.checklist.commit.return_value = mock_clients.checklist.create.return_value
    builder.commit(checklist, commit_message="Update")
    request = mock_clients.checklist.commit.call_args.args[2]
    assert request.checks[0].create_check.check_lineage_rid == builder._checks[0].check_lineage_rid
    assert UUID(request.checks[0].create_check.check_lineage_rid).version == 4


def test_commit_requires_nonempty_definition(client: NominalClient, mock_clients: MagicMock) -> None:
    """An empty builder cannot accidentally clear the checklist."""
    checklist = Checklist._from_conjure(mock_clients, mock_clients.checklist.create.return_value)
    with pytest.raises(ValueError, match="at least one check"):
        ChecklistBuilder().commit(checklist, commit_message="Empty")
    mock_clients.checklist.commit.assert_not_called()


def test_commit_requires_loaded_version(mock_clients: MagicMock) -> None:
    """Manually constructed resources without a version cannot perform an unconditional overwrite."""
    checklist = Checklist(
        rid="checklist-rid", name="Checks", description="", properties={}, labels=[], _clients=mock_clients
    )
    builder = ChecklistBuilder().add(
        CheckDefinition("Check", "(x > 0).ranges()", key="first", priority=Priority.P1),
        inputs={"x": ChannelBinding("scope", "x")},
    )
    with pytest.raises(ValueError, match="refresh"):
        builder.commit(checklist, commit_message="Update")
    mock_clients.checklist.commit.assert_not_called()


def test_source_survives_create_and_full_replacement(client: NominalClient, mock_clients: MagicMock) -> None:
    """Source and stable variable identities survive creation and full-definition replacement."""
    sources = ["(temperature > 100).ranges()", "(temperature > 180).ranges()"]
    builders = [
        ChecklistBuilder().add(
            CheckDefinition("Hot", source, key="thermal", priority=Priority.P1),
            inputs={"temperature": ChannelBinding("telemetry", "temperature")},
        )
        for source in sources
    ]
    checklist = builders[0].create(client, "Thermal")
    initial = mock_clients.checklist.create.call_args.args[1]
    mock_clients.checklist.commit.return_value = mock_clients.checklist.create.return_value
    builders[1].commit(checklist, commit_message="Raise threshold")
    replacement = mock_clients.checklist.commit.call_args.args[2]
    assert replacement.latest_commit == "commit-id"
    assert [v.name for v in initial.checklist_variables] == [v.name for v in replacement.checklist_variables]
    for request, source in zip([initial, replacement], sources):
        assert request.checklist_variables[-1].compute_expression.v1.python.expression == source
        assert (
            request.checks[0].create_check.condition.num_ranges_v3.variables["failure"].checklist_variable
            == request.checklist_variables[-1].name
        )


@pytest.mark.parametrize("event_type", [EventType.INFO, EventType.FLAG, EventType.ERROR, EventType.SUCCESS])
def test_event_settings_captured_on_add(client: NominalClient, mock_clients: MagicMock, event_type: EventType) -> None:
    """Capture event labels independently of later caller edits and preserve the selected event category."""
    labels = ["thermal"]
    builder = ChecklistBuilder().add(
        CheckDefinition(
            "Temperature",
            "(x > 100).ranges()",
            priority=Priority.P1,
            event_type=event_type,
            event_labels=labels,
        ),
        inputs={"x": ChannelBinding("telemetry", "temperature")},
    )
    labels.clear()
    builder.create(client, "Checks", assignee="user")
    check = mock_clients.checklist.create.call_args.args[1].checks[0].create_check
    assert check.generated_event_type == event.EventType(event_type.value)
    assert check.generated_event_labels == ["thermal"]


def test_unknown_event_type_rejected_before_add(client: NominalClient) -> None:
    """The read-side unknown sentinel cannot be published as an event category."""
    builder = ChecklistBuilder()
    with pytest.raises(ValueError, match="UNKNOWN"):
        builder.add(
            CheckDefinition("Invalid", "(x > 0).ranges()", priority=Priority.P1, event_type=EventType.UNKNOWN),
            inputs={"x": ChannelBinding("telemetry", "temperature")},
        )
    with pytest.raises(ValueError, match="at least one check"):
        builder.create(client, "Empty")
