import warnings
from datetime import UTC, datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest
from nominal_api import scout_checklistexecution_api as api
from nominal_api import scout_checks_api, scout_integrations_api, scout_run_api, scout_versioning_api

from nominal.core import Asset, Checklist, Integration, NominalClient, WorkspaceSearchType
from nominal.core import streaming_checklist as streaming
from nominal.protos.asset.v2 import asset_pb2
from nominal.protos.integrations.v1 import integrations_pb2


@pytest.fixture
def checklist():
    return Checklist(rid="checklist", name="Monitor", description="", properties={}, labels=[], _clients=MagicMock())


def _versioned_checklist(message: str, commit_id: str = "commit") -> scout_checks_api.VersionedChecklist:
    return scout_checks_api.VersionedChecklist(
        rid="checklist",
        checklist_variables=[],
        checks=[],
        metadata=scout_checks_api.ChecklistMetadata(
            asset_type_rids=[],
            title="Monitor",
            description="",
            author_rid="author",
            assignee_rid="assignee",
            created_at="2026-01-01T00:00:00Z",
            is_archived=False,
            is_published=True,
            labels=[],
            properties={},
        ),
        commit=scout_versioning_api.Commit(
            id=commit_id,
            committed_at="2026-01-01T00:00:00Z",
            committed_by="author",
            is_working_state=False,
            message=message,
            resource_rid="checklist",
        ),
    )


def test_notification_configuration_preserves_options(checklist):
    """Typed destinations preserve tags, workbook, timezone and execution-error suppression."""
    integration = Integration._from_proto(MagicMock(), integrations_pb2.Integration(rid="integration"))
    until = datetime(2030, 1, 1, 12, 0, 0, 123456, tzinfo=timezone(timedelta(hours=2)))
    config = streaming.StreamingNotificationConfig(
        integration=integration, tags=["flight"], workbook="workbook", mute_until=until, mute_execution_errors=True
    )
    checklist.execute_streaming(
        ["asset"], notifications=[config], evaluation_delay=1_000_000_001, auto_create_events=True
    )
    request = checklist._clients.checklist_execution.execute_streaming_checklist.call_args.args[1]
    notification = request.notification_configurations[0]
    assert notification.integration_rid == "integration"
    assert notification.tags == ["flight"]
    assert notification.appended_workbook_rid == "workbook"
    assert datetime.fromisoformat(notification.mute_until) == until
    assert notification.notification_filters == [scout_integrations_api.NotificationFilter.EXECUTION_ERROR]
    assert (request.evaluation_delay.seconds, request.evaluation_delay.nanos) == (1, 1)
    assert request.auto_create_events is True


@pytest.mark.parametrize("keyword", [False, True])
@pytest.mark.parametrize("integrations", [[], ["one", "two"]])
def test_existing_rid_calls_keep_their_behavior(checklist, integrations, keyword):
    """Legacy positional and keyword RID calls warn at the caller while preserving destinations and defaults."""
    with pytest.warns(UserWarning, match="integration_rids.*deprecated.*notifications") as caught:
        if keyword:
            checklist.execute_streaming(["asset"], integration_rids=integrations)
        else:
            checklist.execute_streaming(["asset"], integrations)
    assert len(caught) == 1
    assert caught[0].filename == __file__
    request = checklist._clients.checklist_execution.execute_streaming_checklist.call_args.args[1]
    assert [n.integration_rid for n in request.notification_configurations] == integrations
    assert all(n.tags == [] for n in request.notification_configurations)
    assert request.recovery_delay.seconds == 15
    assert request.auto_create_events is False


def test_conflicting_notification_arguments_fail_before_request(checklist):
    """Two notification inputs are rejected even when explicitly empty."""
    with pytest.raises(ValueError, match="integration_rids.*notifications"):
        checklist.execute_streaming(["asset"], [], notifications=[])
    checklist._clients.checklist_execution.execute_streaming_checklist.assert_not_called()


def test_naive_mute_time_is_rejected(checklist):
    """Mute deadlines require a timezone rather than silently using the caller's local zone."""
    with pytest.raises(ValueError, match="timezone"):
        checklist.execute_streaming(
            ["asset"],
            notifications=[streaming.StreamingNotificationConfig("integration", mute_until=datetime(2030, 1, 1))],
        )
    checklist._clients.checklist_execution.execute_streaming_checklist.assert_not_called()


def test_configuration_readback_can_be_reapplied(checklist):
    """Readback retains each asset's options and exact durations for a subsequent replacement."""
    raw = scout_integrations_api.NotificationConfiguration(
        integration_rid="integration",
        tags=["flight"],
        appended_workbook_rid="workbook",
        mute_until="2030-01-01T00:00:00.123456+00:00",
        notification_filters=[scout_integrations_api.NotificationFilter.EXECUTION_ERROR],
    )
    checklist._clients.checklist_execution.get_streaming_checklist.return_value = api.StreamingChecklistInfo(
        checklist_rid="checklist",
        asset_configurations={
            "asset": api.AssetStreamConfiguration(
                auto_create_events=True,
                evaluation_delay=scout_run_api.Duration(seconds=1, nanos=1),
                recovery_delay=scout_run_api.Duration(seconds=20, nanos=123),
                notification_configurations=[raw],
            ),
            "other": api.AssetStreamConfiguration(
                auto_create_events=False,
                evaluation_delay=scout_run_api.Duration(seconds=0, nanos=0),
                recovery_delay=scout_run_api.Duration(seconds=15, nanos=0),
                notification_configurations=[],
            ),
        },
    )
    configurations = checklist.get_streaming_configurations()
    config = configurations["asset"]
    assert config.evaluation_delay == 1_000_000_001
    assert config.recovery_delay == 20_000_000_123
    assert config.notifications[0].mute_until == datetime(2030, 1, 1, microsecond=123456, tzinfo=UTC)
    assert config.notifications[0].mute_execution_errors is True
    assert configurations["other"].notifications == ()
    raw.tags.append("later")
    assert config.notifications[0].tags == ("flight",)
    checklist.execute_streaming(
        ["asset"],
        notifications=config.notifications,
        evaluation_delay=config.evaluation_delay,
        recovery_delay=config.recovery_delay,
        auto_create_events=config.auto_create_events,
    )
    request = checklist._clients.checklist_execution.execute_streaming_checklist.call_args.args[1]
    assert request.notification_configurations[0].tags == ["flight"]
    assert request.recovery_delay.nanos == 123
    assert request.auto_create_events is True


def test_live_status_keeps_state_commit_and_parameter_indices(checklist):
    """Status readback distinguishes lifecycle states and preserves check identities including index zero."""
    variants = {
        "running": api.ChecklistLiveStatus(
            running=api.StreamingChecklistRunning(
                commit_id="commit",
                check_results=[
                    api.CheckLiveStatusResponse(
                        check_rid="check", check_parameter_index=0, status=api.CheckStatus(pass_=api.Pass())
                    ),
                    api.CheckLiveStatusResponse(
                        check_rid="check", check_parameter_index=1, status=api.CheckStatus(skipped=api.Skipped())
                    ),
                    api.CheckLiveStatusResponse(check_rid="other", status=api.CheckStatus(invalid=api.Invalid())),
                    api.CheckLiveStatusResponse(check_rid="failure", status=api.CheckStatus(fail=api.Fail())),
                ],
            )
        ),
        "initializing": api.ChecklistLiveStatus(initializing=api.StreamingChecklistInitializing()),
        "failed": api.ChecklistLiveStatus(failed=api.StreamingChecklistFailed()),
    }
    checklist._clients.checklist_execution.checklist_live_status.return_value = api.BatchChecklistLiveStatusResponse(
        checklist_live_status_responses=[
            api.ChecklistLiveStatusResponse(
                asset_rid=asset_rid,
                checklist_rid="checklist",
                status=status,
            )
            for asset_rid, status in variants.items()
        ]
    )
    statuses = checklist.get_streaming_status(list(variants))
    assert {asset: status.state.value for asset, status in statuses.items()} == {
        "running": "running",
        "initializing": "initializing",
        "failed": "failed",
    }
    assert statuses["running"].commit_id == "commit"
    assert [(c.check_rid, c.parameter_index, c.state.value) for c in statuses["running"].checks] == [
        ("check", 0, "pass"),
        ("check", 1, "skipped"),
        ("other", None, "invalid"),
        ("failure", None, "fail"),
    ]
    assert statuses["initializing"].commit_id is None
    assert statuses["initializing"].checks == ()
    assert statuses["failed"].commit_id is None
    assert statuses["failed"].checks == ()


def test_empty_asset_reads_do_not_request_status(checklist):
    """Empty read batches are local no-ops rather than invalid service requests."""
    assert checklist.get_streaming_status([]) == {}
    assert checklist._clients.checklist_execution.mock_calls == []


def test_default_notification_readback_and_asset_objects(checklist):
    """Unconfigured optional notification fields stay absent and asset objects normalize to RIDs."""
    config = streaming.StreamingNotificationConfig._from_conjure(
        scout_integrations_api.NotificationConfiguration(integration_rid="integration", tags=[])
    )
    assert config.mute_until is None
    assert config.workbook is None
    assert config.mute_execution_errors is False
    asset = Asset._from_proto(MagicMock(), asset_pb2.Asset(rid="asset"))
    checklist.execute_streaming([asset], notifications=[config])
    request = checklist._clients.checklist_execution.execute_streaming_checklist.call_args.args[1]
    assert request.assets == ["asset"]
    assert request.notification_configurations[0].appended_workbook_rid is None
    assert request.notification_configurations[0].mute_until is None
    assert request.notification_configurations[0].notification_filters == []


def test_status_reads_batch_at_the_service_limit(checklist):
    """More than 10,000 assets are split into bounded requests without losing either response."""
    checklist._clients.checklist_execution.checklist_live_status.side_effect = [
        api.BatchChecklistLiveStatusResponse(
            checklist_live_status_responses=[
                api.ChecklistLiveStatusResponse(
                    asset_rid="asset-0",
                    checklist_rid="checklist",
                    status=api.ChecklistLiveStatus(initializing=api.StreamingChecklistInitializing()),
                ),
            ]
        ),
        api.BatchChecklistLiveStatusResponse(
            checklist_live_status_responses=[
                api.ChecklistLiveStatusResponse(
                    asset_rid="asset-10000",
                    checklist_rid="checklist",
                    status=api.ChecklistLiveStatus(failed=api.StreamingChecklistFailed()),
                ),
            ]
        ),
    ]
    assets = [f"asset-{i}" for i in range(10_001)]
    statuses = checklist.get_streaming_status(assets)
    calls = checklist._clients.checklist_execution.checklist_live_status.call_args_list
    assert [len(call.args[1].requests) for call in calls] == [10_000, 1]
    assert [request.asset_rid for call in calls for request in call.args[1].requests] == assets
    assert statuses["asset-0"].state is streaming.StreamingChecklistState.INITIALIZING
    assert statuses["asset-10000"].state is streaming.StreamingChecklistState.FAILED


def test_mixed_notification_destinations_use_defaults_without_warnings(checklist):
    """Objects and RIDs use default settings while configured destinations retain their options."""
    integration = Integration._from_proto(MagicMock(), integrations_pb2.Integration(rid="object"))
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        checklist.execute_streaming(
            ["asset"],
            notifications=[integration, "rid", streaming.StreamingNotificationConfig("configured", tags=["tag"])],
        )
    request = checklist._clients.checklist_execution.execute_streaming_checklist.call_args.args[1]
    assert [n.integration_rid for n in request.notification_configurations] == ["object", "rid", "configured"]
    assert [n.tags for n in request.notification_configurations] == [[], [], ["tag"]]
    for notification in request.notification_configurations[:2]:
        assert notification.appended_workbook_rid is None
        assert notification.mute_until is None
        assert notification.notification_filters == []


@pytest.mark.parametrize("kwargs", [{}, {"integration_rids": None}, {"notifications": []}])
def test_omitted_notification_destinations_do_not_warn(checklist, kwargs):
    """Omitted legacy inputs and empty canonical inputs disable notifications without warnings."""
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        checklist.execute_streaming(["asset"], **kwargs)
    request = checklist._clients.checklist_execution.execute_streaming_checklist.call_args.args[1]
    assert request.notification_configurations == []


def test_get_checklist_keeps_the_commit_message_verbatim(mock_clients):
    """A loaded checklist exposes its version's message unchanged, including line breaks."""
    message = "Raise overheating limit\n\nsource: 4f2a9c1"
    mock_clients.checklist.get.return_value = _versioned_checklist(message)

    loaded = NominalClient(mock_clients).get_checklist("checklist")

    assert loaded.commit_message == message


def test_search_checklists_carries_commit_messages_without_extra_requests(mock_clients):
    """Search exposes each result's commit message from the search response alone."""
    hits = [
        scout_checks_api.SearchVersionedChecklist(rid=hit.rid, checks=[], commit=hit.commit, metadata=hit.metadata)
        for hit in (_versioned_checklist("Initial checks"), _versioned_checklist("Raise limit"))
    ]
    mock_clients.checklist.search.return_value = scout_checks_api.SearchVersionedChecklistPage(values=hits)

    found = NominalClient(mock_clients).search_checklists(workspace=WorkspaceSearchType.ALL)

    assert [result.commit_message for result in found] == ["Initial checks", "Raise limit"]
    mock_clients.checklist.get.assert_not_called()


def test_checklist_refresh_replaces_the_commit_message_in_place(mock_clients):
    """After someone else commits, refresh loads the new version's ID and message into the same instance."""
    loaded = Checklist._from_conjure(mock_clients, _versioned_checklist("Initial checks", "first"))
    mock_clients.checklist.get.return_value = _versioned_checklist("Raise limit", "second")

    refreshed = loaded.refresh()

    assert refreshed is loaded
    assert (loaded.commit_id, loaded.commit_message) == ("second", "Raise limit")
