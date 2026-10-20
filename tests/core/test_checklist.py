from datetime import UTC, datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest
from nominal_api import scout_api, scout_integrations_api, scout_run_api
from nominal_api import scout_checklistexecution_api as api

from nominal.core import Asset, Checklist, Integration
from nominal.core import streaming_checklist as streaming
from nominal.protos.asset.v2 import asset_pb2
from nominal.protos.integrations.v1 import integrations_pb2


@pytest.fixture
def checklist():
    return Checklist(rid="checklist", name="Monitor", description="", properties={}, labels=[], _clients=MagicMock())


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


@pytest.mark.parametrize("integrations", [[], ["one", "two"]])
def test_existing_rid_calls_keep_their_behavior(checklist, integrations):
    """Existing positional RID calls retain destinations, empty tags, and timing defaults."""
    checklist.execute_streaming(["asset"], integrations)
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


def test_empty_notifications_clear_destinations(checklist):
    """An explicit empty notification list produces an empty replacement configuration."""
    checklist.execute_streaming(["asset"], notifications=[])
    request = checklist._clients.checklist_execution.execute_streaming_checklist.call_args.args[1]
    assert request.notification_configurations == []


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
    assert statuses["running"].commit == "commit"
    assert [(c.check_rid, c.parameter_index, c.state.value) for c in statuses["running"].checks] == [
        ("check", 0, "pass"),
        ("check", 1, "skipped"),
        ("other", None, "invalid"),
        ("failure", None, "fail"),
    ]
    assert statuses["initializing"].commit is None
    assert statuses["initializing"].checks == ()
    assert statuses["failed"].commit is None
    assert statuses["failed"].checks == ()


def test_resolution_preserves_missing_channels_and_unsupported_features(checklist):
    """Resolution keeps channel scopes/tags, unknown status, and unsupported features per parameterized check."""
    checklist._clients.checklist_execution.validate_checklist_resolution.return_value = (
        api.BatchValidateChecklistResolutionResponse(
            checklist_results=[
                api.ValidateChecklistResolutionResponse(
                    asset_rid="asset",
                    checklist_rid="checklist",
                    resolution_status=[
                        api.ResolvedCheckStatus(
                            check_rid="check",
                            check_parameter_index=0,
                            invalid_streaming_compute_nodes=[api.InvalidStreamingComputeNode.INTEGRAL],
                            channels=[
                                api.ChannelLocatorWithStatus(
                                    channel_locator=scout_api.ChannelLocator(
                                        channel="temperature", data_source_ref="daq", tags={"sensor": "1"}
                                    ),
                                    resolution_status=api.ChannelResolutionStatus.UNRESOLVED,
                                ),
                                api.ChannelLocatorWithStatus(
                                    channel_locator=scout_api.ChannelLocator(
                                        channel="pressure", data_source_ref="daq", tags={}
                                    ),
                                    resolution_status=api.ChannelResolutionStatus.UNKNOWN,
                                ),
                            ],
                        )
                    ],
                )
            ]
        )
    )
    checks = checklist.validate_resolution(["asset", "other"], commit="revision")["asset"]
    check = checks[0]
    assert (check.check_rid, check.parameter_index) == ("check", 0)
    assert check.unsupported_streaming_features == ("INTEGRAL",)
    channel = check.channels[0]
    assert (channel.channel, channel.data_scope, channel.tags, channel.state.value) == (
        "temperature",
        "daq",
        {"sensor": "1"},
        "unresolved",
    )
    assert check.channels[1].state.value == "unknown"
    request = checklist._clients.checklist_execution.validate_checklist_resolution.call_args.args[1]
    assert [(r.asset_rid, r.checklist_rid, r.commit_id) for r in request.requests] == [
        ("asset", "checklist", "revision"),
        ("other", "checklist", "revision"),
    ]


def test_empty_asset_reads_do_not_request_status(checklist):
    """Empty read batches are local no-ops rather than invalid service requests."""
    assert checklist.get_streaming_status([]) == {}
    assert checklist.validate_resolution([]) == {}
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
