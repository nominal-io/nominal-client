from __future__ import annotations

from unittest.mock import MagicMock

import grpc
import pytest
from google.protobuf.timestamp_pb2 import Timestamp

from nominal.core import (
    Integration,
    IntegrationDetails,
    OpsgenieIntegrationDetails,
    OpsgenieRegion,
    PagerDutyIntegrationDetails,
    SecureWebhookIntegrationDetails,
    SlackWebhookIntegrationDetails,
    TeamsIntegrationDetails,
    UnknownIntegrationDetails,
    WebhookContentType,
    WebhookIntegrationDetails,
)
from nominal.core.client import NominalClient, WorkspaceSearchType
from nominal.exceptions import NominalNotFoundError
from nominal.protos.integrations.v1 import integrations_pb2


def test_get_preserves_description_presence_and_nanoseconds() -> None:
    """Absent metadata stays absent and creation time retains submicrosecond precision."""
    clients = MagicMock()
    clients.integrations.GetIntegration.return_value = integrations_pb2.GetIntegrationResponse(
        integration=integrations_pb2.Integration(rid="integration", created_at=Timestamp(seconds=1, nanos=123))
    )

    integration = NominalClient(_clients=clients).get_integration(rid="integration")

    assert integration.description is None
    assert integration.created_at == 1_000_000_123
    assert integration.rid == "integration"


def test_update_clears_description_and_refreshes_same_instance() -> None:
    """An empty description clears it, an omitted name stays unchanged, and update preserves object identity."""
    clients = MagicMock()
    clients.integrations.GetIntegration.return_value = integrations_pb2.GetIntegrationResponse(
        integration=integrations_pb2.Integration(rid="integration", name="original", description="old")
    )
    integration = NominalClient(_clients=clients).get_integration("integration")
    clients.integrations.UpdateIntegrationMetadata.return_value = integrations_pb2.UpdateIntegrationMetadataResponse(
        integration=integrations_pb2.Integration(rid="integration", name="original", description="")
    )

    assert integration.update(description="") is integration

    request = clients.integrations.UpdateIntegrationMetadata.call_args.args[0]
    assert request.integration_rid == integration.rid
    assert not request.HasField("name")
    assert request.HasField("description")
    assert request.description == ""
    assert integration.description == ""
    assert clients.integrations.GetIntegration.call_count == 1


@pytest.mark.parametrize("workspace,expected", [(None, ["default"]), (WorkspaceSearchType.ALL, [])])
def test_list_follows_pages_and_workspace_selection(workspace, expected) -> None:
    """Listing retains workspace selection on every page, with ALL bypassing default resolution."""
    clients = MagicMock()
    clients.resolve_default_workspace_rid.return_value = "default"
    clients.integrations.ListIntegrations.side_effect = [
        integrations_pb2.ListIntegrationsResponse(
            integrations=[integrations_pb2.Integration(rid="first")], next_page_token="next"
        ),
        integrations_pb2.ListIntegrationsResponse(integrations=[integrations_pb2.Integration(rid="second")]),
    ]

    results = NominalClient(_clients=clients).list_integrations(workspace=workspace)

    assert [result.rid for result in results] == ["first", "second"]
    requests = [call.args[0] for call in clients.integrations.ListIntegrations.call_args_list]
    assert [list(request.workspaces) for request in requests] == [expected, expected]
    assert not requests[0].HasField("page_token")
    assert requests[1].page_token == "next"
    assert clients.resolve_default_workspace_rid.call_count == (workspace is None)


@pytest.mark.parametrize(
    "method,args,details",
    [
        (
            "create_webhook_integration",
            ("https://example.com/webhook",),
            integrations_pb2.CreateIntegrationDetails(
                simple_webhook=integrations_pb2.CreateSimpleWebhookDetails(webhook="https://example.com/webhook")
            ),
        ),
        (
            "create_teams_integration",
            ("https://example.com/teams",),
            integrations_pb2.CreateIntegrationDetails(
                teams_webhook=integrations_pb2.CreateTeamsWebhookDetails(webhook="https://example.com/teams")
            ),
        ),
        (
            "create_pagerduty_integration",
            ("routing-key",),
            integrations_pb2.CreateIntegrationDetails(
                pager_duty=integrations_pb2.CreatePagerDutyDetails(routing_key="routing-key")
            ),
        ),
    ],
)
def test_create_selects_details_and_resolves_default_workspace(method, args, details) -> None:
    """Each creation entry point sends its own details variant and the client's resolved workspace."""
    clients = MagicMock()
    clients.resolve_default_workspace_rid.return_value = "default"
    clients.integrations.CreateIntegration.return_value = integrations_pb2.CreateIntegrationResponse(
        integration=integrations_pb2.Integration(rid="created", name="alerts")
    )

    result = getattr(NominalClient(_clients=clients), method)("alerts", *args)

    request = clients.integrations.CreateIntegration.call_args.args[0]
    assert request.details == details
    assert request.workspace == "default"
    assert not request.HasField("description")
    assert result.rid == "created"


def test_create_opsgenie_maps_region() -> None:
    """Opsgenie creation maps the SDK region enum into the correct protobuf value."""
    clients = MagicMock()
    clients.resolve_default_workspace_rid.return_value = "default"
    clients.integrations.CreateIntegration.return_value = integrations_pb2.CreateIntegrationResponse(
        integration=integrations_pb2.Integration(rid="created")
    )

    NominalClient(_clients=clients).create_opsgenie_integration("alerts", "api-key", OpsgenieRegion.EU)

    request = clients.integrations.CreateIntegration.call_args.args[0]
    assert request.details.opsgenie.api_key == "api-key"
    assert request.details.opsgenie.region == integrations_pb2.OPSGENIE_REGION_EU


@pytest.mark.parametrize("slack_instance,expected", [(None, None), (2, "GOV"), (99, None)])
def test_slack_details_handle_absent_and_unknown_instance(slack_instance, expected) -> None:
    """Slack details expose known instance types and tolerate absent or future enum values."""
    clients = MagicMock()
    clients.integrations.GetIntegration.return_value = integrations_pb2.GetIntegrationResponse(
        integration=integrations_pb2.Integration(
            rid="integration",
            details=integrations_pb2.IntegrationDetails(
                slack_webhook=integrations_pb2.SlackWebhookIntegration(
                    team_name="team", channel="alerts", channel_id="channel", slack_instance=slack_instance
                )
            ),
        )
    )
    integration = NominalClient(_clients=clients).get_integration("integration")

    details = integration.details

    assert isinstance(details, SlackWebhookIntegrationDetails)
    assert clients.integrations.GetIntegration.call_count == 1
    assert details.channel_id == "channel"
    assert (details.slack_instance_type.value if details.slack_instance_type else None) == expected


def test_non_slack_details_use_their_provider_type() -> None:
    """A webhook response produces webhook details."""
    clients = MagicMock()
    clients.integrations.GetIntegration.return_value = integrations_pb2.GetIntegrationResponse(
        integration=integrations_pb2.Integration(
            rid="integration",
            details=integrations_pb2.IntegrationDetails(simple_webhook=integrations_pb2.SimpleWebhookIntegration()),
        )
    )

    assert isinstance(NominalClient(_clients=clients).get_integration("integration").details, WebhookIntegrationDetails)


def test_get_translates_grpc_errors(fake_rpc_error) -> None:
    """Integration lookup exposes the SDK not-found exception instead of a transport exception."""
    clients = MagicMock()
    clients.integrations.GetIntegration.side_effect = fake_rpc_error(grpc.StatusCode.NOT_FOUND)

    with pytest.raises(NominalNotFoundError):
        NominalClient(_clients=clients).get_integration("missing")


def test_refresh_replaces_slack_details_with_the_same_snapshot() -> None:
    """Refresh updates metadata and Slack details together, clearing details when no longer present."""
    clients = MagicMock()
    clients.integrations.GetIntegration.side_effect = [
        integrations_pb2.GetIntegrationResponse(
            integration=integrations_pb2.Integration(
                rid="integration",
                name="old",
                details=integrations_pb2.IntegrationDetails(
                    slack_webhook=integrations_pb2.SlackWebhookIntegration(channel_id="old-channel")
                ),
            )
        ),
        integrations_pb2.GetIntegrationResponse(
            integration=integrations_pb2.Integration(rid="integration", name="new")
        ),
    ]
    integration = NominalClient(_clients=clients).get_integration("integration")
    assert isinstance(integration.details, SlackWebhookIntegrationDetails)
    assert integration.details.channel_id == "old-channel"

    assert integration.refresh() is integration

    assert integration.name == "new"
    assert isinstance(integration.details, UnknownIntegrationDetails)
    assert clients.integrations.GetIntegration.call_count == 2


@pytest.mark.parametrize(
    "raw,expected_type",
    [
        (
            integrations_pb2.IntegrationDetails(teams_webhook=integrations_pb2.TeamsWebhookIntegration()),
            TeamsIntegrationDetails,
        ),
        (
            integrations_pb2.IntegrationDetails(pager_duty=integrations_pb2.PagerDutyIntegration()),
            PagerDutyIntegrationDetails,
        ),
        (integrations_pb2.IntegrationDetails(), UnknownIntegrationDetails),
        # Field 99 is a future length-delimited provider variant with an empty payload.
        (integrations_pb2.IntegrationDetails.FromString(b"\x9a\x06\x00"), UnknownIntegrationDetails),
    ],
)
def test_empty_and_unknown_details_variants(raw, expected_type) -> None:
    """Empty known providers stay distinguishable; absent and future oneof variants remain readable."""
    integration = Integration._from_proto(MagicMock(), integrations_pb2.Integration(details=raw))
    assert isinstance(integration.details, IntegrationDetails)
    assert isinstance(integration.details, expected_type)


@pytest.mark.parametrize("region,expected", [(1, OpsgenieRegion.US), (2, OpsgenieRegion.EU), (0, None), (99, None)])
def test_opsgenie_snapshot_region(region, expected) -> None:
    """Opsgenie snapshots map known regions and tolerate unspecified or future values."""
    integration = Integration._from_proto(
        MagicMock(),
        integrations_pb2.Integration(
            details=integrations_pb2.IntegrationDetails(opsgenie=integrations_pb2.OpsgenieIntegration(region=region))
        ),
    )
    assert isinstance(integration.details, OpsgenieIntegrationDetails)
    assert integration.details.region is expected


def test_webhook_snapshot_preserves_presence_and_copies_headers() -> None:
    """Webhook snapshots preserve optional values, copy headers, and omit credentials from repr."""
    raw = integrations_pb2.Integration(
        details=integrations_pb2.IntegrationDetails(
            simple_webhook=integrations_pb2.SimpleWebhookIntegration(
                hmac_secret_rid="", content_type=1, timeout_seconds=0, custom_headers={"Authorization": "credential"}
            )
        )
    )
    integration = Integration._from_proto(MagicMock(), raw)
    details = integration.details
    assert isinstance(details, WebhookIntegrationDetails)
    assert details.hmac_secret_rid == ""
    assert details.timeout_seconds == 0
    assert details.content_type is WebhookContentType.APPLICATION_JSON
    raw.details.simple_webhook.custom_headers.clear()
    assert details.custom_headers == {"Authorization": "credential"}
    assert "credential" not in repr(integration)
    empty = Integration._from_proto(MagicMock(), raw).details
    assert isinstance(empty, WebhookIntegrationDetails)
    assert empty.custom_headers == {}


@pytest.mark.parametrize("content_type", [None, 0, 99])
def test_webhook_absent_and_unknown_configuration(content_type) -> None:
    """Missing optional configuration stays absent and unknown MIME types do not break reads."""
    details = Integration._from_proto(
        MagicMock(),
        integrations_pb2.Integration(
            details=integrations_pb2.IntegrationDetails(
                simple_webhook=integrations_pb2.SimpleWebhookIntegration(content_type=content_type)
            )
        ),
    ).details
    assert isinstance(details, WebhookIntegrationDetails)
    assert details.hmac_secret_rid is None
    assert details.timeout_seconds is None
    assert details.content_type is None


def test_refresh_replaces_provider_details_and_preserves_delivery_presence() -> None:
    """Refresh changes provider snapshots in place and preserves explicit zero/false delivery options."""
    clients = MagicMock()
    clients.integrations.GetIntegration.side_effect = [
        integrations_pb2.GetIntegrationResponse(
            integration=integrations_pb2.Integration(
                rid="integration",
                details=integrations_pb2.IntegrationDetails(teams_webhook=integrations_pb2.TeamsWebhookIntegration()),
            )
        ),
        integrations_pb2.GetIntegrationResponse(
            integration=integrations_pb2.Integration(
                rid="integration",
                details=integrations_pb2.IntegrationDetails(
                    secure_webhook=integrations_pb2.SecureWebhookIntegration(
                        url="https://example.com/credential",
                        custom_headers={"Authorization": "credential"},
                        delivery_config=integrations_pb2.WebhookDeliveryConfig(
                            default_max_retries=0, default_exponential_backoff=False
                        ),
                    )
                ),
            )
        ),
    ]
    integration = NominalClient(_clients=clients).get_integration("integration")
    original = integration.details
    assert integration.refresh() is integration
    assert integration.details is not original
    assert isinstance(integration.details, SecureWebhookIntegrationDetails)
    assert integration.details.url == "https://example.com/credential"
    config = integration.details.delivery_config
    assert config is not None
    assert config.default_max_retries == 0
    assert config.default_exponential_backoff is False
    assert config.default_request_timeout_seconds is None
    assert config.default_retry_interval_seconds is None
    assert config.default_max_retry_interval_seconds is None
    assert config.default_delivery_timeout_seconds is None
    assert "credential" not in repr(integration)


def test_empty_update_refreshes_without_sending_metadata_update() -> None:
    """Omitting all metadata refreshes the same instance without issuing an empty update request."""
    clients = MagicMock()
    clients.integrations.GetIntegration.return_value = integrations_pb2.GetIntegrationResponse(
        integration=integrations_pb2.Integration(rid="integration", name="current")
    )
    integration = Integration._from_proto(clients, integrations_pb2.Integration(rid="integration", name="old"))

    assert integration.update() is integration

    assert integration.name == "current"
    clients.integrations.UpdateIntegrationMetadata.assert_not_called()
    clients.integrations.GetIntegration.assert_called_once_with(
        integrations_pb2.GetIntegrationRequest(integration_rid="integration")
    )
