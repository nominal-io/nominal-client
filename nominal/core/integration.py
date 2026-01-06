from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Protocol

from nominal_api import scout_integrations_api
from typing_extensions import Self

from nominal.core._clientsbunch import HasScoutParams
from nominal.core._utils.api_tools import HasRid, RefreshableMixin, rid_from_instance_or_string
from nominal.core.workspace import Workspace
from nominal.ts import IntegralNanosecondsUTC, _SecondsNanos

logger = logging.getLogger(__name__)


class SlackInstanceType(Enum):
    """Type of slack instance (commercial or gov)"""

    COMMERCIAL = "COMMERCIAL"
    GOV = "GOV"

    @classmethod
    def _from_conjure(cls, status: scout_integrations_api.SlackInstanceType) -> SlackInstanceType:
        if status.value in cls.__members__:
            return cls(status.value)
        else:
            raise ValueError(f"Unknown stack instance type: {status.value}")


@dataclass(frozen=True)
class SlackWebhookIntegrationDetails:
    """Details about webhook integrations with slack."""

    team_name: str
    """Name of the overall slack instance name"""

    channel: str
    """Name of the channel to send messages to. Refers to dms to a specific user if prefixed with `@`."""

    channel_id: str
    """Underlying ID of the channel to send notifications to."""

    slack_instance_type: SlackInstanceType | None
    """Type of slack instance (gov or commercial)"""

    @classmethod
    def _from_conjure(cls, webhook_integration: scout_integrations_api.SlackWebhookIntegration) -> Self:
        instance_type = (
            None
            if webhook_integration.slack_instance is None
            else SlackInstanceType._from_conjure(webhook_integration.slack_instance)
        )
        return cls(
            team_name=webhook_integration.team_name,
            channel=webhook_integration.channel,
            channel_id=webhook_integration.channel_id,
            slack_instance_type=instance_type,
        )


class OpsgenieRegion(Enum):
    """Region for Opsgenie servers"""

    US = "US"
    EU = "EU"

    @classmethod
    def _from_conjure(cls, status: scout_integrations_api.OpsgenieRegion) -> OpsgenieRegion:
        if status.value in cls.__members__:
            return cls(status.value)
        else:
            raise ValueError(f"Unknown opsgenie region type: {status.value}")

    def _to_conjure(self) -> scout_integrations_api.OpsgenieRegion:
        return {
            "US": scout_integrations_api.OpsgenieRegion.US,
            "EU": scout_integrations_api.OpsgenieRegion.EU,
        }[self.value]


@dataclass(frozen=True)
class Integration(HasRid, RefreshableMixin[scout_integrations_api.Integration]):
    """External integration with Nominal for sending notifications"""

    rid: str
    name: str
    description: str | None
    created_at: IntegralNanosecondsUTC

    _clients: _Clients = field(repr=False)

    class _Clients(HasScoutParams, Protocol):
        @property
        def integrations(self) -> scout_integrations_api.IntegrationsService: ...

    def _get_latest_api(self) -> scout_integrations_api.Integration:
        return self._clients.integrations.get_integration(self._clients.auth_header, self.rid)

    def update(self, name: str | None = None, description: str | None = None) -> Self:
        """Update the integration's metadata

        Args:
            name: If provided, update the name of the integration
            description: If provided, update the description ofthe integration

        Returns:
            Updated instance of the integration
        """
        request = scout_integrations_api.UpdateIntegrationRequest(
            name=name,
            description=description,
        )
        updated_integration = self._clients.integrations.update_integration_metadata(
            self._clients.auth_header, self.rid, request
        )
        return self._refresh_from_api(updated_integration)

    def get_slack_integration_details(self) -> SlackWebhookIntegrationDetails | None:
        """Return slack integration details

        NOTE: returns None if the integration is not actually a slack integration.
        """
        # TODO(drake): make more generic if/when other integrations have actual details (e.g. webhook url)
        raw_integration = self._get_latest_api()
        if raw_integration.integration_details.slack_webhook_integration:
            return SlackWebhookIntegrationDetails._from_conjure(
                raw_integration.integration_details.slack_webhook_integration
            )
        else:
            # integration isn't a slack-style integration
            return None

    def archive(self) -> None:
        """Archive the integration, hiding it from view on the UI.

        NOTE: currently, there is no way to unarchive an integration once archived.
        """
        self._clients.integrations.delete_integration(self._clients.auth_header, self.rid)

    @classmethod
    def _from_conjure(cls, clients: _Clients, integration: scout_integrations_api.Integration) -> Self:
        return cls(
            rid=integration.rid,
            name=integration.name,
            description=integration.description,
            created_at=_SecondsNanos.from_flexible(integration.created_at).to_nanoseconds(),
            _clients=clients,
        )


@dataclass(frozen=True)
class IntegrationBuilder:
    """Builder instance to construct and initialize external Integrations."""

    _clients: Integration._Clients

    def _create_integration(
        self,
        name: str,
        description: str | None,
        workspace: Workspace | str | None,
        details: scout_integrations_api.CreateIntegrationDetails,
    ) -> Integration:
        create_request = scout_integrations_api.CreateIntegrationRequest(
            name=name,
            description=description,
            create_integration_details=details,
            workspace=None if workspace is None else rid_from_instance_or_string(workspace),
        )

        raw_integration = self._clients.integrations.create_integration(self._clients.auth_header, create_request)
        return Integration._from_conjure(self._clients, raw_integration)

    def create_webhook_integration(
        self, name: str, webhook_url: str, *, description: str | None = None, workspace: Workspace | str | None = None
    ) -> Integration:
        """Create an integration with a generic webhook

        Args:
            name: Name of the integration to create
            webhook_url: URL to send webhook contents to
            description: Optionally, description of the integration to create
            workspace: Workspace to create the integration within.
                NOTE: if not provided, uses the organization's default workspace, and fails if no default has been
                    configured

        Returns:
            Constructed and initialized integration
        """
        create_details = scout_integrations_api.CreateIntegrationDetails(
            create_simple_webhook_details=scout_integrations_api.CreateSimpleWebhookDetails(webhook_url)
        )
        return self._create_integration(name, description, workspace, create_details)

    def create_opsgenie_integration(
        self,
        name: str,
        api_key: str,
        region: OpsgenieRegion,
        *,
        description: str | None = None,
        workspace: Workspace | str | None = None,
    ) -> Integration:
        """Create an integration with a Opsgenie

        Args:
            name: Name of the integration to create
            api_key: Opsgenie API Key
            region: Opsgenie region to create an integration for
            description: Optionally, description of the integration to create
            workspace: Workspace to create the integration within.
                NOTE: if not provided, uses the organization's default workspace, and fails if no default has been
                    configured

        Returns:
            Constructed and initialized integration
        """
        create_details = scout_integrations_api.CreateIntegrationDetails(
            create_opsgenie_integration_details=scout_integrations_api.CreateOpsgenieIntegrationDetails(
                api_key,
                region._to_conjure(),
            )
        )
        return self._create_integration(name, description, workspace, create_details)

    def create_teams_integration(
        self, name: str, webhook_url: str, *, description: str | None = None, workspace: Workspace | str | None = None
    ) -> Integration:
        """Create an integration with Microsoft Teams

        Args:
            name: Name of the integration to create
            webhook_url: Microsoft Teams Webhook URL to send notification contents to
            description: Optionally, description of the integration to create
            workspace: Workspace to create the integration within.
                NOTE: if not provided, uses the organization's default workspace, and fails if no default has been
                    configured

        See: https://support.microsoft.com/en-us/office/create-incoming-webhooks-with-workflows-for-microsoft-teams-8ae491c7-0394-4861-ba59-055e33f75498

        Returns:
            Constructed and initialized integration
        """
        create_details = scout_integrations_api.CreateIntegrationDetails(
            create_teams_webhook_integration_details=scout_integrations_api.CreateTeamsWebhookIntegrationDetails(
                webhook_url
            )
        )
        return self._create_integration(name, description, workspace, create_details)

    def create_pagerduty_integration(
        self, name: str, routing_key: str, *, description: str | None = None, workspace: Workspace | str | None = None
    ) -> Integration:
        """Create an integration with Pagerduty

        Args:
            name: Name of the integration to create
            routing_key: Routing key for Pagerduty
            description: Optionally, description of the integration to create
            workspace: Workspace to create the integration within.
                NOTE: if not provided, uses the organization's default workspace, and fails if no default has been
                    configured

        See: https://support.pagerduty.com/docs/services-and-integrations#create-a-generic-events-api-integration

        Returns:
            Constructed and initialized integration
        """
        create_details = scout_integrations_api.CreateIntegrationDetails(
            create_pager_duty_integration_details=scout_integrations_api.CreatePagerDutyIntegrationDetails(routing_key)
        )
        return self._create_integration(name, description, workspace, create_details)
