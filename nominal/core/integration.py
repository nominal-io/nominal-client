from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Mapping, Protocol, Self

from nominal.core._clientsbunch import HasScoutParams
from nominal.core._utils.api_tools import HasRid, RefreshableGrpcMixin
from nominal.core._utils.grpc_tools import translate_grpc_errors
from nominal.protos.integrations.v1 import integrations_pb2, integrations_pb2_grpc
from nominal.ts import IntegralNanosecondsUTC


class SlackInstanceType(Enum):
    """Slack environment used by a notification integration."""

    COMMERCIAL = "COMMERCIAL"
    GOV = "GOV"


class OpsgenieRegion(Enum):
    """Region hosting an Opsgenie integration."""

    US = "US"
    EU = "EU"

    def _to_proto(self) -> integrations_pb2.OpsgenieRegion.ValueType:
        return {
            OpsgenieRegion.US: integrations_pb2.OPSGENIE_REGION_US,
            OpsgenieRegion.EU: integrations_pb2.OPSGENIE_REGION_EU,
        }[self]


@dataclass(frozen=True)
class IntegrationDetails:
    """Provider-specific settings returned with an `Integration`.

    Use ``isinstance`` to access a provider's fields. These snapshots are not creation or update inputs.
    """


@dataclass(frozen=True)
class UnknownIntegrationDetails(IntegrationDetails):
    """Integration details are unavailable or unrecognized by this SDK."""


@dataclass(frozen=True)
class SlackWebhookIntegrationDetails(IntegrationDetails):
    """Slack destination metadata, without webhook credentials."""

    team_name: str
    channel: str
    channel_id: str
    slack_instance_type: SlackInstanceType | None
    """Slack environment, or None if absent, unspecified, or unrecognized by this SDK."""

    @classmethod
    def _from_proto(cls, details: integrations_pb2.SlackWebhookIntegration) -> Self:
        instance_type = {
            integrations_pb2.SLACK_INSTANCE_COMMERCIAL: SlackInstanceType.COMMERCIAL,
            integrations_pb2.SLACK_INSTANCE_GOV: SlackInstanceType.GOV,
        }.get(details.slack_instance)
        return cls(details.team_name, details.channel, details.channel_id, instance_type)


class WebhookContentType(Enum):
    """MIME type configured for a generic webhook."""

    APPLICATION_JSON = "application/json"
    APPLICATION_X_WWW_FORM_URLENCODED = "application/x-www-form-urlencoded"


@dataclass(frozen=True)
class WebhookIntegrationDetails(IntegrationDetails):
    """Generic webhook settings, excluding the destination URL and HMAC secret.

    ``custom_headers`` contains the HTTP headers. Its values may contain credentials and are excluded from ``repr``.
    """

    hmac_secret_rid: str | None
    content_type: WebhookContentType | None
    """Configured MIME type, or None if absent, unspecified, or unrecognized."""
    timeout_seconds: int | None
    """Request timeout in seconds, or None if unspecified."""
    custom_headers: Mapping[str, str] = field(repr=False)


@dataclass(frozen=True)
class WebhookDeliveryConfig:
    """Secure webhook delivery settings. None means the setting is unspecified."""

    default_request_timeout_seconds: int | None
    default_max_retries: int | None
    default_retry_interval_seconds: int | None
    default_exponential_backoff: bool | None
    default_max_retry_interval_seconds: int | None
    default_delivery_timeout_seconds: int | None

    @classmethod
    def _from_proto(cls, config: integrations_pb2.WebhookDeliveryConfig) -> Self:
        return cls(
            default_request_timeout_seconds=(
                config.default_request_timeout_seconds if config.HasField("default_request_timeout_seconds") else None
            ),
            default_max_retries=config.default_max_retries if config.HasField("default_max_retries") else None,
            default_retry_interval_seconds=(
                config.default_retry_interval_seconds if config.HasField("default_retry_interval_seconds") else None
            ),
            default_exponential_backoff=(
                config.default_exponential_backoff if config.HasField("default_exponential_backoff") else None
            ),
            default_max_retry_interval_seconds=(
                config.default_max_retry_interval_seconds
                if config.HasField("default_max_retry_interval_seconds")
                else None
            ),
            default_delivery_timeout_seconds=(
                config.default_delivery_timeout_seconds if config.HasField("default_delivery_timeout_seconds") else None
            ),
        )


@dataclass(frozen=True)
class SecureWebhookIntegrationDetails(IntegrationDetails):
    """Secure webhook settings, excluding the signing key.

    ``url`` is the destination URL; ``custom_headers`` contains the HTTP headers.
    Both may contain credentials and are excluded from ``repr``.
    """

    url: str = field(repr=False)
    custom_headers: Mapping[str, str] = field(repr=False)
    delivery_config: WebhookDeliveryConfig | None
    """Configured delivery policy, or None when absent."""


@dataclass(frozen=True)
class OpsgenieIntegrationDetails(IntegrationDetails):
    """Opsgenie configuration, without the API key."""

    region: OpsgenieRegion | None
    """Region, or None when unspecified or unrecognized by this SDK."""


@dataclass(frozen=True)
class TeamsIntegrationDetails(IntegrationDetails):
    """Identifies a Teams destination; its webhook credentials are not returned."""


@dataclass(frozen=True)
class PagerDutyIntegrationDetails(IntegrationDetails):
    """Identifies a PagerDuty destination; its routing key is not returned."""


def _integration_details_from_proto(details: integrations_pb2.IntegrationDetails) -> IntegrationDetails:
    result: IntegrationDetails
    match details.WhichOneof("integration_details"):
        case "slack_webhook":
            result = SlackWebhookIntegrationDetails._from_proto(details.slack_webhook)
        case "simple_webhook":
            webhook = details.simple_webhook
            result = WebhookIntegrationDetails(
                hmac_secret_rid=webhook.hmac_secret_rid if webhook.HasField("hmac_secret_rid") else None,
                content_type={
                    integrations_pb2.WEBHOOK_CONTENT_TYPE_APPLICATION_JSON: WebhookContentType.APPLICATION_JSON,
                    integrations_pb2.WEBHOOK_CONTENT_TYPE_APPLICATION_X_WWW_FORM_URLENCODED: (
                        WebhookContentType.APPLICATION_X_WWW_FORM_URLENCODED
                    ),
                }.get(webhook.content_type),
                timeout_seconds=webhook.timeout_seconds if webhook.HasField("timeout_seconds") else None,
                custom_headers=dict(webhook.custom_headers),
            )
        case "secure_webhook":
            secure = details.secure_webhook
            result = SecureWebhookIntegrationDetails(
                url=secure.url,
                custom_headers=dict(secure.custom_headers),
                delivery_config=(
                    WebhookDeliveryConfig._from_proto(secure.delivery_config)
                    if secure.HasField("delivery_config")
                    else None
                ),
            )
        case "opsgenie":
            result = OpsgenieIntegrationDetails(
                region={
                    integrations_pb2.OPSGENIE_REGION_US: OpsgenieRegion.US,
                    integrations_pb2.OPSGENIE_REGION_EU: OpsgenieRegion.EU,
                }.get(details.opsgenie.region)
            )
        case "teams_webhook":
            result = TeamsIntegrationDetails()
        case "pager_duty":
            result = PagerDutyIntegrationDetails()
        case _:
            result = UnknownIntegrationDetails()

    return result


@dataclass(frozen=True)
class Integration(HasRid, RefreshableGrpcMixin[integrations_pb2.Integration]):
    """Reusable notification destination.

    Retrieve one with `NominalClient.get_integration` or create one with a client creation method.

    Example:
        Create a destination using a client configured for the desired workspace:

        ```python
        from nominal.core import NominalClient

        client = NominalClient.from_profile("default")
        integration = client.create_webhook_integration(
            "Test alerts", "https://example.com/alerts"
        )
        integration.update(description="Checklist notifications")
        ```
    """

    rid: str
    name: str
    description: str | None
    created_at: IntegralNanosecondsUTC
    details: IntegrationDetails
    """Provider settings. Reading this field makes no request; `refresh` replaces its contents.

    Example:
        ```python
        from nominal.core import OpsgenieIntegrationDetails

        if isinstance(integration.details, OpsgenieIntegrationDetails):
            print(integration.details.region)
        ```
    """
    _clients: _Clients = field(repr=False)

    class _Clients(HasScoutParams, Protocol):
        @property
        def integrations(self) -> integrations_pb2_grpc.IntegrationsServiceStub: ...

    def _get_latest_api(self) -> integrations_pb2.Integration:
        return _get_integration(self._clients, self.rid)

    def update(self, *, name: str | None = None, description: str | None = None) -> Self:
        """Update the name or description in place.

        Args:
            name: New name, or None to leave it unchanged.
            description: New description, or None to leave it unchanged. An empty string clears it.

        Returns:
            This instance with current metadata. If both arguments are None, calls `refresh`.
        """
        if name is None and description is None:
            return self.refresh()

        request = integrations_pb2.UpdateIntegrationMetadataRequest(
            integration_rid=self.rid, name=name, description=description
        )
        with translate_grpc_errors():
            response = self._clients.integrations.UpdateIntegrationMetadata(request)
        return self._refresh_from_api(response.integration)

    def delete(self) -> None:
        """Delete this notification integration.

        Deleted integrations cannot be restored through this API. The local snapshot remains unchanged.
        """
        with translate_grpc_errors():
            self._clients.integrations.DeleteIntegration(
                integrations_pb2.DeleteIntegrationRequest(integration_rid=self.rid)
            )

    @classmethod
    def _from_proto(cls, clients: _Clients, integration: integrations_pb2.Integration) -> Self:
        return cls(
            rid=integration.rid,
            name=integration.name,
            description=integration.description if integration.HasField("description") else None,
            created_at=integration.created_at.ToNanoseconds(),
            details=_integration_details_from_proto(integration.details),
            _clients=clients,
        )


def _get_integration(clients: Integration._Clients, rid: str) -> integrations_pb2.Integration:
    with translate_grpc_errors():
        return clients.integrations.GetIntegration(
            integrations_pb2.GetIntegrationRequest(integration_rid=rid)
        ).integration


def _create_integration(
    clients: Integration._Clients,
    name: str,
    description: str | None,
    details: integrations_pb2.CreateIntegrationDetails,
) -> Integration:
    request = integrations_pb2.CreateIntegrationRequest(
        name=name,
        description=description,
        details=details,
        workspace=clients.resolve_default_workspace_rid(),
    )
    with translate_grpc_errors():
        response = clients.integrations.CreateIntegration(request)
    return Integration._from_proto(clients, response.integration)


def _create_webhook_integration(
    clients: Integration._Clients, name: str, webhook_url: str, description: str | None
) -> Integration:
    details = integrations_pb2.CreateIntegrationDetails(
        simple_webhook=integrations_pb2.CreateSimpleWebhookDetails(webhook=webhook_url)
    )
    return _create_integration(clients, name, description, details)


def _create_opsgenie_integration(
    clients: Integration._Clients, name: str, api_key: str, region: OpsgenieRegion, description: str | None
) -> Integration:
    details = integrations_pb2.CreateIntegrationDetails(
        opsgenie=integrations_pb2.CreateOpsgenieDetails(api_key=api_key, region=region._to_proto())
    )
    return _create_integration(clients, name, description, details)


def _create_teams_integration(
    clients: Integration._Clients, name: str, webhook_url: str, description: str | None
) -> Integration:
    details = integrations_pb2.CreateIntegrationDetails(
        teams_webhook=integrations_pb2.CreateTeamsWebhookDetails(webhook=webhook_url)
    )
    return _create_integration(clients, name, description, details)


def _create_pagerduty_integration(
    clients: Integration._Clients, name: str, routing_key: str, description: str | None
) -> Integration:
    details = integrations_pb2.CreateIntegrationDetails(
        pager_duty=integrations_pb2.CreatePagerDutyDetails(routing_key=routing_key)
    )
    return _create_integration(clients, name, description, details)
