from __future__ import annotations

import uuid
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Self

from google.protobuf import field_mask_pb2, json_format, struct_pb2

from nominal.core._utils.api_tools import HasRid, RefreshableGrpcMixin
from nominal.experimental.analyst._automation_run import AutomationRun
from nominal.protos.ai.v1 import analyst_automation_pb2 as api
from nominal.ts import IntegralNanosecondsUTC


@dataclass(frozen=True)
class AutomationWebhookSecret:
    """A newly issued signing secret. Store it securely; later reads do not return it."""

    trigger_id: str
    secret: str = field(repr=False)
    webhook_path: str

    @classmethod
    def _from_proto(cls, raw: api.AutomationWebhookSecret) -> Self:
        return cls(raw.trigger_id, raw.secret, raw.webhook_path)


@dataclass(frozen=True)
class AutomationSaveResult:
    """Saved automation and secrets issued by this operation only.

    Retrying a create with the same request ID returns no secrets. Rotate a webhook's secret
    if the original response was lost. Refreshing the automation does not recover secrets.
    """

    automation: Automation
    webhook_secrets: Sequence[AutomationWebhookSecret]


@dataclass(frozen=True)
class Automation(HasRid, RefreshableGrpcMixin[api.Automation]):
    """A saved Analyst workflow owned by its creator in one workspace.

    Updates and deletes use this snapshot's revision to detect conflicting YAML changes.
    Refresh before retrying a conflict. Enabled state controls schedules and webhooks;
    manual runs can also execute disabled automations.
    """

    rid: str
    workspace_rid: str
    owner_rid: str
    revision: int
    name: str
    description: str
    tags: Sequence[str]
    yaml_text: str | None
    """Saved source; None for search summaries. Refresh to load the source."""
    enabled: bool
    url: str
    created_at: IntegralNanosecondsUTC
    updated_at: IntegralNanosecondsUTC
    _clients: AutomationRun._Clients = field(repr=False)

    def _get_latest_api(self) -> api.Automation:
        return self._clients.analyst_automations.call(
            "GetAutomation", api.GetAutomationRequest(automation_rid=self.rid), api.GetAutomationResponse
        ).automation

    def update(self, *, yaml_text: str | None = None, enabled: bool | None = None) -> AutomationSaveResult:
        """Replace supplied fields and refresh this instance from the saved response.

        Args:
            yaml_text: Replacement YAML. None leaves source unchanged; empty text is sent for server validation.
            enabled: Whether schedules and webhooks can start runs. None leaves it unchanged.

        Returns:
            This automation and any newly issued webhook secrets.

        Raises:
            ValueError: Neither field was supplied.
            nominal.exceptions.NominalConflictError: The saved YAML revision has changed.
        """
        paths = []
        if yaml_text is not None:
            paths.append("yaml_text")
        if enabled is not None:
            paths.append("enabled")
        if not paths:
            raise ValueError("Supply yaml_text or enabled to update an automation.")
        response = self._clients.analyst_automations.call(
            "UpdateAutomation",
            api.UpdateAutomationRequest(
                automation=api.Automation(
                    automation_rid=self.rid,
                    yaml_text="" if yaml_text is None else yaml_text,
                    enabled=False if enabled is None else enabled,
                ),
                update_mask=field_mask_pb2.FieldMask(paths=paths),
                expected_revision=self.revision,
            ),
            api.UpdateAutomationResponse,
        )
        self._refresh_from_api(response.automation)
        return AutomationSaveResult(
            self, tuple(AutomationWebhookSecret._from_proto(raw) for raw in response.webhook_secrets)
        )

    def delete(self) -> None:
        """Delete the definition and stop future schedules/webhooks; retain its run history.

        Uses this snapshot's revision. Already admitted runs are not canceled by this call.
        """
        self._clients.analyst_automations.call(
            "DeleteAutomation",
            api.DeleteAutomationRequest(automation_rid=self.rid, expected_revision=self.revision),
            api.DeleteAutomationResponse,
        )

    def run(
        self, *, payload: Mapping[str, object] | None = None, trigger_id: str = "", request_id: str | None = None
    ) -> AutomationRun:
        """Submit a manual run of this snapshot's revision without waiting for completion.

        Args:
            payload: JSON-compatible trigger data passed to the agent as untrusted input.
            trigger_id: Trigger to fire. Empty selects the only manual trigger or the only trigger.
            request_id: UUID for idempotent retries. A new UUID is generated when omitted.

        Returns:
            The admitted or rejected run. Check its status and refresh to observe execution.
        """
        encoded = None if payload is None else json_format.ParseDict(dict(payload), struct_pb2.Struct())
        response = self._clients.analyst_automations.call(
            "CreateAutomationRun",
            api.CreateAutomationRunRequest(
                automation_rid=self.rid,
                request_id=str(uuid.uuid4()) if request_id is None else request_id,
                expected_revision=self.revision,
                trigger_id=trigger_id,
                payload=encoded,
            ),
            api.CreateAutomationRunResponse,
        )
        return AutomationRun._from_proto(self._clients, response.automation_run)

    def list_runs(self) -> Iterator[AutomationRun]:
        """Iterate run history newest first, fetching subsequent pages lazily."""
        page_token = ""
        while True:
            response = self._clients.analyst_automations.call(
                "SearchAutomationRuns",
                api.SearchAutomationRunsRequest(automation_rid=self.rid, page_token=page_token),
                api.SearchAutomationRunsResponse,
            )
            yield from (AutomationRun._from_proto(self._clients, raw) for raw in response.automation_runs)
            page_token = response.next_page_token
            if not page_token:
                return

    def rotate_webhook_secret(self, trigger_id: str) -> AutomationWebhookSecret:
        """Replace a webhook's signing secret and return its new value once.

        Existing senders must switch to the new secret. Do not blindly retry after a lost response:
        another rotation invalidates the secret issued by the previous request.
        """
        response = self._clients.analyst_automations.call(
            "RotateAutomationWebhookSecret",
            api.RotateAutomationWebhookSecretRequest(automation_rid=self.rid, trigger_id=trigger_id),
            api.RotateAutomationWebhookSecretResponse,
        )
        return AutomationWebhookSecret(trigger_id, response.secret, response.webhook_path)

    @classmethod
    def _from_proto(cls, clients: AutomationRun._Clients, raw: api.Automation) -> Self:
        return cls(
            rid=raw.automation_rid,
            workspace_rid=raw.workspace_rid,
            owner_rid=raw.owner_rid,
            revision=raw.revision,
            name=raw.name,
            description=raw.description,
            tags=tuple(raw.tags),
            yaml_text=raw.yaml_text or None,
            enabled=raw.enabled,
            url=raw.url,
            created_at=raw.created_at.ToNanoseconds(),
            updated_at=raw.updated_at.ToNanoseconds(),
            _clients=clients,
        )
