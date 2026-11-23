from __future__ import annotations

import uuid
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass

from google.protobuf.json_format import MessageToDict

from nominal.core._utils.api_tools import rid_from_instance_or_string
from nominal.core.client import NominalClient
from nominal.core.workspace import Workspace
from nominal.experimental.analyst._automation import Automation, AutomationSaveResult, AutomationWebhookSecret
from nominal.experimental.analyst._automation_run import AutomationRun
from nominal.protos.ai.v1 import analyst_automation_pb2 as api


@dataclass(frozen=True)
class AutomationValidationIssue:
    """A validation failure at a dotted path in the YAML document."""

    path: str
    message: str


@dataclass(frozen=True)
class AutomationValidation:
    """Whether YAML can be saved, with the server's validation issues."""

    valid: bool
    issues: Sequence[AutomationValidationIssue]
    name: str
    description: str


@dataclass(frozen=True)
class AutomationSchema:
    """Server-owned JSON schema, YAML examples, and currently authorable capabilities."""

    api_version: str
    json_schema: Mapping[str, object]
    examples: Mapping[str, str]
    capabilities: Mapping[str, object]


def get_automation_schema(client: NominalClient) -> AutomationSchema:
    """Get the server's current automation schema and supported components."""
    response = client._clients.analyst_automations.call(
        "GetAutomationSchema", api.GetAutomationSchemaRequest(), api.GetAutomationSchemaResponse
    )
    return AutomationSchema(
        response.api_version,
        MessageToDict(response.json_schema),
        dict(response.examples),
        MessageToDict(response.capabilities),
    )


def validate_automation(
    client: NominalClient, yaml_text: str, *, workspace: Workspace | str | None = None
) -> AutomationValidation:
    """Validate YAML without saving it; invalid documents return issues rather than raising.

    Omit workspace to use the client's configured or resolved default workspace.
    Authorization and transport failures still raise exceptions.
    """
    response = client._clients.analyst_automations.call(
        "ValidateAutomation",
        api.ValidateAutomationRequest(
            workspace_rid=client._clients.resolve_default_workspace_rid()
            if workspace is None
            else rid_from_instance_or_string(workspace),
            yaml_text=yaml_text,
        ),
        api.ValidateAutomationResponse,
    )
    return AutomationValidation(
        response.valid,
        tuple(AutomationValidationIssue(raw.path, raw.message) for raw in response.issues),
        response.name,
        response.description,
    )


def create_automation(
    client: NominalClient,
    yaml_text: str,
    *,
    workspace: Workspace | str | None = None,
    enabled: bool = False,
    request_id: str | None = None,
) -> AutomationSaveResult:
    """Save YAML as a new automation owned by the authenticated user.

    Args:
        client: Authenticated Nominal client.
        yaml_text: Workflow source; validation is owned by the server.
        workspace: Workspace or workspace RID. Omit to use the client's default workspace.
        enabled: Start schedules and accept webhooks immediately when true. Defaults to disabled.
        request_id: UUID for idempotent retries. Generated when omitted. Reusing it does not return secrets again.

    Returns:
        Saved automation and newly issued webhook secrets. Store secrets securely before discarding this result.
    """
    response = client._clients.analyst_automations.call(
        "CreateAutomation",
        api.CreateAutomationRequest(
            automation=api.Automation(
                workspace_rid=client._clients.resolve_default_workspace_rid()
                if workspace is None
                else rid_from_instance_or_string(workspace),
                yaml_text=yaml_text,
                enabled=enabled,
            ),
            request_id=str(uuid.uuid4()) if request_id is None else request_id,
        ),
        api.CreateAutomationResponse,
    )
    return AutomationSaveResult(
        Automation._from_proto(client._clients, response.automation),
        tuple(AutomationWebhookSecret._from_proto(raw) for raw in response.webhook_secrets),
    )


def get_automation(client: NominalClient, rid: str) -> Automation:
    """Fetch an owned automation, including its saved YAML source."""
    response = client._clients.analyst_automations.call(
        "GetAutomation", api.GetAutomationRequest(automation_rid=rid), api.GetAutomationResponse
    )
    return Automation._from_proto(client._clients, response.automation)


def search_automations(
    client: NominalClient, *, workspace: Workspace | str | None = None, query: str = ""
) -> Iterator[Automation]:
    """Iterate owned automations in a workspace; refresh a result to load its YAML source."""
    workspace_rid = (
        client._clients.resolve_default_workspace_rid() if workspace is None else rid_from_instance_or_string(workspace)
    )
    page_token = ""
    while True:
        response = client._clients.analyst_automations.call(
            "SearchAutomations",
            api.SearchAutomationsRequest(workspace_rid=workspace_rid, query=query, page_token=page_token),
            api.SearchAutomationsResponse,
        )
        yield from (Automation._from_proto(client._clients, raw.automation) for raw in response.results)
        page_token = response.next_page_token
        if not page_token:
            return


def get_automation_run(client: NominalClient, rid: str) -> AutomationRun:
    """Fetch a run by RID, including status and its execution error code."""
    response = client._clients.analyst_automations.call(
        "GetAutomationRun", api.GetAutomationRunRequest(automation_run_rid=rid), api.GetAutomationRunResponse
    )
    return AutomationRun._from_proto(client._clients, response.automation_run)
