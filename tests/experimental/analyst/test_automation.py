from unittest.mock import MagicMock

import pytest

from nominal.core import Workspace
from nominal.experimental.analyst import Automation, AutomationRun, AutomationRunStatus
from nominal.protos.ai.v1 import analyst_automation_pb2 as api


def test_update_preserves_false_revision_and_returns_secrets() -> None:
    """An explicit disable updates the same snapshot and exposes newly issued secrets separately."""
    clients = MagicMock()
    automation = Automation._from_proto(clients, api.Automation(automation_rid="auto", revision=7, enabled=True))
    clients.analyst_automations.call.return_value = api.UpdateAutomationResponse(
        automation=api.Automation(automation_rid="auto", revision=8, enabled=False),
        webhook_secrets=[api.AutomationWebhookSecret(trigger_id="hook", secret="sensitive")],
    )
    result = automation.update(enabled=False, yaml_text="new yaml")
    request = clients.analyst_automations.call.call_args.args[1]
    assert request.expected_revision == 7
    assert set(request.update_mask.paths) == {"enabled", "yaml_text"}
    assert not request.automation.enabled
    assert result.automation is automation
    assert automation.revision == 8
    assert result.webhook_secrets[0].secret == "sensitive"
    assert "sensitive" not in repr(result)


def test_conflict_keeps_snapshot_unchanged() -> None:
    """A failed update never changes the local revision or enabled state."""
    import pytest

    from nominal.exceptions import NominalConflictError

    clients = MagicMock()
    automation = Automation._from_proto(clients, api.Automation(automation_rid="auto", revision=7, enabled=True))
    clients.analyst_automations.call.side_effect = NominalConflictError("conflict")
    with pytest.raises(NominalConflictError):
        automation.update(enabled=False)
    assert automation.revision == 7
    assert automation.enabled


def test_run_preserves_payload_and_explicit_retry_id() -> None:
    """A manual run pins the snapshot revision and carries the caller's idempotency key."""
    clients = MagicMock()
    automation = Automation._from_proto(clients, api.Automation(automation_rid="auto", revision=7))
    clients.analyst_automations.call.return_value = api.CreateAutomationRunResponse(
        automation_run=api.AutomationRun(automation_run_rid="run", status=api.AUTOMATION_RUN_STATUS_REJECTED)
    )
    run = automation.run(payload={"ready": False, "count": 0}, request_id="retry-id")
    request = clients.analyst_automations.call.call_args.args[1]
    assert request.request_id == "retry-id"
    assert request.expected_revision == 7
    assert dict(request.payload) == {"ready": False, "count": 0}
    assert run.status == AutomationRunStatus.REJECTED
    assert run.is_finished


def test_cancel_does_not_imply_completion() -> None:
    """Cancellation refreshes the same run but remains running until the server reports completion."""
    clients = MagicMock()
    run = AutomationRun._from_proto(clients, api.AutomationRun(automation_run_rid="run"))
    clients.analyst_automations.call.return_value = api.CancelAutomationRunResponse(
        automation_run=api.AutomationRun(
            automation_run_rid="run", status=api.AUTOMATION_RUN_STATUS_RUNNING, cancel_requested=True
        )
    )
    assert run.cancel() is run
    assert run.cancel_requested
    assert not run.is_finished


def test_node_runs_follow_pagination() -> None:
    """Node results preserve conversation identity across all result pages."""
    clients = MagicMock()
    run = AutomationRun._from_proto(clients, api.AutomationRun(automation_run_rid="run"))
    clients.analyst_automations.call.side_effect = [
        api.ListAutomationNodeRunsResponse(node_runs=[api.AutomationNodeRun(node_id="one")], next_page_token="next"),
        api.ListAutomationNodeRunsResponse(node_runs=[api.AutomationNodeRun(node_id="two", conversation_rid="chat")]),
    ]
    nodes = list(run.list_node_runs())
    assert [node.node_id for node in nodes] == ["one", "two"]
    assert nodes[0].conversation_rid is None
    assert nodes[1].conversation_rid == "chat"
    assert clients.analyst_automations.call.call_args_list[1].args[1].page_token == "next"


def test_unknown_status_is_not_treated_as_success_or_finished() -> None:
    """New server statuses remain readable without declaring an unknown run complete."""
    run = AutomationRun._from_proto(MagicMock(), api.AutomationRun(status=999))
    assert run.status is AutomationRunStatus.UNSPECIFIED
    assert not run.is_finished


def test_update_omission_does_not_disable_or_replace_yaml() -> None:
    """Updating only source leaves enabled state out of the field mask, including for empty source."""
    clients = MagicMock()
    automation = Automation._from_proto(clients, api.Automation(automation_rid="auto", revision=1, enabled=True))
    clients.analyst_automations.call.return_value = api.UpdateAutomationResponse(
        automation=api.Automation(automation_rid="auto", revision=2, enabled=True)
    )
    automation.update(yaml_text="")
    request = clients.analyst_automations.call.call_args.args[1]
    assert list(request.update_mask.paths) == ["yaml_text"]
    assert request.automation.yaml_text == ""
    assert automation.enabled


@pytest.mark.parametrize("workspace", ["explicit", Workspace(rid="explicit", id="test", org="org")])
def test_create_returns_secrets_and_uses_explicit_workspace(workspace: Workspace | str) -> None:
    """Creation preserves secrets and a supplied workspace without looking up a default."""
    from nominal.core import NominalClient
    from nominal.experimental.analyst import create_automation

    clients = MagicMock()
    client = NominalClient(clients)
    clients.analyst_automations.call.return_value = api.CreateAutomationResponse(
        automation=api.Automation(automation_rid="created", workspace_rid="explicit", yaml_text="source"),
        webhook_secrets=[api.AutomationWebhookSecret(trigger_id="hook", secret="new-secret")],
    )
    saved = create_automation(client, "source", workspace=workspace, request_id="retry-id")
    request = clients.analyst_automations.call.call_args.args[1]
    assert request.automation.workspace_rid == "explicit"
    assert not request.automation.enabled
    assert request.request_id == "retry-id"
    clients.resolve_default_workspace_rid.assert_not_called()
    assert saved.automation.yaml_text == "source"
    assert saved.webhook_secrets[0].secret == "new-secret"
    assert "new-secret" not in repr(saved)


def test_validation_returns_document_issues() -> None:
    """Invalid source returns server-owned paths and messages without throwing or saving."""
    from nominal.core import NominalClient
    from nominal.experimental.analyst import validate_automation

    clients = MagicMock()
    clients.resolve_default_workspace_rid.return_value = "default"
    clients.analyst_automations.call.return_value = api.ValidateAutomationResponse(
        valid=False, issues=[api.AutomationValidationIssue(path="nodes.0.config.prompt", message="required")]
    )
    result = validate_automation(NominalClient(clients), "")
    assert not result.valid
    assert [(issue.path, issue.message) for issue in result.issues] == [("nodes.0.config.prompt", "required")]
    request = clients.analyst_automations.call.call_args.args[1]
    assert request.workspace_rid == "default"
    assert request.yaml_text == ""
