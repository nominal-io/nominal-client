# Run Analyst automations

Use the experimental Analyst API to save a YAML workflow, enable its triggers, and follow a run
from Python. Your organization must have Analyst automations enabled, and your workspace must
support Analyst. Configuration changes require workspace configuration-edit permission; running
an automation requires workflow-execution permission. You can manage only automations you own.

```{include} _snippets/connect-the-client.md
```

## Save a workflow

{py:func}`~nominal.experimental.analyst.validate_automation` checks the document without saving it.
The server owns the YAML schema. Use
{py:func}`~nominal.experimental.analyst.get_automation_schema` for its current schema,
examples, and supported components.

```python
from nominal.experimental import analyst

yaml_text = """\
apiVersion: nominal.io/analyst-automation/v1
metadata:
  name: Confirm automation execution
triggers:
  - id: manual
    type: manual
    version: 1
    config: {}
nodes:
  - id: confirm
    type: agent
    version: 1
    config:
      prompt: Reply with confirmation that the automation ran. Do not use tools.
      resources: []
edges:
  - source: manual
    target: confirm
"""

validation = analyst.validate_automation(client, yaml_text)
if not validation.valid:
    raise ValueError(validation.issues)

saved = analyst.create_automation(client, yaml_text)
automation = saved.automation  # New automations default to disabled.
```

{py:func}`~nominal.experimental.analyst.create_automation` returns the saved automation and any
new webhook signing secrets. Store `saved.webhook_secrets` securely. They are returned once;
fetching or refreshing an automation does not recover them.

## Enable and run

{py:meth}`~nominal.experimental.analyst.Automation.update` changes only supplied fields and
refreshes the same Python object. `None` leaves a field unchanged. Updates and deletes use the
snapshot's revision; a concurrent YAML change raises
{py:exc}`~nominal.exceptions.NominalConflictError`. Refresh, inspect the latest source, and decide
whether to retry.

```python
automation.update(enabled=True)
run = automation.run(payload={"source": "python"})
print(run.rid, run.status.name)
```

Enabling controls cron and webhook triggers. Manual runs work even while an automation is disabled.
The API returns after submission; a run may be queued or rejected. Agent nodes run unattended and
may edit resources. Organization tool policies still apply, and a run that needs an answer or approval
can finish with `INTERACTION_REQUIRED`.

## Follow execution

{py:class}`~nominal.experimental.analyst.AutomationRun` exposes status and a server error code.
Refresh it to observe progress. A polling timeout does not cancel the server's run.

```python
import time

deadline = time.monotonic() + 300
while not run.is_finished:
    if time.monotonic() >= deadline:
        raise TimeoutError(f"Automation run is still active: {run.rid}")
    time.sleep(2)
    run.refresh()

if run.status != analyst.AutomationRunStatus.SUCCEEDED:
    raise RuntimeError(f"{run.status.name}: {run.error_code}")

for node in run.list_node_runs():
    print(node.node_id, node.status.name, node.conversation_url)
```

Use {py:func}`~nominal.experimental.analyst.get_automation_run` to resume observing a run by RID.
{py:meth}`~nominal.experimental.analyst.AutomationRun.cancel` requests cancellation and refreshes
the snapshot; refresh again until it reaches a terminal state.

## Use a skill

{py:meth}`~nominal.experimental.analyst.Skill.as_prompt` formats a skill selection without executing
it. Put the returned text in the agent node's `prompt` field before validating and saving the YAML.
The receiving service resolves current skill content and access when it consumes the prompt.

```python
import yaml

skill = analyst.get_skill(client, "<SKILL_RID>")
document = yaml.safe_load(yaml_text)
document["nodes"][0]["config"]["prompt"] = skill.as_prompt("Review the attached run.")
# Replace with a run or other resource the skill needs.
document["nodes"][0]["config"]["resources"] = ["<RESOURCE_RID>"]
updated = automation.update(yaml_text=yaml.safe_dump(document))
```

Store `updated.webhook_secrets` if the new source adds webhook triggers.

## Delete the workflow

```python
# Stop future scheduled/webhook runs when you no longer need the workflow.
automation.update(enabled=False)
automation.delete()  # Retains run history; does not cancel already admitted runs.
```

## Authentication and retries

This experimental integration exchanges a user API key for a session token and caches it briefly.
It renews that cache automatically. Service-account keys are not supported by this exchange path.
An explicitly supplied session token is used unchanged; the caller owns its renewal.

The client uses the configured API endpoint, additional headers, TLS trust, and request timeouts.
Automation requests are not automatically retried. For deliberate retries of creation or manual
submission, supply the same UUID as `request_id`. A retry of creation returns the original automation
but does not repeat its webhook secrets. If that response was lost, use
{py:meth}`~nominal.experimental.analyst.Automation.rotate_webhook_secret` and update the sender.
Rotation invalidates the previous secret.

These APIs do not start an interactive chat or add native ingest-event triggers. Use the server's
capability catalog to determine which YAML components are currently authorable.
