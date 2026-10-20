"""Deploy a collection of checks from source control.

Generate checks with different limits, then create a checklist once or replace its complete
source-owned definition using its saved RID. Record the printed JSON with your deployment
artifacts to associate the source revision with the exact checklist commit.

See [Authoring checklists as code](/guides/checklists/authoring.md) for the full workflow.

Install `nominal[compute]` and configure a Nominal profile before running. For example:

```shell
python deploy_checklist.py --profile my-profile --source-revision YOUR_GIT_SHA
python deploy_checklist.py --profile my-profile --source-revision NEXT_GIT_SHA --checklist-rid SAVED_RID
```

Each invocation publishes a version, even if the source has not changed. Updates replace
all checks and variables, including UI edits. This script does not start execution.
"""

import json

import click

from nominal.core import EventType, NominalClient, Priority
from nominal.experimental.compute_as_code import ChannelBinding, CheckDefinition, ChecklistBuilder


def temperature_checks(limits: dict[str, float]) -> ChecklistBuilder:
    """Build one independently identified check per channel and limit."""
    builder = ChecklistBuilder(strict=True)
    for channel, limit in limits.items():
        builder.add(
            CheckDefinition(
                title=f"{channel} temperature warning",
                expression=f"(temperature > {limit}).ranges(min_duration=Duration.Seconds(2))",
                priority=Priority.P2,
                key=f"thermal.{channel}",
                event_type=EventType.FLAG,
                event_labels=["thermal", "acceptance"],
            ),
            inputs={"temperature": ChannelBinding("telemetry", channel)},
        )
    return builder


@click.command()
@click.option("--profile", required=True, help="Nominal profile to use.")
@click.option("--source-revision", required=True, help="Git commit identifying the deployed source.")
@click.option("--checklist-rid", help="Previously created checklist; omit only for initial creation.")
def main(profile: str, source_revision: str, checklist_rid: str | None) -> None:
    """Publish the complete definition and print a record for this deployment."""
    client = NominalClient.from_profile(profile)
    # Keep this configuration in source control. Expand it with ordinary Python as needed.
    builder = temperature_checks({"engine.temperature": 100.0, "backup_engine.temperature": 110.0})
    message = f"Deploy checks from {source_revision}"
    if checklist_rid is None:
        checklist = builder.create(client, "Thermal acceptance", commit_message=message)
    else:
        checklist = client.get_checklist(checklist_rid)
        builder.commit(checklist, commit_message=message)
    click.echo(
        json.dumps(
            {
                "source_revision": source_revision,
                "checklist_rid": checklist.rid,
                "checklist_commit": checklist.commit_id,
                "url": checklist.nominal_url,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
