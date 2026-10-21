r"""Review a propulsion test stand.

Create temperature, oil-pressure, and controller-fault checks, then evaluate a historical run.
For setup, creation and updates, stable keys, and reviewing results, see
[Authoring checklists as code](/guides/checklists/authoring.md).

This example accepts runs with at most one asset. For multi-asset runs, select an asset using
the [data review builder](/guides/checklists/data-review.md#applying-checklists-to-runs).
The run needs a data source reference named `telemetry`. Channel names, units, and expected
results are documented alongside each check below; adapt them to your test setup.

Create the checklist and review a run:

```shell
python deploy_checklist.py --profile my-profile --source-revision YOUR_GIT_SHA --run-rid RUN_RID
```

The script prints the checklist RID and version, review URL, and generated events.
To compare limits against the same telemetry, use the saved checklist RID and the same run RID:

```shell
python deploy_checklist.py --profile my-profile --source-revision NEXT_GIT_SHA \
    --run-rid RUN_RID --checklist-rid SAVED_RID --temperature-limit 110
```

"""

import click

from nominal.core import EventType, NominalClient, Priority
from nominal.experimental.compute_as_code import ChannelBinding, CheckDefinition, ChecklistBuilder


def test_stand_checks(temperature_limit: float) -> ChecklistBuilder:
    """Compile the complete thermal, pressure, and controller-state requirements locally."""
    builder = ChecklistBuilder(strict=True)

    # Catch sustained overheating, allowing spikes shorter than two seconds.
    # engine.temperature is in °C: 105 °C for three seconds should violate the
    # initial 100 °C limit, but not the revised 110 °C limit on the same data.
    # Keep the key unchanged when revising the limit to preserve check identity.
    builder.add(
        CheckDefinition(
            title="Sustained engine overheating",
            expression=f"(temperature > {temperature_limit}).ranges(min_duration=Duration.Seconds(2))",
            description=f"Temperature exceeds {temperature_limit} °C for at least two seconds.",
            priority=Priority.P1,
            key="engine.overheating",
            event_type=EventType.FLAG,
            event_labels=["thermal", "test-stand"],
        ),
        inputs={"temperature": ChannelBinding("telemetry", "engine.temperature")},
    )
    # engine.speed is in rpm and engine.oil_pressure is in bar. Apply the
    # 2–5 bar band only above 1,000 rpm so normal low pressure at rest is excluded.
    # Both the running condition and the violation must persist for one second.
    builder.add(
        CheckDefinition(
            title="Oil pressure outside operating envelope",
            expression="""
running = speed > 1000
outside_band = (pressure < 2) | (pressure > 5)
(running & outside_band).ranges(min_duration=Duration.Seconds(1))
""",
            description="Oil pressure leaves 2–5 bar for at least one second while speed exceeds 1,000 rpm.",
            priority=Priority.P1,
            key="engine.oil_pressure",
            event_type=EventType.FLAG,
            event_labels=["lubrication", "test-stand"],
        ),
        inputs={
            "speed": ChannelBinding("telemetry", "engine.speed"),
            "pressure": ChannelBinding("telemetry", "engine.oil_pressure"),
        },
    )
    # controller.mode is categorical. Flag any FAULT interval independently
    # of the numeric checks; the binding must declare the categorical type.
    builder.add(
        CheckDefinition(
            title="Controller fault",
            expression='mode.category_ranges().eq("FAULT")',
            priority=Priority.P1,
            key="controller.fault",
            event_type=EventType.FLAG,
            event_labels=["controller", "test-stand"],
        ),
        inputs={"mode": ChannelBinding("telemetry", "controller.mode", series_type="categorical")},
    )
    return builder


@click.command()
@click.option("--profile", required=True, help="Nominal profile to use.")
@click.option("--source-revision", required=True, help="Git commit identifying the deployed source.")
@click.option("--run-rid", required=True, help="Existing run exposing the telemetry channels used by the checks.")
@click.option("--checklist-rid", help="Previously created checklist; omit only for initial creation.")
@click.option("--temperature-limit", type=float, default=100.0, show_default=True, help="Temperature ceiling in °C.")
def main(profile: str, source_revision: str, run_rid: str, checklist_rid: str | None, temperature_limit: float) -> None:
    """Publish the complete definition, record its version, and review a historical run."""
    # Compile all checks locally before making any requests.
    builder = test_stand_checks(temperature_limit)
    client = NominalClient.from_profile(profile)
    run = client.get_run(run_rid)
    # Check the example's run restriction before publishing a new checklist version.
    if len(run.assets) > 1:
        raise click.ClickException("Use the data review builder with asset= to review a multi-asset run.")
    message = f"Deploy test-stand checks from {source_revision}"
    # Create once; later deployments use the saved RID and replace the complete
    # definition. Include all three checks even when only the thermal limit changes.
    if checklist_rid is None:
        checklist = builder.create(client, "Propulsion test-stand review", commit_message=message)
    else:
        checklist = client.get_checklist(checklist_rid)
        builder.commit(checklist, commit_message=message)
    print(f"Source revision: {source_revision}")
    print(f"Temperature limit: {temperature_limit} °C")
    print(f"Checklist RID: {checklist.rid}")
    print(f"Checklist version: {checklist.commit_id}")
    print(f"Checklist: {checklist.nominal_url}")
    # Evaluate exactly this version. Print the URL before waiting so the review
    # remains accessible if polling is interrupted.
    review = checklist.execute(run, commit=checklist.commit_id)
    print(f"Review: {review.nominal_url}")
    review.poll_for_completion()
    # Compare the actual events with the cases described alongside the checks.
    # An empty list alone does not establish that all required data was present.
    for event in review.get_events():
        print(f"{event.name}: start={event.start} ns UTC, duration={event.duration} ns")
    print("Inspect evaluation status and telemetry coverage before accepting the run.")


if __name__ == "__main__":
    main()
