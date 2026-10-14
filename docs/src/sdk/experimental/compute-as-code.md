# Compute as code

Author expressions with `nominal.compute` and use the client to evaluate them or create
checklists and derived datasets. Install the optional dependency with:

```sh
pip install 'nominal[compute]'
```

The compute extra uses `nominal-compute` 0.1480.0. Older scripts must replace
`import nominal_compute as nc` with `import nominal.compute as nc`.

## Author a checklist

`ChecklistBuilder` accepts compute **range expressions describing violations**. For example,
this check detects temperatures above 100 for at least two seconds:

```python
import nominal.compute as nc
from nominal.core import NominalClient, Priority
from nominal.experimental.compute_as_code import ChannelBinding, ChecklistBuilder

client = NominalClient.from_profile("my-profile")
temperature = nc.NumericSeries.Reference("temperature")
overheated = (temperature > 100).ranges(min_duration=nc.Duration.Seconds(2))

builder = ChecklistBuilder().add_check(
    "Sustained overheating",
    overheated,
    inputs={"temperature": ChannelBinding("telemetry", "engine.temperature")},
    priority=Priority.P1,
    description="Temperature exceeds 100 for at least two seconds.",
)
checklist = builder.create(client, "Engine acceptance", labels=["acceptance"])
```

Adding checks makes no network requests. The builder snapshots each expression and its
bindings when added. `create` creates a **published** checklist in the client's default
workspace, assigned to the authenticated user unless specified otherwise. It does not execute
the checklist. Each call creates a new resource; it does not update a previously created checklist.

`ChannelBinding` uses the data source reference or data scope name and channel name,
so the same checklist can run against different runs or assets. Its optional `tags` mapping
selects a particular tagged series. Missing references and channel availability are resolved
by the service, not checked while building.

Execute through the existing checklist API:

```python
run = client.get_run("run-rid")
review = checklist.execute(run).poll_for_completion()
print(review.nominal_url)
```

Completion means evaluation has finished, not that all checks passed. For multi-asset runs,
use `client.data_review_builder().execute_checklist(run, checklist, asset=asset).initiate()`
to select the asset explicitly.

## Reuse a condition across channels

For several sensors with the same rule, supply one binding map per implementation:

```python
builder = ChecklistBuilder().add_parameterized_check(
    "Sustained overheating",
    overheated,
    implementations=(
        {"temperature": ChannelBinding("telemetry", "engine.temperature", tags={"sensor": sensor})}
        for sensor in ("left", "right")
    ),
    priority=Priority.P1,
)
checklist = builder.create(client, "Engine sensors")
```

The service evaluates each implementation independently and associates violations with its
implementation index. Use separate `add_check` calls when checks need different names,
descriptions, or priorities.

## Evaluation and authoring scope

Use compute operations to express persistence, intersections, unions, and other range logic.
The builder requires a `RangeSeries`; convert a boolean expression with `.ranges()`.
Alignment, fill, and missing-data handling use the service's checklist defaults. In particular,
an empty range result should not be used as a local assertion that input data was present.

This API stores the serialized compute graph, not Python source. Keep authoring code in your
repository. Source-string compilation, Python-source round trips in the UI, draft editing,
and updating existing checklist versions are not supported by this builder.

For series evaluation and derived datasets, see the
[package walkthrough](https://github.com/nominal-io/nominal-client/blob/main/nominal/experimental/compute_as_code/README.md).

## API reference

```{eval-rst}
.. automodule:: nominal.experimental.compute_as_code
```
