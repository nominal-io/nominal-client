# Nominal Compute DSL

This library hosts an experimental Python-internal DSL for expressing a Nominal compute graph, and functions to execute the compute graph and retrieve results.

## Python-internal DSL

The DSL is primarily backed by a method-chaining API which allows for clear composition and good LSP support inside your IDE.

The entrypoint into node operations is to select a channel using the `.channel()` class method on the desired node type.

```py
node = NumericNode.channel(asset_rid, data_scope_name, channel_name)
```

From there, you can rely on IDE hints to compose operations:

```
node.⇥
    ┌─────────────────────────────┬───────────────────────────────────────────────┐
    │ abs                         │ def derivative(time_unit: TimeUnitLiteral)  x │
    │ acos                        │ ──────────────────────────────────────────    │
    │ asin                        │ Calculates the rate of change between         │
    │ atan2                       │ subsequent points.                            │
    │ channel                     │                                               │
    │ cos                         │                                               │
    │ cumulative_sum              │                                               │
    │ derivative                ▶ │                                               │
    ├─────────────────────────────┴───────────────────────────────────────────────┤
    │ Tab to accept • ↑/↓ to navigate • Esc to dismiss                            │
    └─────────────────────────────────────────────────────────────────────────────┘
```

The DSL exposes operator overloads where appropriate, and also includes explicit methods for the same operations:

```py
assert (node + node) == node.plus(node)
```

## Compute library

After you have a compute tree defined, you will want to test your compute logic. We expose a function to retrieve bucketed compute results, and will be expanding this library in the future with additional ways to retrieve compute results. For a full example:

```py
from nominal import NominalClient
from nominal.experimental.compute.dsl import nodes
from nominal.experimental.compute import compute_buckets

client = NominalClient.from_profile("staging")
asset_rid = "ri.scout.gov-staging.asset.b51d64ef-e61e-490c-ba94-3988ec5b121f"
scope_name = "my_scope"
channel_name = "some_value"

asset = client.get_asset(asset_rid)
scope = asset.get_dataset(scope_name)
assert scope.bounds is not None

channel = nodes.NumericNode.channel(asset_rid, scope_name, channel_name)

deriv = channel.derivative(time_unit="s")
integ = channel.integral(start_timestamp=scope.bounds.start, time_unit="s")
node = (deriv + integ) / channel.abs()
for bucket in compute_buckets(client, node, scope.bounds.start, scope.bounds.end):
    print(f"Timestamp: {bucket.timestamp}, Mean: {bucket.mean}")
```
