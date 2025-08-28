# Nominal Compute DSL

This library hosts an experimental Python-internal DSL for expressing Nominal compute queries, with functions to execute and retrieve results.

## Python-internal DSL

The DSL is primarily backed by a method-chaining API which allows for clear composition and good language support inside your IDE.

The entrypoint into expression operations is to select a channel using the `.channel()` class methods.

```py
chan = NumericExpr.channel(asset_rid, data_scope_name, channel_name)
```

From there, you can rely on IDE hints to compose operations:

```
chan.⇥
    ┌─────────────────────────────┬───────────────────────────────────────────────┐
    │◇ abs                        │ def derivative(time_unit: TimeUnitLiteral)  x │
    │◇ acos                       │ ──────────────────────────────────────────    │
    │◇ asin                       │ Calculates the rate of change between         │
    │◇ atan2                      │ subsequent points.                            │
    │◇ channel                    │                                               │
    │◇ cos                        │                                               │
    │◇ cumulative_sum             │                                               │
    │◇ derivative               ▶ │                                               │
    ├─────────────────────────────┴───────────────────────────────────────────────┤
    │ Tab to accept • ↑/↓ to navigate • Esc to dismiss                            │
    └─────────────────────────────────────────────────────────────────────────────┘
```

The DSL exposes operator overloads where appropriate, and also includes explicit methods for the same operations:

```py
assert (chan + chan) == chan.plus(chan)
```

## Compute library

After you have a compute query defined, you will want to test your compute logic. We expose a function to retrieve bucketed compute results, and will be expanding this library in the future with additional ways to retrieve compute results. For a full example:

```py
from nominal import NominalClient
from nominal.experimental.compute.dsl import exprs
from nominal.experimental.compute import compute_buckets

client = NominalClient.from_profile("staging")
asset_rid = "ri.scout.gov-staging.asset.b51d64ef-e61e-490c-ba94-3988ec5b121f"
scope_name = "my_scope"
channel_name = "some_value"

asset = client.get_asset(asset_rid)
scope = asset.get_dataset(scope_name)
assert scope.bounds is not None

# Get channel by asset, data scope name, and channel name
channel = exprs.NumericExpr.asset_channel(asset_rid, scope_name, channel_name)

deriv = channel.derivative(time_unit="s")
integ = channel.integral(start_timestamp=scope.bounds.start, time_unit="s")
expr = (deriv + integ) / channel.abs()
for bucket in compute_buckets(client, expr, scope.bounds.start, scope.bounds.end):
    print(f"Timestamp: {bucket.timestamp}, Mean: {bucket.mean}")
```

When retrieving channels using the expressions library, you alternatively can retrieve channels in a few ways:

* By datasource:
  
  ```python
  dataset_rid = "ri.catalog.gov-staging.dataset.b373ff5a-cd2b-4969-bf5b-772688a11249"
  tags = {"platform": "electric-glider-mk1"}
  channel = exprs.NumericExpr.datasource_channel(dataset_rid, channel_name, tags=tags)
  ```

* By run:
  
  ```python
  run_rid = "ri.catalog.gov-staging.dataset.b373ff5a-cd2b-4969-bf5b-772688a11249"
  tags = {"color": "green"}
  channel = exprs.NumericExpr.run_channel(run_rid, channel_name, additional_tags=tags)
  ```

* Direct `nominal.core.Channel` reference:
  
  ```python
  dataset = client.get_dataset(dataset_rid)
  channels = dataset.search_channels(exact_match=["apple_sizes"])
  tags = {"color": "red"}
  channel = exprs.NumericExpr.channel(channels[0], tags=tags)
  ```
