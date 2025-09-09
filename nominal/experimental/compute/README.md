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

When retrieving channels using the expressions library, you may also reference channels directly on datasources or runs:

```python
# Reference a channel present on a run by data scope name
run_rid = "ri.catalog.gov-staging.dataset.b373ff5a-cd2b-4969-bf5b-772688a11249"
run_channel = exprs.NumericExpr.run_channel(run_rid, scope_name, channel_name)

# Reference a channel present in a dataset
dataset_rid = "ri.catalog.gov-staging.dataset.b373ff5a-cd2b-4969-bf5b-772688a11249"
datasource_channel = exprs.NumericExpr.datasource_channel(dataset_rid, scope_name, channel_name)
```

When retrieving channels on an asset or a run, the default tag filters for a given data scope are applied to underlying expressions.
However, it may still be useful to further filter data on tags that aren't specified by the data scope definition-- this is done using the `additional_tags` args (or just `tags` for datasource channels):

```py
channel = exprs.NumericExpr.asset_channel(asset_rid, scope_name, channel_name, additional_tags={"color": "green"})
run_channel = exprs.NumericExpr.run_channel(run_rid, scope_name, channel_name, additional_tags={"color": "green"})
datasource_channel = exprs.NumericExpr.datasource_channel(dataset_rid, scope_name, channel_name, tags={"platform": "electric-glider-mk1"})
```

## Modules

Now that we can express compute expressions with Python code, a natural extension is to want to save these compute expressions for reuse. Nominal supports this workflow with Modules, which are reusable groups of functions.

We'll walk through an example of creating a module for frame transforms, namely converting quaternions to roll-pitch-yaw Euler angles.

We have an Ardupilot flight controller hooked up in the office streaming data to Nominal constantly. We'll first fetch the quaternion data to ensure we have the channel names correct and data flowing.

```py
import time
from datetime import timedelta
from nominal import NominalClient
from nominal.experimental.compute import batch_compute_buckets
from nominal.experimental.compute.dsl.exprs import NumericExpr

client = NominalClient.from_profile("orion-staging")

asset = client.get_asset("ri.scout.gov-staging.asset.435c0bf0-4cc7-446a-8333-11a85c0bdea3")
w = NumericExpr.asset_channel(asset.rid, "mavlink", "attitude_quaternion.q1")
x = NumericExpr.asset_channel(asset.rid, "mavlink", "attitude_quaternion.q2")
y = NumericExpr.asset_channel(asset.rid, "mavlink", "attitude_quaternion.q3")
z = NumericExpr.asset_channel(asset.rid, "mavlink", "attitude_quaternion.q4")
now_ns = time.time_ns()
delta_ns = int(timedelta(minutes=10).total_seconds() * 1e9)


batches = batch_compute_buckets(client, [x, y, z, w], now_ns - delta_ns, now_ns)
for channel, buckets in zip(["x", "y", "z", "w"], batches):
    print(channel, len(buckets))
```
```
x 1000
y 1000
z 1000
w 1000
```

As expected, we see ~1000 buckets returned for each channel. Next, let's define some functions to transform these quaternions into Euler angles:

```py
def roll(x: NumericExpr, y: NumericExpr, z: NumericExpr, w: NumericExpr) -> NumericExpr:
    """Compute roll (rotation around X) from quaternion (x, y, z, w)."""
    return (w * x + y * z).scale(2).atan2((x * x + y * y).scale(-2).offset(1))


def pitch(x: NumericExpr, y: NumericExpr, z: NumericExpr, w: NumericExpr) -> NumericExpr:
    """Compute pitch (rotation around Y) from quaternion (x, y, z, w)."""
    return (w * y - z * x).scale(2).asin()


def yaw(x: NumericExpr, y: NumericExpr, z: NumericExpr, w: NumericExpr) -> NumericExpr:
    """Compute yaw (rotation around Z) from quaternion (x, y, z, w)."""
    return (w * z + x * y).scale(2).atan2((y * y + z * z).scale(-2).offset(1))
```

And now we can see if we get data back from our transforms:

```py
exprs = [roll(x, y, z, w), pitch(x, y, z, w), yaw(x, y, z, w)]
batches = batch_compute_buckets(client, exprs, now_ns - delta_ns, now_ns)
for channel, buckets in zip(["roll", "pitch", "yaw"], batches):
    print(channel, len(buckets))
```
```
roll 1001
pitch 1001
yaw 1001
```

Great! Now, let's define a module for these frame transforms. The main difference is that our channels are no longer hardcoding the asset ID. Instead, the asset becomes a module parameter, which allows us to re-use the module definition on any asset.

We can simply wrap our functions with a decorator to include them in the module. The variables (x, y, z, w) are returned from the module definition so that our functions can be called with those channels automatically, so that we don't have to re-specify the asset, data scope, and channel name for every function call.

```py
from nominal.experimental.compute import module
from nominal.experimental.compute.dsl import params


@module.defn
def frame_transforms(asset: params.StringVariable) -> module.ModuleVariables:
    """Frame-transform utilities for quaternions (channels x, y, z, w)."""
    return {
        "w": NumericExpr.asset_channel(asset, "mavlink", "attitude_quaternion.q1"),
        "x": NumericExpr.asset_channel(asset, "mavlink", "attitude_quaternion.q2"),
        "y": NumericExpr.asset_channel(asset, "mavlink", "attitude_quaternion.q3"),
        "z": NumericExpr.asset_channel(asset, "mavlink", "attitude_quaternion.q4"),
    }


@frame_transforms.func
def roll(x: NumericExpr, y: NumericExpr, z: NumericExpr, w: NumericExpr) -> NumericExpr:
    """Compute roll (rotation around X) from quaternion (x, y, z, w)."""
    return (w * x + y * z).scale(2).atan2((x * x + y * y).scale(-2).offset(1))


@frame_transforms.func
def pitch(x: NumericExpr, y: NumericExpr, z: NumericExpr, w: NumericExpr) -> NumericExpr:
    """Compute pitch (rotation around Y) from quaternion (x, y, z, w)."""
    return (w * y - z * x).scale(2).asin()


@frame_transforms.func
def yaw(x: NumericExpr, y: NumericExpr, z: NumericExpr, w: NumericExpr) -> NumericExpr:
    """Compute yaw (rotation around Z) from quaternion (x, y, z, w)."""
    return (w * z + x * y).scale(2).atan2((y * y + z * z).scale(-2).offset(1))

```

Finally, we have a function call to register this module in Nominal.

```py
mod = frame_transforms.register(client)
```

Now that this module is stored in Nominal, we can apply an asset. That will allow us to use the `roll`, `pitch`, `yaw` derived series here in Workbooks inside Nominal.

```py
orion = client.get_asset("ri.scout.gov-staging.asset.435c0bf0-4cc7-446a-8333-11a85c0bdea3")
app = mod.apply(asset=orion.rid)
```
