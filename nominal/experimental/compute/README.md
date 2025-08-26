# Nominal Compute DSL Example

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

channel = exprs.NumericExpr.channel(asset_rid, scope_name, channel_name)

deriv = channel.derivative(time_unit="s")
integ = channel.integral(start_timestamp=scope.bounds.start, time_unit="s")
expr = (deriv + integ) / channel.abs()
for bucket in compute_buckets(client, expr, scope.bounds.start, scope.bounds.end):
    print(f"Timestamp: {bucket.timestamp}, Mean: {bucket.mean}")
```
