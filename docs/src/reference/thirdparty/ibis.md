# Ibis

Install the local workspace package from the repository root:

```sh
uv sync --extra sql
uv run --extra sql python
```

Use an existing Nominal profile, or create one with
`uv run --extra sql nom config profile add`. The server must provide recursive
catalog types and apply the final SQL projection.

```python
import json

import ibis
from nominal.core import NominalClient

con = ibis.nominal.connect(NominalClient.from_profile("your-profile"))
print(con.list_tables())

points = con.table("points_struct")
print(points.schema())

query = (
    points.filter(
        points.dataset_rid == "your-dataset-rid",
        points.channel == "your-struct-channel",
    )
    .select("ts", "value")
    .limit(10)
)
print(con.compile(query))  # Inspect SQL without executing the query.
df = query.to_pandas()
df["parsed_value"] = df["value"].map(lambda value: None if value is None else json.loads(value))
print(df)
```

`MAP<ANY, ANY>` columns such as `points_struct.value` are JSON strings in
results, matching Scout's Arrow serialization. Parse them after fetching;
Ibis map operations are unavailable on these string columns. Concretely typed
maps remain maps. Other unresolved `ANY` types are unsupported.

To test without contacting a server:

```sh
uv run --extra sql pytest tests/ibis --no-cov -q
```

::: nominal.ibis
