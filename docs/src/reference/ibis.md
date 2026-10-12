# Ibis

Install the Ibis backend with the `ibis` extra:

```sh
pip install "nominal[ibis]"
```

Use an existing Nominal profile, or create one with
`nom config profile add`.

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

Structured channel values, such as `points_struct.value`, are returned as JSON
strings. Use `json.loads()` to parse them.

For large results, use `query.to_pyarrow_batches()` to process rows in batches
without loading the entire result into memory.

```{eval-rst}
.. automodule:: nominal.ibis
```
