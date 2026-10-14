# Ibis

Install the Ibis backend with the `ibis` extra:

```sh
pip install "nominal[ibis]"
```

Use an existing Nominal profile, or create one with
`nom config profile add`.

```python
import ibis
from nominal.core import NominalClient

con = ibis.nominal.connect(NominalClient.from_profile("your-profile"))
print(con.list_tables())

points = con.table("points_double")
print(points.schema())

query = (
    points.filter(
        points.dataset_rid == "your-dataset-rid",
        points.channel == "your-numeric-channel",
    )
    .select("ts", "value")
    .limit(10)
)
print(con.compile(query))  # Inspect SQL without executing the query.
df = query.to_polars()
print(df)
```

Structured channel values, such as `points_struct.value`, are returned as JSON
strings. Use `json.loads()` to parse them.

Timestamp columns include their UTC timezone. Naive filter datetimes are
treated as UTC; timezone-aware ones are converted to UTC.

Ibis returns `truncate()` and `bucket()` results without a timezone. Add
`.cast("timestamp('UTC')")` to get timezone-aware UTC results.

For large results, use `query.to_pyarrow_batches()` to process rows in batches
without loading the entire result into memory.

```{eval-rst}
.. automodule:: nominal.ibis
```
