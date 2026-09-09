# SQL queries

This module provides read-only SQL query access to Nominal telemetry tables, installed with the `sql` extra:

```sh
pip install 'nominal[sql]'
```

## Query Nominal

Run a SQL query against your workspace's tables and get results as a `pyarrow.Table`:

```python
from nominal.core import NominalClient
from nominal.experimental.sql import query_sql

client = NominalClient.from_profile("default")
table = query_sql(client, "SELECT dataset_rid, name FROM datasets LIMIT 10")

for row in table.to_pylist():
    print(row["dataset_rid"], row["name"])
```

Your workspace's telemetry tables can be queried as well. Doing so requires a dataset_rid and will also return results as a `pyarrow.Table`:

```python
# from nominal.experimental.sql import query_sql
dataset_rid = 'ri.dataset.example.dataset.12345'
table = query_sql(
    client,
    f"SELECT ts, value, channel FROM points_double WHERE dataset_rid = '{dataset_rid}'",
)
```

Optionally specify a row limit (1..5000):

```python
# from nominal.experimental.sql import query_sql
dataset_rid = 'ri.dataset.example.dataset.12345'
table = query_sql(
    client,
    f"SELECT ts, value, channel FROM points_double WHERE dataset_rid = '{dataset_rid}'",
    max_rows=100,
)
```

Or an explicit workspace:

```python
# from nominal.experimental.sql import query_sql
workspace_rid = "ri.workspace.specific"
table = query_sql(
    client,
    "SELECT name FROM datasets WHERE name LIKE '%test%'",
    workspace_rid=workspace_rid,
)
```

Once your data is available in python, it can be processed using the arrow library or converted to other formats.

## Process data directly using Arrow

Access columns and iterate over rows:

```python
import datetime

table = query_sql(client, "SELECT ts, channel, value FROM points_double LIMIT 100")

timestamps = table.column("ts")
channels = table.column("channel")
values = table.column("value")

for i in range(min(5, table.num_rows)):
    # ts arrives as nanosecond-precision Arrow, which Python's datetime cannot
    # represent, so it has to be read as an integer and converted by hand.
    moment = datetime.datetime.fromtimestamp(timestamps[i].value / 1e9, datetime.timezone.utc)
    print(f"{moment.isoformat()} {channels[i].as_py()}\t\t{values[i].as_py()}")
```

Or use vectorized operations on the entire table:

```python
import pyarrow.compute as pc

table = query_sql(client, "SELECT dataset_rid, num_channels FROM datasets")

# Filter rows where num_channels > 10
filtered = table.filter(pc.greater(table.column("num_channels"), 10))
print(f"Found {filtered.num_rows} datasets with >10 channels")

# Get all dataset RIDs as Python objects
rids = table.column("dataset_rid").to_pylist()
```

## Convert to pandas or polars

For convenience, use the DataFrame wrappers in `nominal.thirdparty`:

**Pandas:**
```python
from nominal.core import NominalClient
from nominal.thirdparty.pandas import query_sql_to_dataframe

client = NominalClient.from_profile("default")
df = query_sql_to_dataframe(client, "SELECT dataset_rid, name FROM datasets LIMIT 10")
print(df)
```

**Polars:**
```python
from nominal.core import NominalClient
from nominal.thirdparty.polars import query_sql_to_dataframe

client = NominalClient.from_profile("default")
df = query_sql_to_dataframe(client, "SELECT dataset_rid, name FROM datasets LIMIT 10")
print(df)
```

## Export to CSV

Run a query and download the full result set as CSV via a presigned URL:

```python
from nominal.experimental.sql import export_sql

url = export_sql(client, "SELECT * FROM datasets")
# Download from the presigned URL, valid for a limited time
```

## Inspect the schema

Fetch the list of queryable tables, columns, and functions:

```python
from nominal.experimental.sql import get_sql_catalog

catalog = get_sql_catalog(client)
for table in catalog.tables:
    print(f"Table: {table.name}")
    for col in table.columns:
        print(f"  {col.name}: {col.type} (nullable={col.nullable})")

for func in catalog.functions:
    print(f"Function: {func.name}")
```


## Putting it all together
```python
"""Comprehensive example demonstrating all SQL query capabilities.

This example queries telemetry data, processes it with Arrow/pandas/polars,
and demonstrates exporting and schema inspection.
"""

import datetime

import pyarrow.compute as pc

from nominal.core import NominalClient
from nominal.experimental.sql import export_sql, get_sql_catalog, query_sql
from nominal.thirdparty.pandas import query_sql_to_dataframe
from nominal.thirdparty.polars import query_sql_to_dataframe as query_sql_to_polars_dataframe



def main(dataset_rid: str) -> None:
    # Initialize client
    client = NominalClient.from_profile("default")

    # ============================================================================
    # 1. INSPECT THE CATALOG
    # ============================================================================
    print("=" * 80)
    print("SCHEMA INSPECTION")
    print("=" * 80)
    catalog = get_sql_catalog(client)
    print(f"Found {len(catalog.tables)} queryable tables:")
    for table in catalog.tables[:3]:  # Show first 3
        print(f"  - {table.name}: {len(table.columns)} columns")
    print(f"Found {len(catalog.functions)} functions available")
    print()

    # ============================================================================
    # 2. RUN A QUERY AND PROCESS WITH ARROW
    # ============================================================================
    print("=" * 80)
    print("PROCESSING WITH ARROW")
    print("=" * 80)
    table = query_sql(
        client,
        f"SELECT ts, value, channel, dataset_rid FROM points_double WHERE dataset_rid = '{dataset_rid}' LIMIT 5000",
    )

    print(f"Query returned {table.num_rows} rows with columns: {table.column_names}")

    # Extract columns
    timestamps = table.column("ts")
    channels = table.column("channel")
    values = table.column("value")

    # Display first 5 rows with formatted timestamps
    print("\nFirst 5 rows:")
    for i in range(min(5, table.num_rows)):
        moment = datetime.datetime.fromtimestamp(timestamps[i].value / 1e9, datetime.timezone.utc)
        print(f"  {moment.isoformat()} {channels[i].as_py()}\t{values[i].as_py()}")

    # Vectorized operation: sum values by channel
    print("\nSum by channel (Arrow vectorized):")
    unique_channels = sorted(pc.unique(channels).to_pylist())
    for channel in unique_channels:
        mask = pc.equal(channels, channel)
        channel_values = values.filter(mask)
        total = pc.sum(channel_values).as_py()
        print(f"  {channel}: {total}")
    print()

    # ============================================================================
    # 3. CONVERT TO PANDAS AND PROCESS
    # ============================================================================
    print("=" * 80)
    print("PROCESSING WITH PANDAS")
    print("=" * 80)
    df_pandas = query_sql_to_dataframe(
        client,
        f"SELECT ts, value, channel, dataset_rid FROM points_double WHERE dataset_rid = '{dataset_rid}' LIMIT 5000",
    )

    print(f"DataFrame shape: {df_pandas.shape}")
    print(f"Columns: {list(df_pandas.columns)}")

    # Sum by channel
    print("\nSum by channel (Pandas):")
    channel_sums = df_pandas.groupby("channel")["value"].sum().sort_index()
    for channel in channel_sums.index:
        print(f"  {channel}: {channel_sums[channel]}")
    print()

    # ============================================================================
    # 4. CONVERT TO POLARS AND PROCESS
    # ============================================================================
    print("=" * 80)
    print("PROCESSING WITH POLARS")
    print("=" * 80)
    df_polars = query_sql_to_polars_dataframe(
        client,
        f"SELECT ts, value, channel, dataset_rid FROM points_double WHERE dataset_rid = '{dataset_rid}' LIMIT 5000",
    )

    print(f"DataFrame shape: {df_polars.shape}")
    print(f"Columns: {df_polars.columns}")

    # Sum by channel
    print("\nSum by channel (Polars):")
    import polars as pl

    channel_sums_polars = df_polars.group_by("channel").agg(pl.col("value").sum()).sort("channel")
    for row in channel_sums_polars.iter_rows(named=True):
        print(f"  {row['channel']}: {row['value']}")
    print()

    # ============================================================================
    # 5. EXPORT TO CSV
    # ============================================================================
    print("=" * 80)
    print("EXPORT TO CSV")
    print("=" * 80)
    presigned_url = export_sql(
        client, f"SELECT ts, value, channel, dataset_rid FROM points_double WHERE dataset_rid = '{dataset_rid}'"
    )
    print(f"Query exported to CSV at: {presigned_url}")
    print("(URL is valid for a limited time)")
    print()

    # ============================================================================
    # 6. SUMMARY
    # ============================================================================
    print("=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print("This example demonstrated:")
    print("  1. Inspecting the SQL catalog (tables, columns, functions)")
    print("  2. Running a query and processing with Arrow (vectorized operations)")
    print("  3. Converting results to pandas and performing groupby aggregations")
    print("  4. Converting results to polars and performing group aggregations")
    print("  5. Exporting query results to CSV via presigned URL")

if __name__ == "__main__":
    dataset = "ri.dataset.example.dataset.12345"
    main(dataset)

```

## Notes

- This module wraps the `nominal.sql.v1.SqlService` backend. Query semantics and available tables are defined by the backend.
- `query_sql()` defaults to your client's default workspace; override with `workspace_rid=...` if needed.
- The `query_id` from a query is not currently retrievable via `query_sql()` (the backend response strips it). Use `export_sql()` if you need it.
- Errors surface as `conjure_python_client.ConjureHTTPError` with details from the backend (invalid syntax, missing tables, timeouts, resource exhaustion, etc.).
