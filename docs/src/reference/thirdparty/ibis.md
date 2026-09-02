Query Nominal's SQL interface and get the results back as [ibis](https://ibis-project.org) tables.

Install with the `ibis` extra:

```sh
pip install "nominal[ibis]"
```

```python
from nominal.core import NominalClient
from nominal.thirdparty.ibis import NominalSqlConnection

client = NominalClient.from_profile("my-profile")
conn = NominalSqlConnection.from_client(client)

conn.list_tables()
conn.get_schema("points_double")

rpm = conn.sql(f"""
    SELECT channel, date_bin(INTERVAL '1' SECOND, ts) AS bucket, avg(value) AS value
    FROM points_double
    WHERE dataset_rid = '{dataset.rid}' AND channel LIKE 'engine%'
    GROUP BY channel, bucket
""")
rpm.filter(rpm.value > 4000).order_by("bucket").to_pandas()
```

::: nominal.thirdparty.ibis
