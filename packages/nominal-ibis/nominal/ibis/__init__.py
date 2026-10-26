"""Ibis backend for the Nominal SQL API.

Compiles Ibis expressions to SQL in the Nominal SQL API's dialect and executes
them through an authenticated NominalClient, streaming results back as Arrow.

Structured channel values, such as ``points_struct.value``, are returned as
JSON strings. Use ``json.loads()`` to parse them.

Example:
    ```python
    import ibis
    from ibis import _
    from nominal.core import NominalClient

    client = NominalClient.from_profile("prod")
    con = ibis.nominal.connect(client)
    pts = con.table("points_double")
    per_minute = (
        pts.filter(_.dataset_rid == "ri.catalog....", _.channel == "temperature")
        .group_by(minute=_.ts.truncate("m"))
        .agg(n=_.count(), avg=_.value.mean().round(2))
        .to_polars()
    )
    print(per_minute.head())
    ```

"""

from nominal.ibis._backend import Backend, connect
from nominal.ibis.exceptions import NominalSqlError

__all__ = [
    "Backend",
    "NominalSqlError",
    "connect",
]
