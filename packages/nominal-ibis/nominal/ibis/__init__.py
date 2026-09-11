"""Ibis backend for the Nominal SQL API.

Compiles Ibis expressions to SQL in the Nominal SQL API's dialect and executes
them through an authenticated NominalClient, streaming results back as Arrow.

Requires a server that returns recursive catalog column types and applies the
final SQL projection. Tables containing unresolved ANY types (such as
points_struct.value) cannot be opened with con.table(); use con.sql() to select
concrete columns or explicitly cast the values you need.

Example:
    import ibis
    from ibis import _
    from nominal.core import NominalClient

    client = NominalClient.from_profile("prod")
    con = ibis.nominal.connect(client)
    pts = con.table("points_double")
    per_minute = (
        pts.filter(_.dataset_rid == "ri.catalog....", _.channel == "temperature")
        .group_by(minute=_.ts.truncate("m"))
        .agg(n=_.count(), avg=_.value.mean())
        .to_pandas()
    )
    print(per_minute.head())

"""

from nominal.ibis._backend import Backend, NominalSqlError, connect

__all__ = [
    "Backend",
    "NominalSqlError",
    "connect",
]
